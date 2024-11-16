import asyncio
import hashlib
import json
import math
import os
import shutil
from collections import defaultdict
from functools import partial
from typing import List, Optional, Union

import aiofiles
import msgpack
import supervisely as sly
from supervisely.api.api import ApiField
from supervisely.api.image_api import ImageApi
from supervisely.api.video.video_api import VideoApi, VideoInfo
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential
from tqdm import tqdm
from tqdm.asyncio import tqdm as tqdm_async

# -------------------------------- Global Variables For Migration -------------------------------- #

entity_api = None
download_api_url = None
entities_map = {}

api = sly.Api.from_env()

# ------------------------------------ Constants For Migration ----------------------------------- #
os.environ["GM_REMOTE"] = os.path.expanduser("~/Work/GM")
os.environ["SLY_DIR"] = "sly"
GM_REMOTE = os.getenv("GM_REMOTE")  # TODO Change to your remote storage path
SLY_DIR = os.getenv("SLY_DIR")  # TODO Change to your remote SLY storage directory

# ----------------------------- Asynchronous Functions For Migration ----------------------------- #


class HashMismatchError(Exception):
    def __init__(self, expected_hash, remote_hash):
        self.expected_hash = expected_hash
        self.actual_hash = remote_hash
        super().__init__(f"Hash mismatch: expected {expected_hash}, got {remote_hash}")


class ProjectItemsMap:

    def __init__(self, api: sly.Api, project_id: str):
        self.structure = defaultdict(dict)
        self.api = api
        self.project_id = project_id
        self.file_path = os.path.join(GM_REMOTE, SLY_DIR, f"{project_id}.json")
        self.lock = asyncio.Lock()
        self.api_semaphore = asyncio.Semaphore(10)
        self.loop = sly.utils.get_or_create_event_loop()
        self.loop.create_task(self.initialize())

    async def initialize(self):
        await self.load_from_file()

    async def dump_to_file(self):
        async with aiofiles.open(self.file_path, "w") as f:
            await f.write(json.dumps(self.structure, indent=4))

    async def load_from_file(self):
        async with self.lock:
            if os.path.exists(self.file_path):
                async with aiofiles.open(self.file_path, "r") as f:
                    content = await f.read()
                    if content.strip():  # Check if the file is not empty
                        self.structure.update(json.loads(content))
            else:
                sly.fs.ensure_base_path(self.file_path)

    async def update_file(self, key: str, value: dict):
        async with self.lock:
            if os.path.exists(self.file_path):
                async with aiofiles.open(self.file_path, "r+") as f:
                    content = await f.read()
                    data = json.loads(content) if content.strip() else {}
                    data[str(key)] = value  # Convert key to string

                    # Write only the updated data back to the file
                    await f.seek(0)
                    await f.write(json.dumps(data, indent=4))
                    await f.truncate()
            else:
                await self.dump_to_file()

    async def run_in_executor(self, func, *args):
        loop = sly.utils.get_or_create_event_loop()
        return await loop.run_in_executor(None, func, *args)

    async def calculate_hash(self, file_path: str) -> str:
        hash_algo = hashlib.sha256()
        async with aiofiles.open(file_path, "rb") as f:
            while True:
                chunk = await f.read(8192)
                if not chunk:
                    break
                hash_algo.update(chunk)
        return hash_algo.hexdigest()

    async def move_to_nas(self, local_path: str, nas_path: str, hash_value: str = None):
        shutil.move(local_path, nas_path)
        sly.logger.debug(f"Moved file from {local_path} to {nas_path}")
        if hash_value:
            remote_hash = await self.calculate_hash(nas_path)
            if remote_hash != hash_value:
                raise HashMismatchError(hash_value, remote_hash)

    def list_items_batched(self, dataset_info: sly.DatasetInfo, project_type: str):
        items_api = self.detect_api(project_type)
        fields = [ApiField.ID, ApiField.NAME, ApiField.PATH_ORIGINAL, ApiField.DATASET_ID]

        # loop = sly.utils.get_or_create_event_loop()
        items = self.loop.run_until_complete(
            get_list_optimized(
                dataset_info,
                items_api,
                fields=fields,
                force_metadata_for_links=False,
                semaphore=self.api_semaphore,
                loop=self.loop,
            )
        )
        return items

    def move_items(
        self,
        dataset: dict,
        project: sly.ProjectInfo,
        workspace: sly.WorkspaceInfo,
        team: sly.TeamInfo,
    ):
        dataset_dir = dataset.get("path")
        dataset_info: sly.DatasetInfo = dataset.get("info")
        items = self.list_items_batched(dataset_info, project.type)

        async def process_item(item: Union[sly.ImageInfo, VideoInfo], dataset_progress: tqdm):
            item_id = str(item.id)
            item_path = os.path.join(dataset_dir, f"{item_id}-{item.name}")
            item_dict = {
                "name": item.name,
                "path": item_path,
                "src_path": item.path_original,
                "dataset_id": dataset_info.id,
                "project_id": project.id,
                "workspace_id": workspace.id,
                "team_id": team.id,
            }
            try:
                await self.move_to_nas(item.path_original, item_path, item.hash)
                item_dict["status"] = "success"
            except Exception:
                item_dict["status"] = "failed"
                sly.logger.debug(f"Failed to move item {item_id} to NAS")
            finally:
                await self.update_file(item_id, item_dict)
            dataset_progress.update(1)

        dataset_progress = tqdm(total=len(items), desc=f"Process items in dataset", leave=False)
        item_tasks = [process_item(item, dataset_progress) for item in items]
        self.loop.run_until_complete(asyncio.gather(*item_tasks))
        dataset_progress.close()

    def detect_api(self, project_type: str):
        if project_type == "images":
            return self.api.image
        elif project_type == "videos":
            return self.api.video
        else:
            raise ValueError(f"Project type '{project_type}' is not supported")


async def get_list_idx_page(item_api: Union[ImageApi, VideoApi], method: str, data: dict):
    response = await api.post_async(method, data)
    results = response.json().get("entities", [])
    convert_func = item_api._convert_json_info
    return [convert_func(item) for item in results]


async def get_list_optimized(
    dataset_info: sly.DatasetInfo,
    item_api: Union[ImageApi, VideoApi],
    force_metadata_for_links: Optional[bool] = False,
    fields: Optional[List[str]] = None,
    per_page: Optional[int] = 2000,
    semaphore: Optional[asyncio.Semaphore] = None,
    loop: Optional[asyncio.AbstractEventLoop] = None,
) -> List[Union[sly.ImageInfo, VideoInfo]]:
    """
    List of Images in the given :class:`Dataset<supervisely.project.project.Dataset>`.

    :param dataset_id: :class:`Dataset<supervisely.project.project.Dataset>` ID in which the Images are located.
    :type dataset_id: :class:`int`:
    :param item_api: API object to use for listing items.
    :type item_api: :class:`Api`:
    :param force_metadata_for_links: If True, updates meta for images with remote storage links when listing.
    :type force_metadata_for_links: bool, optional
    :param fields: List of fields to return. If None, returns all fields.
    :type fields: List[str], optional
    :return: List of objects with image information from Supervisely.
    :rtype: :class:`List[ImageInfo]<ImageInfo>`
    """
    tasks = []
    total_pages = math.ceil(dataset_info.items_count / per_page)
    if loop is None:
        loop = sly.utils.get_or_create_event_loop()
    for page_num in range(1, total_pages + 1):
        data = {
            ApiField.PROJECT_ID: dataset_info.project_id,
            ApiField.DATASET_ID: dataset_info.id,
            ApiField.SORT: "id",
            ApiField.SORT_ORDER: "asc",
            ApiField.FORCE_METADATA_FOR_LINKS: force_metadata_for_links,
            ApiField.PER_PAGE: per_page,
            ApiField.PAGE: page_num,
        }
        if fields is not None:
            data[ApiField.FIELDS] = fields
        method = "images.list" if isinstance(item_api, ImageApi) else "videos.list"
        async with semaphore:
            task = get_list_idx_page(item_api, method, data)
            tasks.append(task)
    items = await asyncio.gather(*tasks)
    return [item for sublist in items for item in sublist]


def filter_projects(projects: List[sly.ProjectInfo], types=["images", "videos"]):
    return [project for project in projects if project.type in types]


def get_team_dir(team: sly.TeamInfo) -> str:
    return os.path.join(GM_REMOTE, SLY_DIR, f"{team.id}-{team.name}")


def get_workspace_dir(workspace: sly.WorkspaceInfo, team_dir: str) -> str:
    return os.path.join(team_dir, f"{workspace.id}-{workspace.name}")


def get_project_dir(project: sly.ProjectInfo, workspace_dir: str) -> str:
    return os.path.join(workspace_dir, f"{project.id}-{project.name}")


def flatten_datasets_tree(datasets_tree: dict, project_dir: str) -> dict:
    flat_mapping = {}

    def recursive_flatten(current_tree: dict, parent_path: str):
        for dataset_info, nested_tree in current_tree.items():
            dataset_info: sly.DatasetInfo
            current_path = os.path.join(
                project_dir, parent_path, f"{dataset_info.id}-{dataset_info.name}"
            )
            flat_mapping[str(dataset_info.id)] = {"path": current_path, "info": dataset_info}
            recursive_flatten(nested_tree, current_path)

    recursive_flatten(datasets_tree, "")
    return flat_mapping


def get_datasets(project: sly.ProjectInfo, project_dir: str) -> dict:
    dataset_tree = api.dataset.get_tree(project.id)
    return flatten_datasets_tree(dataset_tree, project_dir)


def collect_project_items_and_move():
    teams = api.team.get_list()
    for team in tqdm(teams, desc="Teams"):
        team_dir = get_team_dir(team)
        workspaces = api.workspace.get_list(team.id)
        for workspace in tqdm(workspaces, desc="Workspaces", leave=False):
            workspace_dir = get_workspace_dir(workspace, team_dir)
            projects = api.project.get_list(workspace.id)
            projects_filtered = filter_projects(projects)
            for project in tqdm(projects_filtered, desc="Projects", leave=False):
                project_dir = get_project_dir(project, workspace_dir)
                flatten_datasets_structure = get_datasets(project, project_dir)
                for dataset in tqdm(flatten_datasets_structure, desc="Datasets", leave=False):
                    project_map = ProjectItemsMap(api, project.id)
                    dataset = flatten_datasets_structure[dataset]
                    project_map.move_items(dataset, project, workspace, team)


if __name__ == "__main__":

    def main():
        try:
            # ------------------------------------ Start Migration Process ------------------------------------ #
            collect_project_items_and_move()
            print("Backup of mapping structure is saved to mapping_structure.json")
        except KeyboardInterrupt:
            print("Migration process was interrupted by the user.")

    main()
