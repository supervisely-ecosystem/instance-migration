import argparse
import asyncio
import base64
import hashlib
import json
import math
import os
import shutil
import time
from collections import defaultdict
from typing import List, Optional, Union

import aiofiles
import supervisely as sly
from dotenv import load_dotenv
from supervisely.api.api import ApiField
from supervisely.api.image_api import ImageApi
from supervisely.api.video.video_api import VideoApi, VideoInfo
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

# -------------------------------- Global Variables For Migration -------------------------------- #

entity_api = None
download_api_url = None
entities_map = {}

api = sly.Api.from_env()
load_dotenv("local.env")
# ------------------------------------ Constants For Migration ----------------------------------- #

IM_DIR_R = os.getenv("IM_DIR_R")  # TODO Change to your NAS storage path
SLYM_DIR_R = os.getenv("SLYM_DIR_R")  # TODO Change to your NAS SLY storage directory
MAX_RETRY_ATTEMPTS = int(
    os.getenv("MAX_RETRY_ATTEMPTS", 3)
)  # Maximum number of retry attempts for failed items
MAPS_DIR_L = os.path.join(os.getcwd(), SLYM_DIR_R, "maps")  # local path to store the maps
MAPS_DIR_R = os.path.join(IM_DIR_R, SLYM_DIR_R, "maps")  # remote path to store the maps
FAILED_PROJECTS_FILE = os.path.join(  # file with failed projects to retry
    MAPS_DIR_L, "failed_projects.json"
)
# ----------------------------- Asynchronous Functions For Migration ----------------------------- #


class HashMismatchError(Exception):
    def __init__(self, expected_hash, remote_hash):
        self.expected_hash = expected_hash
        self.actual_hash = remote_hash
        super().__init__(f"Hash mismatch: expected {expected_hash}, got {remote_hash}")


class ProjectItemsMap:

    def __init__(self, api: sly.Api, project_id: int):
        self.structure = {}
        self.api = api
        self.project_id = project_id
        self.file_path = os.path.join(MAPS_DIR_L, f"{project_id}.json")
        self.lock = asyncio.Lock()
        self.buffer = {}
        self.buffer_size = 100
        self.last_flush_time = time.time()
        self.flush_interval = 20
        self.api_semaphore = asyncio.Semaphore(10)
        self.loop = sly.utils.get_or_create_event_loop()
        self.loop.run_until_complete(self.initialize())
        self.failed_items = {}  # to collect failed items while processing
        self.ds_retry_attempt = 0  # number of retry attempts for failed items in DS

    async def initialize(self):
        if os.path.exists(self.file_path):
            async with aiofiles.open(self.file_path, "r") as f:
                content = await f.read()
                if content.strip():  # Check if the file is not empty
                    self.structure.update(json.loads(content))
        else:
            sly.fs.ensure_base_path(self.file_path)

    # async def dump_to_file(self):
    #     async with aiofiles.open(self.file_path, "w") as f:
    #         await f.write(json.dumps(self.structure, indent=4))

    async def flush_buffer(self):
        async with self.lock:
            if self.buffer:
                if os.path.exists(self.file_path):
                    async with aiofiles.open(self.file_path, "r+") as f:
                        content = await f.read()
                        data = json.loads(content) if content.strip() else {}
                        data.update(self.buffer)

                        # Write only the updated data back to the file
                        await f.seek(0)
                        await f.write(json.dumps(data, indent=4))
                        await f.truncate()
                else:
                    async with aiofiles.open(self.file_path, "w") as f:
                        await f.write(json.dumps(self.buffer, indent=4))
                self.buffer.clear()
                self.last_flush_time = time.time()

    async def update_file(self, key: str, value: dict):
        self.buffer[str(key)] = value
        if (
            len(self.buffer) >= self.buffer_size
            or (time.time() - self.last_flush_time) >= self.flush_interval
        ):
            await self.flush_buffer()

    async def run_in_executor(self, func, *args):
        loop = sly.utils.get_or_create_event_loop()
        return await loop.run_in_executor(None, func, *args)

    @staticmethod
    async def calculate_hash(file_path: str) -> str:
        hash_algo = hashlib.sha256()
        async with aiofiles.open(file_path, "rb") as f:
            while True:
                chunk = await f.read(8192)
                if not chunk:
                    break
                hash_algo.update(chunk)
        return base64.b64encode(hash_algo.digest()).decode("utf-8")

    async def copy_to_nas(self, item_dict: dict):
        src_path = item_dict.get("src_path")
        dst_path = item_dict.get("dst_path")
        hash_value = item_dict.get("hash")
        shutil.copy(src_path, dst_path)
        sly.logger.debug(f"Moved file from {src_path} to {dst_path}")
        if not hash_value:
            local_hash = await self.calculate_hash(src_path)
            item_dict["hash"] = local_hash
        remote_hash = await self.calculate_hash(dst_path)
        if remote_hash != local_hash:
            raise HashMismatchError(hash_value, remote_hash)

    def list_items_batched(self, dataset_info: sly.DatasetInfo, project_type: str):
        items_api = self.detect_api(project_type)
        fields = [ApiField.ID, ApiField.TITLE, ApiField.PATH_ORIGINAL, ApiField.DATASET_ID]

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

    def copy_items(
        self,
        dataset: dict,  # dataset: {"path": str, "info": sly.DatasetInfo}
        project: sly.ProjectInfo,
        workspace_id: int,
        team_id: int,
    ):
        dataset_dir = dataset.get("path")
        dataset_info: sly.DatasetInfo = dataset.get("info")
        items = self.list_items_batched(dataset_info, project.type)

        async def process_item(item: Union[sly.ImageInfo, VideoInfo], dataset_progress: tqdm):
            if self.structure.get(str(item.id), {"status": None}).get("status") == "success":
                dataset_progress.update(1)
                return
            item_id = str(item.id)
            item_path = os.path.join(dataset_dir, f"{item_id}-{item.name}")
            item_dict = {
                "name": item.name,
                "dst_path": item_path,
                "src_path": item.path_original,
                "dataset_id": dataset_info.id,
                "project_id": project.id,
                "workspace_id": workspace_id,
                "team_id": team_id,
                "hash": item.hash,
            }
            try:
                await self.copy_to_nas(item_dict)
                item_dict["status"] = "success"
            except Exception:
                if item.id not in self.failed_items:
                    self.failed_items[item.id] = item_dict
                item_dict["status"] = "failed"
                sly.logger.debug(f"Failed to move item {item_id} to NAS")
            finally:
                await self.update_file(item_id, item_dict)
            dataset_progress.update(1)

        if len(items) > 0:
            filtered_items = [item for item in items if item.link is None]
            dataset_progress = tqdm(
                total=len(filtered_items), desc=f"Process items in dataset", leave=False
            )
            item_tasks = [process_item(item, dataset_progress) for item in filtered_items]
            self.loop.run_until_complete(asyncio.gather(*item_tasks))
            # dataset_progress.close()
        else:
            sly.logger.debug(f"No items found for dataset ID: {dataset_info.id}")

    def copy_failed_items(self):
        async def process_item(item_id: int, item_info: dict, project_progress: tqdm):
            src_path = item_info.get("src_path")
            dst_path = item_info.get("dst_path")
            item_hash = item_info.get("hash")
            try:
                await self.copy_to_nas(src_path, dst_path, item_hash)
                self.failed_items.pop(item_id)
                await self.update_file(item_id, item_info)
            except Exception:
                sly.logger.warning(f"Failed to move item {item_id} to NAS")
            project_progress.update(1)

        project_progress = tqdm(
            total=len(self.failed_items), desc=f"Process failed items in project", leave=False
        )
        item_tasks = [
            process_item(item_id, item_info, project_progress)
            for item_id, item_info in self.failed_items.items()
        ]
        self.loop.run_until_complete(asyncio.gather(*item_tasks))
        project_progress.close()

    def detect_api(self, project_type: str):
        if project_type == "images":
            return self.api.image
        elif project_type == "videos":
            return self.api.video
        else:
            raise ValueError(f"Project type '{project_type}' is not supported")


def load_json_file_safely(file_path: str) -> dict:
    """TODO fix"""
    fixed_data = {}
    try:
        with open(file_path, "r") as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    fixed_data.update(obj)
                except json.JSONDecodeError:
                    sly.logger.warning(f"Skipping invalid JSON line: {line.strip()}")
    except Exception as e:
        sly.logger.error(f"Failed to load JSON file {file_path} due to error: {e}")
    return fixed_data


async def get_list_idx_page(
    item_api: Union[ImageApi, VideoApi],
    method: str,
    data: dict,
    semaphore: asyncio.Semaphore,
):
    async with semaphore:
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
    method = "images.list" if isinstance(item_api, ImageApi) else "videos.list"
    if loop is None:
        loop = sly.utils.get_or_create_event_loop()
    for page_num in range(1, total_pages + 1):
        data = {
            ApiField.DATASET_ID: dataset_info.id,
            ApiField.SORT: "id",
            ApiField.SORT_ORDER: "asc",
            ApiField.FORCE_METADATA_FOR_LINKS: force_metadata_for_links,
            ApiField.PER_PAGE: per_page,
            ApiField.PAGE: page_num,
        }
        if fields is not None:
            data[ApiField.FIELDS] = fields
        if method == "images.list":
            data[ApiField.PROJECT_ID] = dataset_info.project_id
            task = get_list_idx_page(item_api, method, data, semaphore)
            tasks.append(task)
    items = await asyncio.gather(*tasks)
    return [item for sublist in items for item in sublist]


def filter_projects(projects: List[sly.ProjectInfo], types=["images", "videos"]):
    return [project for project in projects if project.type in types]


def get_team_dir(team: sly.TeamInfo) -> str:
    return os.path.join(IM_DIR_R, SLYM_DIR_R, f"{team.id}-{team.name}")


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
                project_map = ProjectItemsMap(api, project.id)
                project_dir = get_project_dir(project, workspace_dir)
                flatten_datasets_structure = get_datasets(project, project_dir)
                for dataset in tqdm(flatten_datasets_structure, desc="Datasets", leave=False):
                    project_map.ds_retry_attempt = 0
                    dataset = flatten_datasets_structure[dataset]
                    project_map.copy_items(dataset, project, workspace.id, team.id)
                while (
                    project_map.ds_retry_attempt < MAX_RETRY_ATTEMPTS
                    and len(project_map.failed_items) > 0
                ):
                    project_map.copy_failed_items()
                    project_map.ds_retry_attempt += 1
                if project_map.failed_items:
                    if os.path.exists(FAILED_PROJECTS_FILE):
                        with open(FAILED_PROJECTS_FILE, "r") as f:
                            try:
                                failed_projects = json.load(f)
                            except json.JSONDecodeError:
                                sly.logger.error(
                                    f"Failed to load JSON file {FAILED_PROJECTS_FILE} due to JSONDecodeError. Attempting to fix..."
                                )
                                failed_projects = load_json_file_safely(FAILED_PROJECTS_FILE)

                    else:
                        failed_projects = {}
                    failed_projects[project.id] = project_map.failed_items
                    with open(FAILED_PROJECTS_FILE, "w") as f:
                        json.dump(failed_projects, f, indent=4)
                    sly.logger.warning(
                        f"Failed to process {len(project_map.failed_items)} item(s) in project ID: {project.id}, "
                        f"to retry please run the script again with 'only_failed' parameter after this run."
                    )
                project_map.loop.run_until_complete(project_map.flush_buffer())
                sly.logger.debug(f"Items map is saved for project ID: {project.id}")


def process_failed_projects():
    if os.path.exists(FAILED_PROJECTS_FILE):
        with open(FAILED_PROJECTS_FILE, "r") as f:
            try:
                failed_projects = json.load(f)
            except json.JSONDecodeError:
                sly.logger.error(
                    f"Failed to load JSON file {FAILED_PROJECTS_FILE} due to JSONDecodeError. Attempting to fix..."
                )
                failed_projects = load_json_file_safely(FAILED_PROJECTS_FILE)
        project_ids = list(failed_projects.keys())
        if len(project_ids) > 0:
            sly.logger.info("Processing failed items in projects...")
            for project_id in tqdm(project_ids, desc="Projects"):
                project_map = ProjectItemsMap(api, project_id)
                project_map.ds_retry_attempt = 0
                project_map.copy_failed_items()
                while (
                    project_map.ds_retry_attempt < MAX_RETRY_ATTEMPTS
                    and len(project_map.failed_items) > 0
                ):
                    project_map.copy_failed_items()
                remained_items = project_map.failed_items[project_id]
                if len(remained_items) > 0:
                    sly.logger.warning(
                        f"Failed to process {len(remained_items)} item(s) in project ID: {project_id}, "
                        f"to retry please run the script again with 'only_failed' parameter after this run."
                    )
                else:
                    del failed_projects[project_id]
                    with open(FAILED_PROJECTS_FILE, "w") as f:
                        json.dump(failed_projects, f, indent=4)
                sly.logger.debug(f"Items map is saved for project ID: {project_id}")
    else:
        sly.logger.info("No failed projects found to retry.")


def copy_maps_to_nas(local_path: str, nas_path: str):
    try:
        shutil.copytree(local_path, nas_path)
        sly.logger.info(f"Maps are copied to NAS: {nas_path}")
    except Exception as e:
        sly.logger.warning(f"Maps are not copied to NAS, error: {e}")


def main(only_failed: bool = False):
    try:
        if only_failed:
            process_failed_projects()
        else:
            collect_project_items_and_move()
            # map = ProjectItemsMap(api, 286321)
            copy_maps_to_nas(MAPS_DIR_L, MAPS_DIR_R)

    except KeyboardInterrupt:
        sly.logger.info("Migration process was interrupted by the user.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process some projects.")
    parser.add_argument("--only_failed", action="store_true", help="Process only failed items")
    args = parser.parse_args()
    main(only_failed=args.only_failed)
