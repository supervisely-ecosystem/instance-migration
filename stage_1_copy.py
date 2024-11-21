import argparse
import asyncio
import base64
import hashlib
import json
import logging
import math
import os
import shutil
import time
from datetime import datetime
from typing import List, Optional, Union

import aiofiles
import aiofiles.os
import supervisely as sly
from json_repair import repair_json
from supervisely.api.api import ApiField
from supervisely.api.image_api import ImageApi
from supervisely.api.video.video_api import VideoApi, VideoInfo
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

from config import (
    DATA_PATH,
    ENDPOINT_PATH,
    MAPS_DIR_L,
    MAPS_DIR_R,
    MAX_RETRY,
    ROOT_DIR_NAME,
    STORAGE_DIR_NAME,
    api,
    logger,
)


class HashMismatchError(Exception):
    def __init__(self, expected_hash, remote_hash):
        self.expected_hash = expected_hash
        self.actual_hash = remote_hash
        super().__init__(f"Hash mismatch: expected {expected_hash}, got {remote_hash}")


class ProjectItemsMap:
    """Class to manage project items mapping and copy items to destination storage"""

    def __init__(self, api: sly.Api, project_id: int, project_type: str = None):
        self.structure = {}
        self.api = api
        self.project_id = project_id
        self.project_type = project_type
        self.items_file = os.path.join(MAPS_DIR_L, f"{self.project_id}-{self.project_type}.json")
        self.failed_items_file = os.path.join(
            MAPS_DIR_L, f"{self.project_id}-{self.project_type}_failed.json"
        )
        self.buffer = {}
        self.buffer_size = 400
        self.last_flush_time = time.time()
        self.flush_interval = 20
        self.api_semaphore = asyncio.Semaphore(10)
        self.failed_items = {}  # to collect failed items while processing
        self.ds_retry_attempt = 0  # number of retry attempts for failed items in DS
        self.loop = sly.utils.get_or_create_event_loop()
        if self.loop.is_running():
            asyncio.create_task(self.initialize())
        else:
            asyncio.run(self.initialize())

    async def initialize(self):
        """Initialize the project items map from the file"""
        try:
            async with aiofiles.open(self.items_file, "r") as f:
                content = await f.read()
                if content.strip():  # Check if the file is not empty
                    self.structure.update(json.loads(content))
        except FileNotFoundError:
            sly.fs.ensure_base_path(self.items_file)
        except Exception as e:
            logger.error(f"Error initializing ProjectItemsMap: {e}")

    def flush_buffer(self):
        """Flush the buffer with Item Infos to the file"""
        if self.buffer:
            temp_file = f"{self.items_file}.tmp"
            try:
                try:
                    with open(self.items_file, "r") as f:
                        content = f.read()
                        data = json.loads(content) if content.strip() else {}
                except FileNotFoundError:
                    data = {}
                data.update(self.buffer)

                with open(temp_file, "w") as f:
                    f.write(json.dumps(data, indent=4))

                os.replace(temp_file, self.items_file)
                logger.debug(f"Items after flush: {len(data)}. File {self.items_file}")
                self.buffer.clear()
                self.last_flush_time = time.time()
            except Exception as e:
                logger.error(f"Error flushing buffer to file: {e}")
                if os.path.exists(temp_file):
                    os.remove(temp_file)

    async def update_file(self, key: str, value: dict):
        """Update the file with the given key and value
        :param key: Item ID
        :type key: str
        :param value: Item Info dict
        :type value: dict
        """
        self.buffer[str(key)] = value
        if (
            len(self.buffer) >= self.buffer_size
            or (time.time() - self.last_flush_time) >= self.flush_interval
        ):
            logger.debug(
                f"Flushing buffer: buffer size = {len(self.buffer)}, time since last flush = {time.time() - self.last_flush_time}"
            )
            self.flush_buffer()

    @staticmethod
    async def calculate_hash(file_path: str) -> str:
        """Calculate the hash of the file at the given path

        :param file_path: Path to the file
        :type file_path: str
        :return: Base64 encoded hash value
        :rtype: str
        """
        hash_algo = hashlib.sha256()
        async with aiofiles.open(file_path, "rb") as f:
            while True:
                chunk = await f.read(8192)
                if not chunk:
                    break
                hash_algo.update(chunk)
        return base64.b64encode(hash_algo.digest()).decode("utf-8")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def copy_file_to_dst(self, item_dict: dict):
        """Copy the file from the source path to the destination path

        :param item_dict: Item Info dict
        :type item_dict: dict
        """
        try:
            src_path = item_dict.get("src_path")
            dst_path = item_dict.get("dst_path")
            hash_value = item_dict.get("hash")
            await aiofiles.os.makedirs(os.path.dirname(dst_path), exist_ok=True)
            shutil.copy(src_path, dst_path)
            logger.trace(f"Moved file from {src_path} to {dst_path}")
            if not hash_value:
                local_hash = await self.calculate_hash(src_path)
                item_dict["hash"] = local_hash
            remote_hash = await self.calculate_hash(dst_path)
            if remote_hash != local_hash:
                raise HashMismatchError(hash_value, remote_hash)
        except Exception as e:
            logger.error(f"Error copying file from {src_path} to {dst_path}: {e}")
            raise

    async def list_dataset_items(
        self, dataset_info: sly.DatasetInfo, project_type: str
    ) -> List[Union[sly.ImageInfo, VideoInfo]]:
        """List items in the dataset with optimized API calls

        :param dataset_info: Dataset Info object
        :type dataset_info: :class:`DatasetInfo`
        :param project_type: Project type
        :type project_type: str
        :return: List of items in the dataset
        :rtype: List[Union[ImageInfo, VideoInfo]]
        """
        items_api = self.detect_api(project_type)
        fields = [ApiField.ID, ApiField.TITLE, ApiField.PATH_ORIGINAL, ApiField.DATASET_ID]

        items = await get_list_optimized(
            dataset_info,
            items_api,
            fields=fields,
            force_metadata_for_links=False,
            semaphore=self.api_semaphore,
        )
        return items

    async def copy_items(
        self,
        dataset: dict,  # dataset: {"path": str, "info": sly.DatasetInfo}
        project: sly.ProjectInfo,
        workspace_id: int,
        team_id: int,
    ):
        """
        Copy items in the dataset to the destination path and save project items mapping to the file.
        Destination path is constructed as:
        {IM_DIR_R}/{SLYM_DIR_R}/{team_id}-{team_name}/{workspace_id}-{workspace_name}/{project_id}-{project_name}/{dataset_id}-{dataset_name}/{item_id}-{item_name}

        :param dataset: Dataset dict with path and info
        :type dataset: dict
        :param project: Project Info object
        :type project: :class:`ProjectInfo`
        :param workspace_id: Workspace ID
        :type workspace_id: int
        :param team_id: Team ID
        :type team_id: int
        """
        dataset_dir = dataset.get("path")
        dataset_info: sly.DatasetInfo = dataset.get("info")
        items = await self.list_dataset_items(dataset_info, project.type)

        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=4, max=10),
            before_sleep=before_sleep_log(logger, logging.WARNING),
        )
        async def process_item(item: Union[sly.ImageInfo, VideoInfo], dataset_progress: tqdm):
            item_id = str(item.id)
            if self.structure.get(item_id, {}).get("status") == "success":
                dataset_progress.update(1)
                return

            item_path = os.path.join(dataset_dir, f"{item_id}-{item.name}")
            item_dict = {
                "name": item.name,
                "dst_path": item_path,
                "src_path": os.path.join(
                    DATA_PATH, STORAGE_DIR_NAME, item.path_original.lstrip("/")
                ),
                "dataset_id": dataset_info.id,
                "project_id": project.id,
                "workspace_id": workspace_id,
                "team_id": team_id,
                "hash": item.hash,
            }
            try:
                await self.copy_file_to_dst(item_dict)
                item_dict["status"] = "success"
            except Exception as e:
                if item.id not in self.failed_items:
                    self.failed_items[item.id] = item_dict
                item_dict["status"] = "failed"
                logger.trace(f"Failed to copy item {item_id} to dst: {e}")
            finally:
                await self.update_file(item_id, item_dict)
            dataset_progress.update(1)

        if items:
            filtered_items = [item for item in items if item.link is None]
            dataset_progress = tqdm(
                total=len(filtered_items), desc=f"Process items in dataset", leave=False
            )
            item_tasks = [process_item(item, dataset_progress) for item in filtered_items]
            await asyncio.gather(*item_tasks)
        else:
            logger.debug(f"No items found for dataset ID: {dataset_info.id}")

    async def try_to_copy_failed_items(self):
        """This method is called when there are failed items in the project after the initial copy."""

        async def process_item(item_id: int, item_info: dict, project_progress: tqdm):
            try:
                await self.copy_file_to_dst(item_info)
                self.failed_items.pop(item_id)
                item_info["status"] = "success"
            except Exception as e:
                item_info["status"] = "failed"
                logger.trace(f"Failed to copy item {item_id} to dst: {e}")
            finally:
                await self.update_file(item_id, item_info)
            project_progress.update(1)

        if self.failed_items:
            project_progress = tqdm(
                total=len(self.failed_items), desc=f"Process failed items in project", leave=False
            )
            item_tasks = [
                process_item(item_id, item_info, project_progress)
                for item_id, item_info in self.failed_items.items()
            ]
            await asyncio.gather(*item_tasks)
            project_progress.close()
        else:
            logger.debug("No failed items to process.")

    def detect_api(self, project_type: str) -> Union[ImageApi, VideoApi]:
        """
        Detect the API object based on the project type

        :param project_type: Project type
        :type project_type: str
        :return: API object
        :rtype: Union[ImageApi, VideoApi]
        """
        if project_type == sly.ProjectType.IMAGES:
            return self.api.image
        elif project_type == sly.ProjectType.VIDEOS:
            return self.api.video
        else:
            raise ValueError(f"Project type '{project_type}' is not supported")


def load_json_file_safely(file_path: str) -> dict:
    """
    Load JSON file safely and return the fixed data.
    Uses the repair_json function from json_repair package to fix the JSON file.

    :param file_path: Path to the JSON file
    :type file_path: str
    :return: Fixed data from the JSON file
    :rtype: dict
    """
    fixed_data = {}
    try:
        with open(file_path, "r") as f:
            content = f.read()
            if content == "":
                return fixed_data
            fixed_data_temp = repair_json(content, return_objects=True)
            if fixed_data_temp != {}:
                fixed_data = json.loads(fixed_data_temp)
    except Exception as e:
        logger.error(f"Failed to load JSON file {file_path} due to error: {e}")
    return fixed_data


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10),
    before_sleep=before_sleep_log(logger, logging.WARNING),
)
async def get_list_idx_page(
    item_api: Union[ImageApi, VideoApi],
    method: str,
    data: dict,
    semaphore: asyncio.Semaphore,
) -> List[Union[sly.ImageInfo, VideoInfo]]:
    """
    Get the list of items for a given page number in the common list of all entities in the dataset.

    :param item_api: API object to use for listing items.
    :type item_api: Union[ImageApi, VideoApi]
    :param method: Method to call for listing items.
    :type method: str
    :param data: Data to pass to the API method.
    :type data: dict
    :param semaphore: Semaphore object to limit the number of concurrent requests.
    :type semaphore: :class:`asyncio.Semaphore`
    :return: List of items from the API.
    :rtype: List[Union[ImageInfo, VideoInfo]]
    """
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
) -> List[Union[sly.ImageInfo, VideoInfo]]:
    """
    List of Images in the given dataset with optimized API calls.

    :param dataset_id: :class:`Dataset<supervisely.project.project.Dataset>` ID in which the Images are located.
    :type dataset_id: :class:`int`:
    :param item_api: API object to use for listing items.
    :type item_api: :class:`Api`:
    :param force_metadata_for_links: If True, updates meta for images with remote storage links when listing.
    :type force_metadata_for_links: bool, optional
    :param fields: List of fields to return. If None, returns all fields.
    :type fields: List[str], optional
    :param per_page: Number of items to return per page.
    :type per_page: int, optional
    :param semaphore: Semaphore object to limit the number of concurrent requests.
    :type semaphore: :class:`asyncio.Semaphore`, optional
    :return: List of objects with image information from Supervisely.
    :rtype: :class:`List[ImageInfo]<ImageInfo>`
    """
    tasks = []
    total_pages = math.ceil(dataset_info.items_count / per_page)
    method = "images.list" if isinstance(item_api, ImageApi) else "videos.list"
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
            if method == "videos.list":
                fields.extend(["dataId", "remoteDataId"])  # TODO revome after API fix
            data[ApiField.FIELDS] = fields
        if method == "images.list":
            data[ApiField.PROJECT_ID] = dataset_info.project_id
        tasks.append(get_list_idx_page(item_api, method, data, semaphore))
    items = await asyncio.gather(*tasks)
    return [item for sublist in items for item in sublist]


def filter_projects(projects: List[sly.ProjectInfo], types=[sly.ProjectType.IMAGES, sly.ProjectType.VIDEOS]):
    return [project for project in projects if project.type in types]


def get_team_dir(team: sly.TeamInfo) -> str:
    return os.path.join(ENDPOINT_PATH, ROOT_DIR_NAME, f"{team.id}-{team.name}")


def get_workspace_dir(workspace: sly.WorkspaceInfo, team_dir: str) -> str:
    return os.path.join(team_dir, f"{workspace.id}-{workspace.name}")


def get_project_dir(project: sly.ProjectInfo, workspace_dir: str) -> str:
    return os.path.join(workspace_dir, f"{project.id}-{project.name}")


def flatten_datasets_tree(datasets_tree: dict, project_dir: str) -> dict:
    """Flatten the datasets tree structure (nested datasets) to a flat mapping with dataset ID as key and destination path as value"""
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
    """Get the datasets in the project including nested datasets as a flat mapping
    with dataset ID as key and destination path as value

    :param project: Project Info object
    :type project: :class:`ProjectInfo`
    :param project_dir: Destination path for the project
    :type project_dir: str
    :return: Flat mapping of datasets in the project
    :rtype: dict
    """
    dataset_tree = api.dataset.get_tree(project.id)
    return flatten_datasets_tree(dataset_tree, project_dir)


async def collect_project_items_and_move():
    """Collect information about all instance project items and move them to destination path.
    Files will be stored in the human-readable structure.
    """
    try:
        teams = api.team.get_list()
        for team in tqdm(teams, desc="Teams"):
            team_dir = get_team_dir(team)
            workspaces = api.workspace.get_list(team.id)
            for workspace in tqdm(workspaces, desc="Workspaces", leave=False):
                workspace_dir = get_workspace_dir(workspace, team_dir)
                projects = api.project.get_list(workspace.id)
                projects_filtered = filter_projects(projects)
                for project in tqdm(projects_filtered, desc="Projects", leave=False):
                    project_map = ProjectItemsMap(api, project.id, project.type)
                    project_dir = get_project_dir(project, workspace_dir)
                    flatten_datasets_structure = get_datasets(project, project_dir)
                    for dataset in tqdm(
                        flatten_datasets_structure.values(), desc="Datasets", leave=False
                    ):
                        await project_map.copy_items(dataset, project, workspace.id, team.id)
                    project_map.ds_retry_attempt = 0
                    while project_map.ds_retry_attempt < MAX_RETRY and project_map.failed_items:
                        await project_map.try_to_copy_failed_items()
                        project_map.ds_retry_attempt += 1
                    if project_map.failed_items:
                        try:
                            with open(project_map.failed_items_file, "r") as f:
                                failed_items_json = json.load(f)
                        except FileNotFoundError:
                            failed_items_json = {}
                        except json.JSONDecodeError:
                            logger.error(
                                f"Failed to load JSON file {project_map.failed_items_file} due to JSONDecodeError. Attempting to fix..."
                            )
                            failed_items_json = load_json_file_safely(project_map.failed_items_file)

                        failed_items_json.update(project_map.failed_items)
                        with open(project_map.failed_items_file, "w") as f:
                            f.write(json.dumps(failed_items_json, indent=4))
                        logger.warning(
                            f"Failed to process {len(project_map.failed_items)} item(s) in project ID: {project.id}, "
                            f"to retry please run the script again with 'only_failed' parameter after this run."
                        )

                    if project_map.buffer:
                        logger.debug(
                            f"Flushing buffer on project completion: buffer size = {len(project_map.buffer)}"
                        )
                        project_map.flush_buffer()
                    logger.debug(f"Items map is saved for project ID: {project.id}")
    except Exception as e:
        logger.error(f"Error in collect_project_items_and_move: {e}")
        raise


def list_failed_projects(directory: str) -> list:
    """List all the failed projects in the given directory
    and return the list of paths to the files with failed items.

    :param directory: Directory path to list files
    :type directory: str
    :return: List of paths to files with failed items

    """
    try:
        return [
            os.path.join(directory, file_name)
            for file_name in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, file_name)) and "_failed.json" in file_name
        ]
    except Exception as e:
        print(f"An error occurred while listing files in directory {directory}: {e}")
        return []


def get_project_id_and_type(file_path: str) -> tuple:
    """Get the project ID and type from the file path"""
    p_id = file_path.split("/")[-1].split("-")[0]
    p_type = file_path.split("/")[-1].split("-")[1].split(".")[0].replace("_failed", "")
    return p_id, p_type


async def process_failed_projects():
    """Process failed items in the projects.
    Run this method after the initial run to retry process failed items in the projects.
    """
    try:
        failed_project_files = list_failed_projects(MAPS_DIR_L)
        if not failed_project_files:
            logger.info("No failed projects found.")
            return

        logger.info("Processing failed items in projects...")

        async def process_file(file):
            project_id, project_type = get_project_id_and_type(file)
            project_map = ProjectItemsMap(api, project_id, project_type)
            try:
                with open(file, "r") as f:
                    failed_items = json.load(f)
            except json.JSONDecodeError:
                logger.error(
                    f"Failed to load JSON file {file} due to JSONDecodeError. Attempting to fix..."
                )
                failed_items = load_json_file_safely(file)
            project_map.failed_items = failed_items
            await project_map.try_to_copy_failed_items()
            while project_map.ds_retry_attempt < MAX_RETRY and project_map.failed_items:
                await project_map.try_to_copy_failed_items()
                project_map.ds_retry_attempt += 1
            if project_map.failed_items:
                with open(file, "w") as f:
                    f.write(json.dumps(project_map.failed_items, indent=4))
                logger.warning(
                    f"Failed to process {len(project_map.failed_items)} item(s) in project ID: {project_id}, "
                    f"to retry please run the script again with 'only_failed' parameter after this run."
                )
            else:
                sly.fs.silent_remove(file)
            logger.debug(f"Items map is saved for project ID: {project_id}")

        tasks = [process_file(file) for file in tqdm(failed_project_files, desc="Projects")]
        await asyncio.gather(*tasks)
    except Exception as e:
        logger.error(f"Error in process_failed_projects: {e}")
        raise


def copy_maps_to_dst(local_path: str, dst_path: str):
    """Copy project maps to destination directory after processing

    :param local_path: Local path to the project maps
    :type local_path: str
    :param dst_path: Destination path to copy the project maps
    :type dst_path: str
    """
    try:
        if os.path.exists(dst_path):
            backup_path = f"{dst_path}_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            shutil.move(dst_path, backup_path)
        shutil.copytree(local_path, dst_path)
        logger.info(f"Maps are copied to destination folder: {dst_path}")
    except Exception as e:
        logger.warning(f"Maps are not copied to destination folder, error: {e}")


def main(only_failed: bool = False):
    try:
        loop = sly.utils.get_or_create_event_loop()
        if only_failed:
            loop.run_until_complete(process_failed_projects())
        else:
            loop.run_until_complete(collect_project_items_and_move())
            copy_maps_to_dst(MAPS_DIR_L, MAPS_DIR_R)

    except KeyboardInterrupt:
        logger.info("Script was stopped by the user.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process projects.")
    parser.add_argument("--only_failed", action="store_true", help="Process only failed items")
    args = parser.parse_args()
    main(only_failed=args.only_failed)
