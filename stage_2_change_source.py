import asyncio
import json
import os
from typing import Union

import aiofiles
from supervisely.api.image_api import ImageApi
from supervisely.api.video.video_api import VideoApi
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

import supervisely as sly
from config import BUCKET_NAME, ENDPOINT_PATH, MAPS_DIR_L, SEMAPHORE_SIZE, api, logger


def list_files_in_directory(directory: str) -> list:
    """List all files in a directory
    except for the ones that contain "_failed.json" in their name.

    :param directory: Path to the directory
    :type directory: str
    :return: List of file paths
    :rtype: list
    """
    try:
        return [
            os.path.join(directory, file_name)
            for file_name in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, file_name))
            and "_failed.json" not in file_name
        ]
    except Exception as e:
        print(f"An error occurred while listing files in directory {directory}: {e}")
        return []


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, max=60),
    before_sleep=before_sleep_log(logger, logger.level),
)
def change_source(entity_api: Union[ImageApi, VideoApi], entity_ids: list, dst_paths: list):
    """Change the source of the entities.

    :param entity_api: Image or Video API
    :type entity_api: Union[ImageApi, VideoApi]
    :param entity_ids: List of entity IDs
    :type entity_ids: list
    :param dst_paths: List of paths to files that have been copied
    :type dst_paths: list
    """
    response = entity_api.set_remote(entity_ids, dst_paths)
    if not response.get("success"):
        raise Exception(f"Failed to set new paths for entities: {entity_ids}")
    return response


def get_entity_api(file_path: str) -> Union[ImageApi, VideoApi]:
    """
    Get the entity API based on the project type.
    This information is extracted from the project file name.

    :param file_path: Path to the project JSON file
    :type file_path: str
    :return: Image or Video API
    :rtype: Union[ImageApi, VideoApi]
    """
    project_type = file_path.split("/")[-1].split("-")[1].split(".")[0]
    if project_type == str(sly.ProjectType.IMAGES):
        entity_api = api.image
    elif project_type == str(sly.ProjectType.VIDEOS):
        entity_api = api.video
    else:
        raise ValueError(f"Unsupported project type: {project_type}")
    return entity_api


def get_project_id(file_path: str) -> str:
    """
    Get the project ID from the project file name.

    :param file_path: Path to the project JSON file
    :type file_path: str
    :return: Project ID
    :rtype: str
    """
    return file_path.split("/")[-1].split("-")[0]


async def open_json_file(file_path: str) -> dict:
    """
    Open the JSON file and return its content.

    :param file_path: Path to the JSON file
    :type file_path: str
    :return: JSON content
    :rtype: dict
    """
    try:
        async with aiofiles.open(file_path, "r") as file:
            content = await file.read()
            return json.loads(content)
    except Exception as e:
        logger.warning(f"An error occurred while opening the file {file_path}: {e}")
        return {}


async def switch_files_source(project_file_path: str, semaphore: asyncio.Semaphore, progress: tqdm):
    """
    Switch the source of the files from local supervisely storage to custom configured storage.

    :param project_file_path: Path to the project JSON file
    :type project_file_path: str
    :param semaphore: Semaphore to limit the number of concurrent tasks
    :type semaphore: asyncio.Semaphore
    :param progress: Progress bar
    :type progress: tqdm
    """
    async with semaphore:
        entity_api = get_entity_api(project_file_path)
        project_id = get_project_id(project_file_path)
        items_dict = await open_json_file(project_file_path)

        entity_ids = []
        dst_paths = []
        for entity_id, info in items_dict.items():
            try:
                if info["status"] == "success":
                    link: str = info["dst_path"]
                    link = link.replace(ENDPOINT_PATH, f"fs://{BUCKET_NAME}")
                    dst_paths.append(link)
                    entity_ids.append(int(entity_id))
            except Exception as e:
                logger.warning(f"An error occurred while processing entity: {entity_id}: {e}")

        if len(entity_ids) > 0:
            for e_ids_batch, dst_paths_batch in zip(
                sly.batched(entity_ids, batch_size=1000),
                sly.batched(dst_paths, batch_size=1000),
            ):
                change_source(entity_api, e_ids_batch, dst_paths_batch)
            logger.info(
                f"Project ID: {project_id} completed to migrate {len(entity_ids)} entities."
            )
        else:
            logger.info(f"No entities to migrate for project ID: {project_id}")
        progress.update(1)


async def main():
    project_files = list_files_in_directory(MAPS_DIR_L)
    semaphore = asyncio.Semaphore(SEMAPHORE_SIZE)
    progress = tqdm(total=len(project_files), desc="Processing projects")
    tasks = [switch_files_source(project, semaphore, progress) for project in project_files]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
