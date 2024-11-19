import asyncio
import json
import os
from typing import Union

import supervisely as sly
from dotenv import load_dotenv
from supervisely.api.image_api import ImageApi
from supervisely.api.video.video_api import VideoApi
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential
from tqdm import tqdm

api = sly.Api.from_env()
load_dotenv("local.env")
SLYM_DIR_R = os.getenv("SLYM_DIR_R")
MAPS_DIR_L = os.path.join(os.getcwd(), SLYM_DIR_R, "maps")
RELINK_SEMAPHORE_SIZE = int(os.getenv("RELINK_SEMAPHORE_SIZE"))


def list_files_in_directory(directory: str) -> list:
    try:
        return [
            os.path.join(directory, file_name)
            for file_name in os.listdir(directory)
            if os.path.isfile(os.path.join(directory, file_name))
        ]
    except Exception as e:
        print(f"An error occurred while listing files in directory {directory}: {e}")
        return []


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, max=60),
    before_sleep=before_sleep_log(sly.logger, sly.logger.level),
)
def set_remote_with_retries(entity_api: Union[ImageApi, VideoApi], e_list: list, r_list: list):
    response = entity_api.set_remote(e_list, r_list)
    if not response.get("success"):
        raise Exception(f"Failed to set remote links for entities: {e_list}")
    return response


def get_entity_api(file_path: str) -> Union[ImageApi, VideoApi]:
    project_type = file_path.split("/")[-1].split("-")[1].split(".")[0]
    if project_type == str(sly.ProjectType.IMAGES):
        entity_api = api.image
    elif project_type == str(sly.ProjectType.VIDEOS):
        entity_api = api.video
    else:
        raise ValueError(f"Unsupported project type: {project_type}")
    return entity_api


def get_project_id(file_path: str) -> str:
    return file_path.split("/")[-1].split("-")[0]


def open_json_file(file_path: str) -> dict:
    try:
        with open(file_path, "r") as file:
            return json.load(file)
    except Exception as e:
        sly.logger.warning(f"An error occurred while opening the file {file_path}: {e}")
        return {}


async def change_files_source(project: str, semaphore: asyncio.Semaphore, progress: tqdm):
    """
    This main function migrates entities of the project to remote storage.

    :param project: Path to the project JSON file
    :type project: str
    """
    async with semaphore:
        entity_api = get_entity_api(project)
        items_dict = open_json_file(project)
        project_id = get_project_id(project)

        entity_list = []
        remote_links_list = []
        for entity_id, info in items_dict.items():
            try:
                if info["status"] == "success":
                    remote_links_list.append(info["dst_path"])
                    entity_list.append(int(entity_id))
            except Exception as e:
                sly.logger.warning(f"An error occurred while processing entity: {entity_id}: {e}")

        if len(entity_list) > 0:
            for e_list, r_list in zip(
                sly.batched(entity_list, batch_size=1000),
                sly.batched(remote_links_list, batch_size=1000),
            ):
                set_remote_with_retries(entity_api, e_list, r_list)
            sly.logger.info(
                f"Entities have been migrated to remote storage for project ID: {project_id}"
            )
        else:
            sly.logger.info(f"No entities to migrate for project ID: {project_id}")
        progress.update(1)


async def main():
    project_files = list_files_in_directory(MAPS_DIR_L)
    semaphore = asyncio.Semaphore(RELINK_SEMAPHORE_SIZE)
    progress = tqdm(total=len(project_files), desc="Processing projects")
    tasks = [change_files_source(project, semaphore, progress) for project in project_files]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
