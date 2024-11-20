import os

from dotenv import load_dotenv
from supervisely.sly_logger import add_default_logging_into_file

from supervisely import Api
from supervisely import fs as fs

api = Api.from_env()
load_dotenv("local.env")
DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "INFO")
DATA_PATH = os.getenv("DATA_PATH", "/supervisely/data")
IM_DIR_R = os.getenv("IM_DIR_R")
SLYM_DIR_R = os.getenv("SLYM_DIR_R")
SEMAPHORE_SIZE = int(os.getenv("SEMAPHORE_SIZE", "10"))  # Number of parallel threads
MAX_RETRY_ATTEMPTS = int(
    os.getenv("MAX_RETRY_ATTEMPTS", "3")
)  # Maximum number of retry attempts for failed items
MAPS_DIR_L = os.path.join(os.getcwd(), SLYM_DIR_R, "maps")  # local path to store the maps
MAPS_DIR_R = os.path.join(IM_DIR_R, SLYM_DIR_R, "maps")  # remote path to store the maps
LOGS_DIR = "./logs/"
STORAGE_DIR_NAME = "storage"


logger = api.logger
logger.setLevel(DEBUG_LEVEL)
fs.ensure_base_path(LOGS_DIR)
add_default_logging_into_file(logger, LOGS_DIR)
