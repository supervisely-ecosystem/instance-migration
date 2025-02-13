import os

from dotenv import load_dotenv
from supervisely import Api
from supervisely import fs as fs
from supervisely.sly_logger import add_default_logging_into_file

api = Api.from_env()

# TODO Uncomment the line below to use the local environment variables via the .env file.
# load_dotenv("local.env")

DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "INFO")

# Path to the directory with the data on the local machine where the instance is running.
DATA_PATH = os.getenv("DATA_PATH")

# "fs endpoint" must be mounted to the same machine where the instance is running.
# Look for it in Instance Settings -> Cloud Credentials.
ENDPOINT_PATH = os.getenv("ENDPOINT_PATH")

# Root directory name, where all the data will be stored during the Stage 1.
ROOT_DIR_NAME = os.getenv("ROOT_DIR_NAME")

# Bucket name. Look for it in Instance Settings -> Cloud Credentials.
BUCKET_NAME = os.getenv("BUCKET_NAME")

# Number of concurrent tasks to run in parallel.
SEMAPHORE_SIZE = int(os.getenv("SEMAPHORE_SIZE", "10"))

# Number of retries for failed items
MAX_RETRY = int(os.getenv("MAX_RETRY", "3"))

required_vars = {
    "DATA_PATH": DATA_PATH,
    "ENDPOINT_PATH": ENDPOINT_PATH,
    "ROOT_DIR_NAME": ROOT_DIR_NAME,
    "BUCKET_NAME": BUCKET_NAME,
}

missing_vars = [var for var, value in required_vars.items() if value is None]

if missing_vars:
    raise ValueError(
        f"The following required environment variables are not set: {', '.join(missing_vars)}. "
        "If you are using the 'local.env' file, make sure the line loading envs from this file is uncommented in config.py."
    )

# Local path to store project maps
MAPS_DIR_L = os.path.join(os.getcwd(), ROOT_DIR_NAME, "maps")
# Destination path to move project maps
MAPS_DIR_R = os.path.join(ENDPOINT_PATH, ROOT_DIR_NAME, "maps")
LOGS_DIR = "./logs/"
STORAGE_DIR_NAME = "storage"

logger = api.logger
logger.setLevel(DEBUG_LEVEL)
fs.ensure_base_path(LOGS_DIR)
add_default_logging_into_file(logger, LOGS_DIR)
