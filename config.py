import os

DEBUG_LEVEL = os.getenv("DEBUG_LEVEL", "INFO")
STORAGE = os.getenv("SLY_SERVER_STORAGE")  # TODO Change to your NAS storage path
IM_DIR_R = os.getenv("IM_DIR_R")  # TODO Change to your NAS storage path
SLYM_DIR_R = os.getenv("SLYM_DIR_R")  # TODO Change to your NAS SLY storage directory
MAX_RETRY_ATTEMPTS = int(
    os.getenv("MAX_RETRY_ATTEMPTS", "3")
)  # Maximum number of retry attempts for failed items
MAPS_DIR_L = os.path.join(os.getcwd(), SLYM_DIR_R, "maps")  # local path to store the maps
MAPS_DIR_R = os.path.join(IM_DIR_R, SLYM_DIR_R, "maps")  # remote path to store the maps
LOGS_DIR = "./logs/"
