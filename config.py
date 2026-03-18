import os

# Input
INPUT_CSV = 'sitemap-1.csv'
INPUT_CSV_COLUMN = 'loc'
INPUT_URL_FILTER = '/address/'

# Scraper settings
BATCH_SIZE = 50
MAX_CONCURRENT = 2
MAX_RETRIES = 2


# Directories
BASE_DIR = os.environ.get("BASE_DIR")
# Derived paths
SOURCE_DIR  = f"{BASE_DIR}input/"
SOURCE_FILE = f"{SOURCE_DIR}{INPUT_CSV}"

OUTPUT_DIR  = f"{BASE_DIR}output/"
BATCH_DIR   = f"{OUTPUT_DIR}url_batches-0/"
PROCESSED_BATCH_DIR = f"{OUTPUT_DIR}processed-0/"
PROGRESS_DIR = "https://api.github.com/repos/kumar-myself/chain-forensic/contents/"


# OUTPUT_DIR  = f"{BASE_DIR}output/"
# BATCH_DIR   = f"{OUTPUT_DIR}url_batches/"
# PROCESSED_BATCH_DIR = f"{OUTPUT_DIR}processed/"
# PROGRESS_DIR = "https://api.github.com/repos/kumar-myself/chain-forensic/contents/"

TOKEN = os.environ.get("TOKEN")
PROGRESS_FILE = f'progress-0.json'

MONGO_USER       = os.environ.get("MONGO_USER")
MONGO_PASSWORD   = os.environ.get("MONGO_PASSWORD")
MONGO_HOST       = os.environ.get("MONGO_HOST")
MONGO_DB         = os.environ.get("MONGO_DB")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION")
