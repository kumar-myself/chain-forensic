import os

# Directories
SOURCE_DATA_URL = os.environ.get("SOURCE_DATA_URL")
OUTPUT_DATA_DIR = os.environ.get("OUTPUT_DATA_DIR")
TOKEN = os.environ.get("TOKEN")
BATCH_DIR = f'{OUTPUT_DATA_DIR}url_batches/'
PROGRESS_FILE = f'progress.json'

# Input
INPUT_CSV = 'sitemap-1.csv'
INPUT_CSV_COLUMN = 'loc'
INPUT_URL_FILTER = '/address/'

# Scraper settings
BATCH_SIZE = 50
MAX_CONCURRENT = 2
MAX_RETRIES = 2
