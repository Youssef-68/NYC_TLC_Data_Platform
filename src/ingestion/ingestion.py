import sys
import os
import time
import requests
from concurrent.futures import ThreadPoolExecutor
from config.logging_config import setup_logging

# Add project root
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.config import *

logger = setup_logging()


# Confegrtions
MAX_WORKERS = 4
CHUNK_SIZE = 5 * 1024 * 1024  # 5MB
TIMEOUT = 30
RETRIES = 3
BASE_DATA_PATH = "data/raw"



# Session per Threads 
import threading

thread_local = threading.local()

def get_session():

    # Get a session per thread (thread-safe)

    if not hasattr(thread_local, "session"):
        session = requests.Session()

        adapter = requests.adapters.HTTPAdapter(
            max_retries=RETRIES,
            pool_connections=MAX_WORKERS,
            pool_maxsize=MAX_WORKERS
        )

        session.mount("http://", adapter)
        session.mount("https://", adapter)

        thread_local.session = session

    return thread_local.session


# Download Function
def download_file(task):

    # Download a file with retries, streaming, and backoff

    url, path = task
    session = get_session()

    # Skip existing files
    if os.path.exists(path):
        logger.info(f"Exists: {path}")
        return

    for attempt in range(RETRIES):
        try:
            logger.info(f"Downloading: {url}")

            with session.get(url, stream=True, timeout=TIMEOUT) as response:
                response.raise_for_status()

                with open(path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                        if chunk:
                            f.write(chunk)

            logger.info(f"Saved: {path}")
            return

        except requests.exceptions.RequestException as e:
            wait_time = 2 ** attempt
            logger.warning(
                f"Retry {attempt+1}/{RETRIES} failed: {url} | {e} | waiting {wait_time}s"
            )
            time.sleep(wait_time)

        except Exception as e:
            logger.error(f"Unexpected error: {url} | {e}")
            return

    logger.error(f"Failed permanently: {url}")



# Task Builder
def build_tasks():

    # Build ingestion tasks dynamically
    tasks = []

    for dataset in DATASETS:
        for year in YEARS:
            for month in MONTHS:

                month_str = str(month).zfill(2)

                filename = f"{dataset}_tripdata_{year}-{month_str}.parquet"
                url = f"{BASE_URL}/{filename}"

                folder = os.path.join(
                    BASE_DATA_PATH,
                    dataset,
                    f"year={year}",
                    f"month={month_str}"
                )

                os.makedirs(folder, exist_ok=True)

                save_path = os.path.join(folder, filename)

                tasks.append((url, save_path))

    return tasks


# Run Pipelines
def run_ingestion():
    
    # Execute ingestion using multithreading
    tasks = build_tasks()
    logger.info(f"Total files: {len(tasks)}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        list(executor.map(download_file, tasks))


# Entry Points
if __name__ == "__main__":
    logger.info("Starting ingestion pipeline")
    run_ingestion()
    logger.info("Pipeline finished successfully")