from pathlib import Path

# BASE PATHS
BASE_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = BASE_DIR / "data"

RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Ensure directories exist
for path in [RAW_DIR, BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
    path.mkdir(parents=True, exist_ok=True)

# DATA SOURCE (NYC TLC)
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# Supported datasets (scalable)
DATASETS = ["yellow", "green"]  

# TIME PARTITIONING
YEARS = [2023, 2024, 2025]
MONTHS = list(range(1, 13))

# FILE NAMING
FILE_FORMAT = "{dataset}_tripdata_{year}-{month}.parquet"

# WAREHOUSE
WAREHOUSE_DIR = BASE_DIR / "warehouse"
WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

DUCKDB_PATH = WAREHOUSE_DIR / "nyc_tlc.duckdb"

# LOGGING
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "pipeline.log"