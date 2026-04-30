from config.logging_config import setup_logging

from src.ingestion.ingestion import run_ingestion
from src.processing.bronze import run_bronze
from src.processing.silver import run_silver


def run_pipeline():
    logger = setup_logging()

    logger.info("Starting NYC TLC Data Pipeline")

    # 1. INGESTION
    logger.info("Step 1: Data Ingestion started")
    run_ingestion()

    # 2. RAW
    logger.info("Step 2: Raw Layer stored")

    # 3. BRONZE
    logger.info("Step 3: Bronze transformation")
    run_bronze()

    # 4. SILVER
    logger.info("Step 4: Silver clean dataset")
    run_silver()

    # 5. DBT (future)
    logger.info("Step 5: DBT modeling (star schema)")

    # 6. GOLD (future)
    logger.info("Step 6: Gold analytics tables")

    logger.info("Pipeline completed successfully")


if __name__ == "__main__":
    run_pipeline()