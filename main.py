from config.logging_config import setup_logging
from src.ingestion.ingestion import run_ingestion
from src.processing.bronze import run_bronze
from src.processing.silver import run_silver

def run_pipeline():
    logger = setup_logging()

    try:
        logger.info("PIPELINE STARTED")

        logger.info("INGESTION")
        run_ingestion()

        logger.info("BRONZE")
        run_bronze()

        logger.info("SILVER")
        run_silver()

        logger.info("PIPELINE COMPLETED SUCCESSFULLY")

    except Exception as e:
        logger.error(f"PIPELINE FAILED: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()