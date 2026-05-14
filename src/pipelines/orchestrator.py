from src.ingestion.ingestion import run_ingestion
from src.processing.bronze import run_bronze
from src.processing.silver import run_silver
from src.processing.gold import run_gold


def run_pipeline():
    print("Starting Full Pipeline")

    run_ingestion()
    run_bronze()
    run_silver()
    run_gold()

    print("Pipeline Completed Successfully")