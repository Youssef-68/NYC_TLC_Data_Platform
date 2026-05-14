import sys
from pathlib import Path

PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
sys.path.append(PROJECT_ROOT)
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

from src.ingestion.ingestion import run_ingestion
from src.processing.bronze import run_bronze
from src.processing.silver import run_silver
from src.processing.gold import run_gold


def safe_run(task_func, task_name):
    try:
        logging.info(f"Starting {task_name}")
        task_func()
        logging.info(f"{task_name} completed")
    except Exception as e:
        logging.error(f"{task_name} failed: {str(e)}")


def run_ingestion_task():
    safe_run(run_ingestion, "ingestion")


def run_bronze_task():
    safe_run(run_bronze, "bronze")


def run_silver_task():
    safe_run(run_silver, "silver")


def run_gold_task():
    safe_run(run_gold, "gold")


with DAG(
    dag_id="tlc_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    ingestion = PythonOperator(
        task_id="ingestion",
        python_callable=run_ingestion_task
    )

    bronze = PythonOperator(
        task_id="bronze",
        python_callable=run_bronze_task
    )

    silver = PythonOperator(
        task_id="silver",
        python_callable=run_silver_task
    )

    gold = PythonOperator(
        task_id="gold",
        python_callable=run_gold_task
    )

    ingestion >> bronze >> silver >> gold