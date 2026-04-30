<<<<<<< HEAD
# NYC TLC End-to-End Data Platform

## Project Overview

This project implements a full data engineering pipeline using NYC Taxi & Limousine Commission (TLC) dataset.

It simulates a real-world **modern data platform architecture**:

- Data Lake (Raw → Bronze → Silver → Gold)
- Data Warehouse (DuckDB / ClickHouse)
- DBT Modeling (Star Schema)
- BI Layer (Power BI)
- Orchestration (Airflow-ready design)

---

## Architecture Flow

NYC TLC Data Source
↓
Ingestion (Python)
↓
Raw Layer (Parquet Storage)
↓
Bronze Layer (Basic cleaning)
↓
Silver Layer (Standardized dataset)
↓
DBT Models (Star Schema)
↓
Gold Layer (Analytics tables)
↓
Data Warehouse (DuckDB / ClickHouse)
↓
Power BI Dashboards
↓
Airflow Orchestration

---

## Project Structure

- config/ → Configuration & Logging
- data/ → Data Lake layers
- logs/ → Pipeline logs
- main.py → Pipeline orchestrator

---

## Tech Stack

- Python
- PySpark
- DuckDB / ClickHouse
- DBT
- Power BI
- Airflow (future integration)

---

## Goal

Build a production-like data platform end-to-end for analytics and BI.

---

## Author

Youssef Wael
Data Analyst / ML & Data Engineering Enthusiast
=======
# NYC_TLC_Data_Platform
NYC Taxi Data Engineering pipeline using Python &amp; PySpark implementing Medallion Architecture (Bronze, Silver, Gold). Handles large-scale parquet ingestion, schema inconsistencies, and builds analytics-ready datasets for BI and warehousing.
>>>>>>> f352f1987a386f8bbd0748da5317ee7f2788dc14
