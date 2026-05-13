# NYC TLC Data Engineering Platform

## Overview
End-to-end data engineering pipeline using NYC Taxi & Limousine Commission (TLC) dataset.

Implements a **Medallion Architecture**:
- Bronze: Raw ingestion layer
- Silver: Cleaned & standardized data
- Gold: Analytics-ready datasets
- Warehouse: Star Schema in PostgreSQL

---

## Architecture

NYC TLC Data Source  
→ Ingestion (Python + Requests)  
→ Raw Data (Parquet)  
→ Bronze Layer (Light cleaning)  
→ Silver Layer (Standardization)  
→ Gold Layer (Business-ready data)  
→ PostgreSQL Data Warehouse (Star Schema)

---

## Tech Stack
- Python
- PySpark
- PostgreSQL
- Pandas
- SQL

---

## Data Model
- Fact Table: `fact_trips`
- Dimensions: `dim_vendor`, `dim_date`

---

## How to Run
```bash
python main.py

```bash
python main.py
