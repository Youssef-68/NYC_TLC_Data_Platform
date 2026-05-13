from warehouse.db import get_conn
from pyspark.sql import SparkSession
from config.config import GOLD_DIR


def load_vendor(cur, spark):
    df = spark.read.parquet(f"{GOLD_DIR}/dim_vendor")
    data = [(r["vendor_id"],) for r in df.collect()]
    cur.executemany(
        "INSERT INTO dim_vendor (vendor_id) VALUES (%s) ON CONFLICT DO NOTHING",
        data
    )


def load_date(cur, spark):
    df = spark.read.parquet(f"{GOLD_DIR}/dim_date")

    data = [
        (int(r["year"]), int(r["month"]))
        for r in df.collect()
    ]

    cur.executemany(
        """
        INSERT INTO dim_date (year, month)
        VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        """,
        data
    )


def run():
    print("Loading dimensions from GOLD...")

    spark = SparkSession.builder.getOrCreate()

    conn = get_conn()
    cur = conn.cursor()

    try:
        load_vendor(cur, spark)
        load_date(cur, spark)

        conn.commit()
        print("Dimensions loaded from GOLD")

    except Exception as e:
        conn.rollback()
        print("Error in dimensions:", e)

    finally:
        cur.close()
        conn.close()