from warehouse.db import get_conn
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from config.config import GOLD_DIR
import tempfile


REQUIRED_COLUMNS = {
    "vendor_id": "int",
    "pickup_ts": "timestamp",
    "dropoff_ts": "timestamp",
    "pickup_location_id": "int",
    "dropoff_location_id": "int",
    "trip_distance": "double",
    "fare_amount": "double",
    "total_amount": "double",
    "year": "int",
    "month": "int",
}


# normalize column names
def normalize_columns(df):
    print("Initial columns:", df.columns)

    rename_map = {
        "VendorID": "vendor_id",
        "vendorid": "vendor_id",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
    }

    for old, new in rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)

    print("Normalized columns:", df.columns)
    return df


# ensure schema completeness
def ensure_schema(df):
    for col_name, dtype in REQUIRED_COLUMNS.items():
        if col_name not in df.columns:
            print(f"Column missing: {col_name} → filling NULL")
            df = df.withColumn(col_name, lit(None))
    return df


# safe type casting
def cast_columns(df):
    for col_name, dtype in REQUIRED_COLUMNS.items():
        df = df.withColumn(col_name, col(col_name).cast(dtype))
    return df


# load staging dataframe
def load_staging(spark):
    print("Reading parquet...")
    df = spark.read.parquet(f"{GOLD_DIR}/fact_trip")
    print("Initial columns:", df.columns)

    df = normalize_columns(df)
    df = ensure_schema(df)
    df = cast_columns(df)

    df = df.select(*REQUIRED_COLUMNS.keys())
    print("Final columns:", df.columns)

    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    df.toPandas().to_csv(temp_file.name, index=False)
    return temp_file.name


# copy to postgres
def copy_to_postgres(file_path):
    conn = get_conn()
    cur = conn.cursor()

    with open(file_path, "r") as f:
        cur.copy_expert(
            """
            COPY stg_trips (
                vendor_id,
                pickup_ts,
                dropoff_ts,
                pickup_location_id,
                dropoff_location_id,
                trip_distance,
                fare_amount,
                total_amount,
                year,
                month
            )
            FROM STDIN WITH CSV HEADER
            """,
            f
        )

    conn.commit()
    cur.close()
    conn.close()


# run pipeline step
def run():
    print("ETL - Loading staging table...")

    spark = SparkSession.builder.getOrCreate()

    file_path = load_staging(spark)
    copy_to_postgres(file_path)

    print("Staging loaded successfully")