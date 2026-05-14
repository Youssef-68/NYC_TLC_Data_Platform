from pyspark.sql.functions import col, input_file_name, lit
from config.config import BRONZE_DIR, SILVER_DIR
from processing.spark_session import get_spark
from pyspark.sql import functions as F


def read_bronze(spark):
    df = spark.read.parquet(BRONZE_DIR.as_posix())
    print("Bronze schema:")
    df.printSchema()
    print(f"Bronze rows: {df.count()}")
    return df


def apply_schema(df):

    casts = {
        "VendorID": "int",
        "passenger_count": "int",
        "trip_distance": "double",
        "fare_amount": "double",
        "total_amount": "double"
    }

    for c, t in casts.items():
        if c in df.columns:
            df = df.withColumn(c, col(c).cast(t))

    return df


def safe_col(df, name):
    return col(name) if name in df.columns else lit(None)


def transform_silver(df):

    df = apply_schema(df)

    df = df.withColumn("source_file", input_file_name())

    # detect dataset
    df = df.withColumn(
        "dataset",
        F.regexp_extract("source_file", r"(yellow|green)", 1)
    )

    # unified datetime (SAFE)
    df = df.withColumn(
        "pickup_datetime",
        F.coalesce(
            safe_col(df, "tpep_pickup_datetime"),
            safe_col(df, "lpep_pickup_datetime")
        )
    )

    df = df.withColumn(
        "dropoff_datetime",
        F.coalesce(
            safe_col(df, "tpep_dropoff_datetime"),
            safe_col(df, "lpep_dropoff_datetime")
        )
    )

    # optional: unify location ids
    df = df.withColumn(
        "pickup_location_id",
        F.coalesce(
            safe_col(df, "PULocationID"),
            safe_col(df, "pulocationid")
        )
    )

    df = df.withColumn(
        "dropoff_location_id",
        F.coalesce(
            safe_col(df, "DOLocationID"),
            safe_col(df, "dolocationid")
        )
    )

    # filters
    df = df.filter(
        (col("trip_distance") > 0) &
        (col("fare_amount") > 0)
    )

    return df


def write_silver(df):

    df.write \
        .mode("append") \
        .partitionBy("dataset") \
        .parquet(SILVER_DIR.as_posix())

    print("Silver written")


def run_silver():
    spark = get_spark()
    df = read_bronze(spark)

    if df.rdd.isEmpty():
        print("No bronze data")
        return

    df_silver = transform_silver(df)

    print(f"Silver rows: {df_silver.count()}")

    write_silver(df_silver)
    print("Silver done")