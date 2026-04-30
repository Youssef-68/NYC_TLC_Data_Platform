from pyspark.sql.functions import col, input_file_name
from config.config import RAW_DIR, BRONZE_DIR
from processing.spark_session import get_spark


def read_raw(spark):

    # Read raw parquet files (no transformations)

    path = f"{RAW_DIR}/*/year=*/month=*/"
    df = spark.read.parquet(path)

    print("Raw schema:")
    df.printSchema()

    print(f"Raw rows: {df.count()}")

    return df


def transform_bronze(df):
    
    # Minimal cleaning only (no schema enforcement here)
    if "airport_fee" in df.columns:
        df = df.drop("airport_fee")
    df = df.withColumn("source_file", input_file_name())

    return df


def write_bronze(df):

    # Write bronze layer as immutable raw snapshot
    output_path = BRONZE_DIR.as_posix()

    df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(output_path)

    print(f"Bronze written to: {output_path}")


def run_bronze():
    
    spark = get_spark()
    df = read_raw(spark)

    if df.rdd.isEmpty():
        print("No raw data found")
        return

    df_bronze = transform_bronze(df)

    print(f"Bronze rows: {df_bronze.count()}")

    write_bronze(df_bronze)

    print("Bronze layer completed")


if __name__ == "__main__":
    run_bronze()