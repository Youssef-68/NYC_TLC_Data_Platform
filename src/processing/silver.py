from pyspark.sql.functions import col, input_file_name, lit
from config.config import BRONZE_DIR, SILVER_DIR
from processing.spark_session import get_spark


def read_bronze(spark):

    # Read bronze layer

    df = spark.read.parquet(BRONZE_DIR.as_posix())
    print("Bronze schema:")
    df.printSchema()

    print(f"Bronze rows: {df.count()}")
    return df


def apply_schema(df):

    # Force consistent schema across all datasets (resolve INT/BIGINT issues)


    if "VendorID" in df.columns:
        df = df.withColumn("VendorID", col("VendorID").cast("int"))

    if "passenger_count" in df.columns:
        df = df.withColumn("passenger_count", col("passenger_count").cast("int"))

    if "trip_distance" in df.columns:
        df = df.withColumn("trip_distance", col("trip_distance").cast("double"))

    if "fare_amount" in df.columns:
        df = df.withColumn("fare_amount", col("fare_amount").cast("double"))

    if "total_amount" in df.columns:
        df = df.withColumn("total_amount", col("total_amount").cast("double"))

    return df


def transform_silver(df):
    
    # Clean + standardize + enrich data

    df = apply_schema(df)
    df = df.withColumn("source_file", input_file_name())

    # Stable dataset label (avoid fragile regex)
    df = df.withColumn("dataset", lit("nyc_tlc"))

    # Data quality filters
    df = df.filter(
        (col("trip_distance") > 0) &
        (col("fare_amount") > 0)
    )

    return df


def write_silver(df):
    
    # Write silver layer partitioned for analytics
    output_path = SILVER_DIR.as_posix()

    df.write \
        .mode("overwrite") \
        .partitionBy("dataset") \
        .parquet(output_path)

    print(f"Silver written to: {output_path}")


def run_silver():
    spark = get_spark()
    df = read_bronze(spark)

    if df.rdd.isEmpty():
        print("No bronze data found")
        return

    df_silver = transform_silver(df)
    print(f"Silver rows: {df_silver.count()}")
    
    write_silver(df_silver)
    print("Silver layer completed")


if __name__ == "__main__":
    run_silver()