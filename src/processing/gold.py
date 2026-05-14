from pyspark.sql import functions as F
from config.config import SILVER_DIR, GOLD_DIR
from processing.spark_session import get_spark


def read_silver(spark):
    df = spark.read.parquet(SILVER_DIR.as_posix())
    df.printSchema()
    print(f"Rows: {df.count()}")
    return df


def add_time_features(df):

    df = df.withColumn("pickup_ts", F.to_timestamp("pickup_datetime")) \
           .withColumn("dropoff_ts", F.to_timestamp("dropoff_datetime"))

    df = df.withColumn("year", F.year("pickup_ts")) \
           .withColumn("month", F.month("pickup_ts")) \
           .withColumn("hour", F.hour("pickup_ts")) \
           .withColumn("day_of_week", F.dayofweek("pickup_ts")) \
           .withColumn(
                "is_weekend",
                F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0)
            )

    return df


def add_quality_flags(df):

    return df.withColumn(
        "is_valid_trip",
        (F.col("trip_distance") > 0) & (F.col("total_amount") > 0)
    ).withColumn(
        "is_high_value_trip",
        F.col("total_amount") > 50
    )


def run_gold():

    spark = get_spark()
    df = read_silver(spark)

    if df.limit(1).count() == 0:
        print("No data")
        return

    df = add_time_features(df)
    df = add_quality_flags(df)

    df = df.filter("is_valid_trip = true")

    # FACT
    fact = df.select(
        F.col("VendorID").alias("vendor_id"),
        "pickup_ts",
        "dropoff_ts",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_distance",
        "fare_amount",
        "total_amount",
        "year",
        "month"
    )

    # DIM
    dim_vendor = df.select(
        F.col("VendorID").alias("vendor_id")
    ).dropDuplicates()

    dim_date = df.select("year", "month").dropDuplicates()

    # AGG
    monthly = df.groupBy("year", "month").agg(
        F.sum("total_amount").alias("monthly_revenue"),
        F.count("*").alias("total_trips")
    )

    vendor = df.groupBy("VendorID").agg(
        F.sum("total_amount").alias("total_revenue"),
        F.count("*").alias("total_trips")
    )

    kpis = df.agg(
        F.sum("total_amount").alias("total_revenue"),
        F.count("*").alias("total_trips"),
        F.avg("total_amount").alias("avg_ticket")
    )

    # WRITE
    def write(df, name, part=None):
        w = df.write.mode("overwrite")
        if part:
            w = w.partitionBy(*part)
        w.parquet(f"{GOLD_DIR.as_posix()}/{name}")

    write(fact, "fact_trip", ["year", "month"])
    write(dim_vendor, "dim_vendor")
    write(dim_date, "dim_date")
    write(monthly, "monthly_summary", ["year", "month"])
    write(vendor, "vendor_summary")
    write(kpis, "kpis")

    print("GOLD DONE")