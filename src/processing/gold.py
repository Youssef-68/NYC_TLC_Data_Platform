from pyspark.sql import functions as F
from config.config import SILVER_DIR, GOLD_DIR
from processing.spark_session import get_spark


# Read from SILVER
def read_silver(spark):
    df = spark.read.parquet(SILVER_DIR.as_posix())

    print("Silver schema:")
    df.printSchema()

    print(f"Silver rows: {df.count()}")
    return df


# Time features
def add_time_features(df):

    df = df.withColumn(
        "pickup_ts",
        F.coalesce(
            F.to_timestamp("pickup_datetime"),
            F.to_timestamp("lpep_pickup_datetime"),
            F.to_timestamp("tpep_pickup_datetime")
        )
    ).withColumn(
        "dropoff_ts",
        F.coalesce(
            F.to_timestamp("dropoff_datetime"),
            F.to_timestamp("lpep_dropoff_datetime"),
            F.to_timestamp("tpep_dropoff_datetime")
        )
    )

    df = df.withColumn("year", F.year("pickup_ts")) \
           .withColumn("month", F.month("pickup_ts")) \
           .withColumn("hour", F.hour("pickup_ts")) \
           .withColumn("day_of_week", F.dayofweek("pickup_ts")) \
           .withColumn(
                "is_weekend",
                F.when(F.col("day_of_week").isin([1, 7]), 1).otherwise(0)
            )

    return df


# Quality flags and filters
def add_quality_flags(df):

    return df.withColumn(
        "is_valid_trip",
        (F.col("trip_distance") > 0) & (F.col("total_amount") > 0)
    ).withColumn(
        "is_high_value_trip",
        F.col("total_amount") > 50
    ).withColumn(
        "is_outlier_fare",
        F.col("total_amount") > 200
    )


def filter_valid_data(df):
    return df.filter(F.col("is_valid_trip") == True)


# Fact Tables
def build_fact_trip(df):
    return df.select(
        F.col("VendorID").alias("vendor_id"),
        "pickup_ts",
        "dropoff_ts",
        F.lit(None).cast("int").alias("pickup_location_id"),
        F.lit(None).cast("int").alias("dropoff_location_id"),
        "trip_distance",
        "fare_amount",
        "total_amount",
        "year",
        "month"
    )


# Dimension tables
def build_dim_vendor(df):
    return df.select(
        F.col("VendorID").alias("vendor_id")
    ).dropDuplicates()


def build_dim_date(df):
    return df.select(
        "year",
        "month"
    ).dropDuplicates()


# Aggregations for KPIs and summaries
def build_kpis(df):

    return df.agg(
        F.sum("total_amount").alias("total_revenue"),
        F.count("*").alias("total_trips"),
        F.avg("total_amount").alias("avg_trip_value"),
        F.avg("trip_distance").alias("avg_distance"),
        F.max("total_amount").alias("max_trip_value"),
        F.min("total_amount").alias("min_trip_value")
    )


def build_monthly_summary(df):

    return df.groupBy("year", "month").agg(
        F.sum("total_amount").alias("monthly_revenue"),
        F.count("*").alias("total_trips"),
        F.avg("trip_distance").alias("avg_distance"),
        F.avg("fare_amount").alias("avg_fare")
    )


def build_vendor_summary(df):

    return df.groupBy("VendorID").agg(
        F.sum("total_amount").alias("total_revenue"),
        F.count("*").alias("total_trips"),
        F.avg("total_amount").alias("avg_trip_value")
    )


# Write to GOLD
def write_gold(df, name, partition_cols=None):

    path = f"{GOLD_DIR.as_posix()}/{name}"

    writer = df.write.mode("overwrite")

    if partition_cols:
        writer = writer.partitionBy(*partition_cols)

    writer.parquet(path)


# Pipeline orchestration
def run_gold():

    spark = get_spark()
    df = read_silver(spark)

    if df.limit(1).count() == 0:
        print("No silver data found")
        return

    # transformations
    df = add_time_features(df)
    df = add_quality_flags(df)
    df = filter_valid_data(df)

    # build tables
    fact = build_fact_trip(df)
    dim_vendor = build_dim_vendor(df)
    dim_date = build_dim_date(df)

    kpis = build_kpis(df)
    monthly = build_monthly_summary(df)
    vendor = build_vendor_summary(df)

    # debug
    print(f"Fact rows: {fact.count()}")
    print(f"Monthly rows: {monthly.count()}")

    # writes
    write_gold(fact, "fact_trip", ["year", "month"])
    write_gold(dim_vendor, "dim_vendor")
    write_gold(dim_date, "dim_date")
    write_gold(monthly, "monthly_summary", ["year", "month"])
    write_gold(vendor, "vendor_summary")
    write_gold(kpis, "kpis")

    print("\nGOLD PIPELINE COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    run_gold()