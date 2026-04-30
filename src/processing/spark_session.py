from pyspark.sql import SparkSession


def get_spark(app_name="NYC_TLC_PIPELINE"):
    
    # Create or return Spark session with safe configs for heterogeneous data.
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.files.ignoreCorruptFiles", "true") \
        .config("spark.sql.files.ignoreMissingFiles", "true") \
        .config("spark.sql.parquet.mergeSchema", "false") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    return spark