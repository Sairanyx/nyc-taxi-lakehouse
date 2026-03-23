from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, when


def create_spark():
    return (
        SparkSession.builder
        .appName("Silver Cleaning")

        # MinIO connection
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .getOrCreate()
    )


def run_cleaning():
    spark = create_spark()

    print("Loading Bronze data...")
    df = spark.read.parquet("s3a://nyc-taxi/bronze/yellow_tripdata/")

    print("Sample BEFORE cleaning:")
    df.show(5)

    # -----------------------------
    # 1. Rename columns
    # -----------------------------
    df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
           .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")

    # -----------------------------
    # 2. Create timestamps
    # -----------------------------
    df = df.withColumn("pickup_ts", unix_timestamp(col("pickup_datetime")))
    df = df.withColumn("dropoff_ts", unix_timestamp(col("dropoff_datetime")))

    # -----------------------------
    # 3. Fix SMALL negative durations (-60 < x < 0)
    # -----------------------------
    df = df.withColumn(
        "dropoff_datetime",
        when(
            (col("dropoff_ts") - col("pickup_ts") < 0) &
            (col("dropoff_ts") - col("pickup_ts") > -60),
            col("pickup_datetime")
        ).otherwise(col("dropoff_datetime"))
    )

    # recompute dropoff_ts after fix
    df = df.withColumn("dropoff_ts", unix_timestamp(col("dropoff_datetime")))

    # -----------------------------
    # 4. Compute duration
    # -----------------------------
    df = df.withColumn(
        "duration_sec",
        col("dropoff_ts") - col("pickup_ts")
    )

    # -----------------------------
    # 5. Handle missing passengers (DO NOT drop)
    # -----------------------------
    df = df.fillna({"passenger_count": 0})

    # -----------------------------
    # 6. Remove INVALID data
    # -----------------------------
    # severe + medium negative durations
    df = df.filter(col("duration_sec") >= 0)

    # impossible values
    df = df.filter(col("trip_distance") >= 0)
    df = df.filter(col("passenger_count") >= 0)

    # -----------------------------
    # 7. Remove OUTLIERS (analysis-ready dataset)
    # -----------------------------
    df = df.filter(
        (col("duration_sec") >= 60) & (col("duration_sec") < 7200)
    )

    df = df.filter(
        (col("trip_distance") > 0) & (col("trip_distance") < 30)
    )

    # -----------------------------
    # 8. Remove duplicates
    # -----------------------------
    df = df.dropDuplicates([
        "pickup_datetime",
        "dropoff_datetime",
        "PULocationID",
        "DOLocationID"
    ])

    # -----------------------------
    # 9. Drop helper columns
    # -----------------------------
    df = df.drop("pickup_ts", "dropoff_ts")

    print("Sample AFTER cleaning:")
    df.show(5)

    # -----------------------------
    # 10. Repartition (small cluster)
    # -----------------------------
    df = df.repartition(4)

    print("Writing Silver layer...")

    df.write \
        .mode("overwrite") \
        .parquet("s3a://nyc-taxi/silver/clean_trips/")

    print("Silver layer completed.")


if __name__ == "__main__":
    run_cleaning()