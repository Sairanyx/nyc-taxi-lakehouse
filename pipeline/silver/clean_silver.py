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

        # FIX timeouts 
        .config("spark.hadoop.fs.s3a.connection.timeout", "60000")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "60000")
        .config("spark.hadoop.fs.s3a.socket.timeout", "60000")

        # stability
        .config("spark.hadoop.fs.s3a.attempts.maximum", "3")

        .getOrCreate()
    )


def run_cleaning():
    spark = create_spark()

    print("Loading Bronze data...")
    df = spark.read.parquet("s3a://nyc-taxi/bronze/yellow_tripdata/")


    # Rename columns (clean schema)
    df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
           .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime")


    # Create timestamp columns
    df = df.withColumn("pickup_ts", unix_timestamp(col("pickup_datetime")))
    df = df.withColumn("dropoff_ts", unix_timestamp(col("dropoff_datetime")))
    # Fix small timestamp errors
    df = df.withColumn(
        "dropoff_datetime",
        when(
            (col("dropoff_ts") - col("pickup_ts") < 0) &
            (col("dropoff_ts") - col("pickup_ts") > -60),
            col("pickup_datetime")
        ).otherwise(col("dropoff_datetime"))
    )

    # Recompute dropoff_ts after fixing timestamps
    df = df.withColumn("dropoff_ts", unix_timestamp(col("dropoff_datetime")))

   
    # Feature: duration 
    df = df.withColumn(
        "duration_sec",
        col("dropoff_ts") - col("pickup_ts")
    )

    
    # Fill missing values
    df = df.fillna({"passenger_count": 0})

   
    # Remove invalid durations
    df = df.filter((col("duration_sec") > 0) & (col("duration_sec") < 7200))

    # Remove distance outliers
    df = df.filter((col("trip_distance") > 0) & (col("trip_distance") < 30))

    # Remove invalid passengers
    df = df.filter(col("passenger_count") > 0)

    # Drop duplicates (ONLY ONCE, after filtering)
    df = df.dropDuplicates([
        "pickup_datetime",
        "dropoff_datetime",
        "PULocationID",
        "DOLocationID"
    ])

    # Repartition for writing (AFTER all transformations)
    df = df.repartition(4)
   
    print("Cleaning completed")

    df = df.drop("pickup_ts", "dropoff_ts")


    df.write  \
            .mode("overwrite") \
            .partitionBy("PULocationID") \
            .parquet("s3a://nyc-taxi/silver/clean_trips/")

    print("Silver layer completed.")


if __name__ == "__main__":
    run_cleaning()