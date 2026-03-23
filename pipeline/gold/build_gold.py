from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, count, avg


def create_spark():
    return (
        SparkSession.builder
        .appName("Gold Aggregations")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def main():
    spark = create_spark()

    print("Loading Silver data...")
    df = spark.read.parquet("s3a://nyc-taxi/silver/clean_trips/")

    # -------------------------
    # Q1: Ride demand by hour
    # -------------------------
    hourly = (
        df.withColumn("hour", hour(col("tpep_pickup_datetime")))
        .groupBy("hour")
        .agg(count("*").alias("ride_count"))
        .orderBy("hour")
    )

    hourly.write.mode("overwrite").parquet(
        "s3a://nyc-taxi/gold/hourly_demand/"
    )

    # -------------------------
    # Q2: Popular routes
    # -------------------------
    routes = (
        df.groupBy("PULocationID", "DOLocationID")
        .agg(count("*").alias("ride_count"))
        .orderBy(col("ride_count").desc())
    )

    routes.write.mode("overwrite").parquet(
        "s3a://nyc-taxi/gold/popular_routes/"
    )

    # -------------------------
    # Q3–Q5: Summary metrics (better grouped)
    # -------------------------
    summary = df.agg(
        avg("duration_sec").alias("avg_duration_sec"),
        avg("trip_distance").alias("avg_distance"),
        avg("passenger_count").alias("avg_passenger_count")
    )

    summary.write.mode("overwrite").parquet(
        "s3a://nyc-taxi/gold/summary_metrics/"
    )

    print("Gold layer completed.")


if __name__ == "__main__":
    main()