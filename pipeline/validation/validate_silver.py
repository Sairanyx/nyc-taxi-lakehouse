from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, min, max


def create_spark():
    return (
        SparkSession.builder
        .appName("Validate Silver")

        # MinIO connection (S3-compatible storage)
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        .getOrCreate()
    )


def run_validation():
    spark = create_spark()

    print("Loading Silver data...")
    df = spark.read.parquet("s3a://nyc-taxi/silver/clean_trips/")

    # Print schema to verify structure
    df.printSchema()

    # --- SINGLE PASS AGGREGATION (efficient) ---
    # We compute ALL checks in one scan of the data
    agg = df.agg(
        min("duration_sec").alias("min_duration"),
        max("duration_sec").alias("max_duration"),

        # Data quality checks
        sum(when(col("duration_sec") < 0, 1).otherwise(0)).alias("negative_duration"),
        sum(when(col("trip_distance") <= 0, 1).otherwise(0)).alias("invalid_distance"),
        sum(when(col("passenger_count") < 0, 1).otherwise(0)).alias("negative_passengers"),
        sum(when(col("duration_sec") > 7200, 1).otherwise(0)).alias("very_long_duration"),
        sum(when(col("trip_distance") > 30, 1).otherwise(0)).alias("very_long_distance"),
        sum(when(col("duration_sec").isNull(), 1).otherwise(0)).alias("null_duration"),
        sum(when(col("trip_distance").isNull(), 1).otherwise(0)).alias("null_distance"),
        sum(when(col("passenger_count").isNull(), 1).otherwise(0)).alias("null_passengers"),
    )

    # Convert Spark Row → Python dict
    result = agg.collect()[0].asDict()

    # --- RANGE CHECK ---
    print("\n--- DURATION RANGE ---")
    print("min_duration:", result["min_duration"])
    print("max_duration:", result["max_duration"])

    # --- VALIDATION OUTPUT ---
    print("\n--- VALIDATION CHECKS ---")
    for key, value in result.items():
        if key not in ["min_duration", "max_duration"]:
            print(f"{key}: {value}")

    # --- FAIL CONDITIONS (pipeline safety) ---
    # These errors should NEVER exist in clean Silver data
    critical_errors = [
        "negative_duration",
        "invalid_distance",
        "null_duration",
        "null_distance",
        "null_passengers"
    ]

    for key in critical_errors:
        if result[key] > 0:
            raise ValueError(f"Validation failed: {key} detected")

    # --- SAMPLE OUTPUT ---
    # Quick sanity check of real rows
    print("\n--- SAMPLE DATA ---")
    df.select(
        "pickup_datetime",
        "dropoff_datetime",
        "duration_sec",
        "trip_distance",
        "passenger_count"
    ).show(5)

    print("\nValidation complete.")


if __name__ == "__main__":
    run_validation()