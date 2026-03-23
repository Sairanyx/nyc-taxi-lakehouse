from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def create_spark():
    return (
        SparkSession.builder
        .appName("Validate Silver")
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
    df = df.cache()
    df.printSchema()

    # BASIC INFO
    total = df.count()
    print(f"Total rows: {total}")

    print("\nColumns:", df.columns)

    if total == 0:
        raise ValueError("Validation failed: Silver dataset is empty")

  
    # GLOBAL SANITY CHECK
    print("\n--- DURATION RANGE ---")
    df.selectExpr(
        "min(duration_sec) as min_duration",
        "max(duration_sec) as max_duration"
    ).show()


    # VALIDATION CHECKS
    print("\n--- VALIDATION CHECKS ---")

    checks = {
        "negative_duration": df.filter(col("duration_sec") < 0).count(),
        "invalid_distance": df.filter(col("trip_distance") <= 0).count(),
        "negative_passengers": df.filter(col("passenger_count") < 0).count(),
        "very_long_duration": df.filter(col("duration_sec") > 7200).count(),
        "very_long_distance": df.filter(col("trip_distance") > 30).count(),
        "null_duration": df.filter(col("duration_sec").isNull()).count(),
        "null_distance": df.filter(col("trip_distance").isNull()).count(),
        "null_passengers": df.filter(col("passenger_count").isNull()).count(),
    }

    for name, value in checks.items():
        print(f"{name}: {value}")

    # Fail pipeline if critical issues exist
    critical_errors = [
        "negative_duration",
        "invalid_distance",
        "null_duration",
        "null_distance",
        "null_passengers"
    ]

    for key in critical_errors:
        if checks[key] > 0:
            raise ValueError(f"Validation failed: {key} detected")


    # SAMPLE DATA
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