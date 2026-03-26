from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as spark_sum


def create_spark():
    return (
        SparkSession.builder
        .appName("Pipeline Consistency Check")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def run_validation():
    spark = create_spark()

    print("\n--- LOADING DATA ---")

    bronze = spark.read.parquet("s3a://nyc-taxi/bronze/yellow_tripdata/")
    silver = spark.read.parquet("s3a://nyc-taxi/silver/clean_trips/")
    gold_hourly = spark.read.parquet("s3a://nyc-taxi/gold/hourly_demand/")

    # -----------------------------
    # ROW COUNTS
    # -----------------------------
    bronze_count = bronze.count()
    silver_count = silver.count()

    print("\n--- ROW COUNTS ---")
    print(f"Bronze rows: {bronze_count}")
    print(f"Silver rows: {silver_count}")

    # -----------------------------
    # DATA LOSS CHECK
    # -----------------------------
    loss = bronze_count - silver_count
    loss_pct = (loss / bronze_count) * 100

    print("\n--- DATA LOSS ---")
    print(f"Rows removed: {loss}")
    print(f"Percentage removed: {loss_pct:.2f}%")

    # sanity expectation
    if loss <= 0:
        print("⚠️ WARNING: No rows removed → cleaning may not be working")

    # -----------------------------
    # GOLD CONSISTENCY CHECK
    # -----------------------------
    print("\n--- GOLD CONSISTENCY ---")

    gold_total = gold_hourly.select(
        spark_sum("ride_count")
    ).collect()[0][0]

    print(f"Total rides in Silver: {silver_count}")
    print(f"Total rides in Gold (hourly sum): {gold_total}")

    if gold_total == silver_count:
        print("✔ Gold matches Silver (correct aggregation)")
    else:
        print("❌ Mismatch between Gold and Silver!")

    print("\nValidation complete.")


if __name__ == "__main__":
    run_validation()