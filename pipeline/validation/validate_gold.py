from pyspark.sql import SparkSession


def create_spark():
    return (
        SparkSession.builder
        .appName("Validate Gold")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def run_validation():
    spark = create_spark()

    print("\n--- Hourly Demand ---")
    spark.read.parquet("s3a://nyc-taxi/gold/hourly_demand/").show()

    print("\n--- Popular Routes ---")
    spark.read.parquet("s3a://nyc-taxi/gold/popular_routes/").show(10)

    print("\n--- Avg Duration ---")
    spark.read.parquet("s3a://nyc-taxi/gold/avg_duration/").show()

    print("\n--- Avg Distance ---")
    spark.read.parquet("s3a://nyc-taxi/gold/avg_distance/").show()

    print("\n--- Avg Passengers ---")
    spark.read.parquet("s3a://nyc-taxi/gold/avg_passengers/").show()


if __name__ == "__main__":
    run_validation()