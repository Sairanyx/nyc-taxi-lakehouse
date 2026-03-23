from pyspark.sql import SparkSession


def create_spark():
    return (
        SparkSession.builder
        .appName("Bronze Ingestion")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def run_ingestion():
    spark = create_spark()

    print("Reading ALL parquet files from folder...")

    df = spark.read.parquet("data/local/yellow_tripdata/")

    print("Schema:")
    df.printSchema()

    print("Row count:")
    print(df.count())

    print("Sample:")
    df.show(5)

    print("Writing to Bronze layer (MinIO)...")

    df.write.mode("overwrite").parquet(
        "s3a://nyc-taxi/bronze/yellow_tripdata/"
    )

    print("Bronze layer created successfully.")


if __name__ == "__main__":
    run_ingestion()
 