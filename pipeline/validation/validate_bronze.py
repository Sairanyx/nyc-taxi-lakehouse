from pyspark.sql import SparkSession


def create_spark():
    return (
        SparkSession.builder
        .appName("Validate Bronze")
        .config("spark.hadoop.fs.s3a.endpoint", "http://host.docker.internal:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def run_validation():
    spark = create_spark()

    df = spark.read.parquet("s3a://nyc-taxi/bronze/yellow_tripdata/")

    print("Rows:", df.count())
    print("Columns:", df.columns)

    df.printSchema()
    df.show(5)


if __name__ == "__main__":
    run_validation()