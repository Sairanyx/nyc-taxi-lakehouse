"""
Creates a Spark session configured to connect to MinIO.
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

def create_spark(app_name="NYC Taxi Lakehouse"):
    """Builds and returns a SparkSession with MinIO and memory settings."""
    return (
        SparkSession.builder
        .appName(app_name)

        # Connecting Spark to MinIO using the S3A connector
        
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # Memory tuning

        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")

        .getOrCreate()
    )