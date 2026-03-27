"""
Shared settings and paths used across the whole project.
"""

from dotenv import load_dotenv
import os

# Loads .env file and tries Docker path first, then local dev path

load_dotenv("/app/.env") or load_dotenv(os.path.join(os.path.dirname(__file__), "../environment/.env"))

# MinIO bucket name

BUCKET = os.getenv("S3_BUCKET", "taxi")

# Spark paths (s3a://) for reading/writing data in each layer

RAW_PATH    = f"s3a://{BUCKET}/raw/"
BRONZE_PATH = f"s3a://{BUCKET}/bronze/yellow_tripdata/"
SILVER_PATH = f"s3a://{BUCKET}/silver/clean_trips/"
GOLD_PATH   = f"s3a://{BUCKET}/gold"
REFERENCE_PATH = f"s3a://{BUCKET}/raw/taxi_zone_lookup.csv"

# DuckDB

GOLD_PATH_DUCKDB = f"s3://{BUCKET}/gold"

# MinIO connection details

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ENDPOINT_LOCAL = os.getenv("MINIO_ENDPOINT_LOCAL", "http://localhost:9000")

# Credentials that work in both Docker and local environments

MINIO_ACCESS   = os.getenv("MINIO_ROOT_USER") or os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET   = os.getenv("MINIO_ROOT_PASSWORD") or os.getenv("MINIO_SECRET_KEY")