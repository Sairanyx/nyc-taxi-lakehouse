from dotenv import load_dotenv
import os

load_dotenv(os.path.join(os.path.dirname(__file__), "../environment/.env"))

BUCKET = os.getenv("S3_BUCKET", "taxi")

RAW_PATH    = f"s3a://{BUCKET}/raw/"
BRONZE_PATH = f"s3a://{BUCKET}/bronze/yellow_tripdata/"
SILVER_PATH = f"s3a://{BUCKET}/silver/clean_trips/"
GOLD_PATH   = f"s3a://{BUCKET}/gold"

GOLD_PATH_DUCKDB = f"s3://{BUCKET}/gold"

REFERENCE_PATH = f"s3a://{BUCKET}/raw/taxi_zone_lookup.csv"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ROOT_USER") or os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET   = os.getenv("MINIO_ROOT_PASSWORD") or os.getenv("MINIO_SECRET_KEY")

MINIO_ENDPOINT_LOCAL = os.getenv("MINIO_ENDPOINT_LOCAL", "http://localhost:9000")