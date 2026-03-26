import os
from dotenv import load_dotenv
 
load_dotenv()

BUCKET = os.getenv("S3_BUCKET", "taxi")

RAW_PATH    = f"s3a://{BUCKET}/raw/"
BRONZE_PATH = f"s3a://{BUCKET}/bronze/yellow_tripdata/"
SILVER_PATH = f"s3a://{BUCKET}/silver/clean_trips/"
GOLD_PATH   = f"s3a://{BUCKET}/gold"

REFERENCE_PATH = f"s3a://{BUCKET}/raw/taxi_zone_lookup.csv"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET   = os.getenv("MINIO_ROOT_PASSWORD")