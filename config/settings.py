import os
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.getenv("S3_BUCKET", "nyc-taxi")

BRONZE_PATH = f"s3a://{BUCKET}/bronze/yellow_tripdata/"
SILVER_PATH = f"s3a://{BUCKET}/silver/clean_trips/"
GOLD_PATH = f"s3a://{BUCKET}/gold"

REFERENCE_PATH = f"s3a://{BUCKET}/reference/taxi_zone.csv"

LOCAL_DATA_PATH = os.getenv("LOCAL_DATA_PATH")