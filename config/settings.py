import os
from dotenv import load_dotenv
 
load_dotenv()

BUCKET = os.getenv("S3_BUCKET", "taxi")

RAW_PATH    = f"s3a://{BUCKET}/raw/"
BRONZE_PATH = f"s3a://{BUCKET}/bronze/yellow_tripdata/"
SILVER_PATH = f"s3a://{BUCKET}/silver/clean_trips/"
GOLD_PATH   = f"s3a://{BUCKET}/gold"

REFERENCE_PATH = f"s3a://{BUCKET}/raw/taxi_zone_lookup.csv"