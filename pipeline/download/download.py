import sys
import os
import logging
import boto3
import requests
from pathlib import Path

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.settings import MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET, BUCKET

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# Local folder where files are saved before uploading

LOCAL_DATA_DIR = Path("data/raw")

# TLC download URLs

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
YEAR     = "2025"
MONTHS   = [f"{m:02d}" for m in range(1, 13)]


def download_file(url, save_path):

    # Skip if already downloaded

    if save_path.exists():
        logger.info(f"[SKIP] {save_path.name} already downloaded")
        return True

    logger.info(f"[DOWNLOAD] {url}")
    response = requests.get(url, stream=True)

    # Save file in chunks to avoid loading it all into memory

    save_path.parent.mkdir(parents=True, exist_ok=True)
    with open(save_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            f.write(chunk)

    logger.info(f"[SAVED] {save_path.name}")
    return True


def upload_to_minio(s3, file_path, filename):

    # Upload file to MinIO under taxi/raw/

    s3.upload_file(
        Filename=str(file_path),
        Bucket=BUCKET,
        Key=f"raw/{filename}",
    )
    logger.info(f"[UPLOADED] {filename} to taxi/raw/")


def main():

    # Create local staging folder if it doesn't exist

    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Connect to MinIO

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )
    logger.info(f"Connected to MinIO at {MINIO_ENDPOINT}")

    # Download and upload all 12 months of Yellow Taxi data

    logger.info("Downloading Yellow Taxi trip data ...")
    for month in MONTHS:
        filename  = f"yellow_tripdata_{YEAR}-{month}.parquet"
        save_path = LOCAL_DATA_DIR / filename
        if download_file(f"{BASE_URL}/{filename}", save_path):
            upload_to_minio(s3, save_path, filename)

    # Download and upload the Zone Lookup CSV

    logger.info("Downloading Taxi Zone Lookup CSV ...")
    zone_path = LOCAL_DATA_DIR / "taxi_zone_lookup.csv"
    if download_file(ZONE_URL, zone_path):
        upload_to_minio(s3, zone_path, "taxi_zone_lookup.csv")

    logger.info("All files uploaded to MinIO at taxi/raw/")


if __name__ == "__main__":
    main()