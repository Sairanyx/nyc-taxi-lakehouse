# 1. Loads credentials from .env
# 2. Connects to MinIO with boto3
# 3. Loop Jan to Dec 2025 and downloads each yellow taxi parquet to data/
# 4. Uploads each file to taxi/raw/ in MinIO
# 5. Downloads + uploads the Zone Lookup CSV
# 6. Skips files already downloaded so its safe to rerun


import os
import boto3
import requests
from pathlib import Path
from dotenv import load_dotenv

# =========================================
# Load environment variables from .env
# =========================================

load_dotenv()

MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS    = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET    = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET          = "taxi"
LOCAL_DATA_DIR  = Path("data/raw")

# =========================================
# Files to download
# =========================================

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
MONTHS   = [f"{m:02d}" for m in range(1, 13)]
YEAR     = "2025"


def connect_to_minio():

    """Create and return a boto3 S3 client pointed at MinIO."""

    client = boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )
    print(f"Connected to MinIO at {MINIO_ENDPOINT}")
    return client


def download_file(url: str, dest: Path) -> bool:

    """
    Download a file from url to dest.
    Returns True if downloaded, False if skipped (already exists).
    """

    if dest.exists():
        print(f"  [SKIP] {dest.name} already exists locally")
        return False

    print(f"  [DOWNLOAD] {url}")

    response = requests.get(url, stream=True)

    if response.status_code == 404:
        print(f"  [NOT FOUND] {dest.name} — skipping (month not yet published)")
        return False

    response.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)

    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):  # 8MB chunks
            f.write(chunk)

    print(f"  [DONE] Saved to {dest}")
    return True


def upload_to_minio(client, local_path: Path, s3_key: str):

    """Upload a local file to MinIO under taxi/raw/."""

    print(f"  [UPLOAD] {local_path.name} → s3://{BUCKET}/raw/{s3_key}")
    client.upload_file(
        Filename=str(local_path),
        Bucket=BUCKET,
        Key=f"raw/{s3_key}",
    )
    print(f"  [DONE] Uploaded {s3_key}")


def main():

    # Create local staging directory

    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Connect to MinIO

    s3 = connect_to_minio()

    # Trip data: loop all 12 months

    print("\n=== Downloading Yellow Taxi trip data ===")

    for month in MONTHS:
        filename = f"yellow_tripdata_{YEAR}-{month}.parquet"
        url      = f"{BASE_URL}/{filename}"
        dest     = LOCAL_DATA_DIR / filename

        download_file(url, dest)

        if dest.exists():
            upload_to_minio(s3, dest, filename)

    # Zone Lookup CSV

    print("\n=== Downloading Taxi Zone Lookup CSV ===")
    zone_dest = LOCAL_DATA_DIR / "taxi_zone_lookup.csv"
    download_file(ZONE_URL, zone_dest)
    
    if zone_dest.exists():
        upload_to_minio(s3, zone_dest, "taxi_zone_lookup.csv")

    print("\n✓ Ingestion complete. All files are in MinIO at taxi/raw/")


if __name__ == "__main__":
    main()