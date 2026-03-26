"""

download.py — Data ingestion: TLC website → MinIO raw/

Downloads all 12 months of NYC Yellow Taxi 2025 trip data plus the
Taxi Zone Lookup CSV from the NYC TLC website and uploads them to
MinIO at taxi/raw/. Safe to rerun — already downloaded files are
skipped automatically.

Run this once before starting the Spark pipeline:
    python pipeline/download.py

"""

import os
import logging
import boto3
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS   = os.getenv("MINIO_ROOT_USER")
MINIO_SECRET   = os.getenv("MINIO_ROOT_PASSWORD")
BUCKET         = "taxi"
LOCAL_DATA_DIR = Path("data/raw")

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
YEAR     = "2025"
MONTHS   = [f"{m:02d}" for m in range(1, 13)]


# ── Helpers ────────────────────────────────────────────────────────────────────

def download_file(url: str, dest: Path) -> bool:
    """Download a file from url to dest. Skips if already exists."""
    if dest.exists():
        logger.info(f"[SKIP] {dest.name} already exists")
        return True

    logger.info(f"[DOWNLOAD] {url}")
    response = requests.get(url, stream=True)

    if response.status_code == 404:
        logger.warning(f"[NOT FOUND] {dest.name} — month not yet published")
        return False

    response.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)

    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
            f.write(chunk)

    logger.info(f"[SAVED] {dest.name}")
    return True


def upload_to_minio(s3, local_path: Path, s3_key: str) -> None:
    """Upload a local file to MinIO under taxi/raw/."""
    logger.info(f"[UPLOAD] {local_path.name} → s3://{BUCKET}/raw/{s3_key}")
    s3.upload_file(
        Filename=str(local_path),
        Bucket=BUCKET,
        Key=f"raw/{s3_key}",
    )
    logger.info(f"[DONE] {s3_key}")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    LOCAL_DATA_DIR.mkdir(parents=True, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS,
        aws_secret_access_key=MINIO_SECRET,
    )
    logger.info(f"Connected to MinIO at {MINIO_ENDPOINT}")

    # ── Trip data: all 12 months ───────────────────────────────────────────────
    logger.info("=== Downloading Yellow Taxi trip data ===")
    for month in MONTHS:
        filename = f"yellow_tripdata_{YEAR}-{month}.parquet"
        dest     = LOCAL_DATA_DIR / filename
        if download_file(f"{BASE_URL}/{filename}", dest):
            upload_to_minio(s3, dest, filename)

    # ── Zone Lookup CSV ────────────────────────────────────────────────────────
    logger.info("=== Downloading Taxi Zone Lookup CSV ===")
    zone_dest = LOCAL_DATA_DIR / "taxi_zone_lookup.csv"
    if download_file(ZONE_URL, zone_dest):
        upload_to_minio(s3, zone_dest, "taxi_zone_lookup.csv")

    logger.info("✔ All files uploaded to MinIO at taxi/raw/")


if __name__ == "__main__":
    main()