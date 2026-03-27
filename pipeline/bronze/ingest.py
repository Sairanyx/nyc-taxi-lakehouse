import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from config.spark_config import create_spark
from config.settings import RAW_PATH, BRONZE_PATH

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

logger = logging.getLogger(__name__) 

def run_ingestion():
    spark = create_spark("Bronze Ingestion")
    logger.info(f"Reading data from: {RAW_PATH}")

    # One month at a time

    months = [f"2025-{m:02d}" for m in range(1, 13)]
    total_rows = 0

    for i, month in enumerate(months):
        try:
            df = spark.read.parquet(f"{RAW_PATH}yellow_tripdata_{month}.parquet")

            # Print schema only once on first month

            if i == 0:
                logger.info("Schema:")
                df.printSchema()

            # First month overwrites, subsequent months append

            mode = "overwrite" if i == 0 else "append"
            df.write.mode(mode).parquet(BRONZE_PATH)

            total_rows += df.count()
            spark.catalog.clearCache()
            logger.info(f"  {month}: written to bronze")

        except Exception as e:
            logger.warning(f"  Skipping {month}: {e}")

    logger.info(f"Bronze layer complete. Total rows: {total_rows:,}")
    
    spark.stop()

if __name__ == "__main__":
    run_ingestion()