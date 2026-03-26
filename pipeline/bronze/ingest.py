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

    df = spark.read.parquet(f"{RAW_PATH}yellow_tripdata_*.parquet")

    logger.info("Schema:")
    df.printSchema()

    logger.info("Writing Bronze layer...")

    df.write.mode("overwrite").parquet(BRONZE_PATH)
    bronze_count = df.count()
    logger.info(f"Bronze layer created successfully. Rows: {bronze_count:,}")


if __name__ == "__main__":
    run_ingestion()