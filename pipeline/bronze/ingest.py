from config.spark_config import create_spark
from config.settings import LOCAL_DATA_PATH, BRONZE_PATH
import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))


logger = logging.getLogger(__name__)

def run_ingestion():
    spark = create_spark("Bronze Ingestion")

    logger.info(f"Reading data from: {LOCAL_DATA_PATH}")

    df = spark.read.parquet(LOCAL_DATA_PATH)

    logger.info("Schema:")
    df.printSchema()

    logger.info(f"Row count: {df.count()}")

    logger.info("Writing Bronze layer...")
    df.write.mode("overwrite").parquet(BRONZE_PATH)

    logger.info("Bronze layer created successfully.")


if __name__ == "__main__":
    run_ingestion()