"""Validates the bronze layer by printing row count, schema and a sample of the data."""

import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from config.spark_config import create_spark
from config.settings import BRONZE_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def run_validation():
    """Reads the bronze layer and logs some simple counts."""
    spark = create_spark("Validating Bronze")

    logger.info(f"Reading Bronze layer from: {BRONZE_PATH}")
    df = spark.read.parquet(BRONZE_PATH)

    logger.info(f"Rows: {df.count():,}")
    logger.info(f"Columns: {df.columns}")
    df.printSchema()
    df.show(5)

    spark.stop()


if __name__ == "__main__":
    run_validation()