import sys
import os
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from config.spark_config import create_spark
from config.settings import BRONZE_PATH, SILVER_PATH, GOLD_PATH
from pyspark.sql.functions import sum as spark_sum

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

def run_validation():
    spark = create_spark("Pipeline Consistency Check")

    # Loading data

    logger.info("LOADING DATA")
    bronze = spark.read.parquet(BRONZE_PATH)
    silver = spark.read.parquet(SILVER_PATH)
    gold_hourly = spark.read.parquet(f"{GOLD_PATH}/hourly_demand/")

    # Row counts

    bronze_count = bronze.count()
    silver_count = silver.count()

    logger.info("ROW COUNTS")
    logger.info(f"Bronze rows: {bronze_count:,}")
    logger.info(f"Silver rows: {silver_count:,}")

    # Data loss check

    loss = bronze_count - silver_count
    loss_pct = (loss / bronze_count) * 100

    logger.info("DATA LOSS")
    logger.info(f"Rows removed: {loss:,}")
    logger.info(f"Percentage removed: {loss_pct:.2f}%")

    if loss <= 0:
        logger.warning("No rows were removed so cleaning may not be working")

    # Gold consistency check

    logger.info("GOLD CONSISTENCY")
    gold_total = gold_hourly.select(
        spark_sum("ride_count")
    ).collect()[0][0]

    logger.info(f"Total rides in Silver: {silver_count:,}")
    logger.info(f"Total rides in Gold (hourly sum): {gold_total:,}")

    if gold_total == silver_count:
        logger.info("Gold matches Silver")
    else:
        logger.warning("Mismatch between Gold and Silver")

    logger.info("Validation complete")

    spark.stop()


if __name__ == "__main__":
    run_validation()