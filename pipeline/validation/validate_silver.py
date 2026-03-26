import sys
import os
import logging
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from config.spark_config import create_spark
from config.settings import SILVER_PATH
from pyspark.sql.functions import col, sum, when, min, max

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",)

logger = logging.getLogger(__name__)

def run_validation():
    spark = create_spark("Validating Silver")

    logger.info("Loading Silver data...")
    df = spark.read.parquet(SILVER_PATH)

    # Printing schema to verify the structure

    df.printSchema()

    # Single Pass aggregation

    agg = df.agg(
        min("duration_sec").alias("min_duration"),
        max("duration_sec").alias("max_duration"),

        # Checking Data Quality

        sum(when(col("duration_sec") < 0, 1).otherwise(0)).alias("negative_duration"),
        sum(when(col("trip_distance") <= 0, 1).otherwise(0)).alias("invalid_distance"),
        sum(when(col("passenger_count") < 0, 1).otherwise(0)).alias("negative_passengers"),
        sum(when(col("duration_sec") > 7200, 1).otherwise(0)).alias("very_long_duration"),
        sum(when(col("trip_distance") > 30, 1).otherwise(0)).alias("very_long_distance"),
        sum(when(col("duration_sec").isNull(), 1).otherwise(0)).alias("null_duration"),
        sum(when(col("trip_distance").isNull(), 1).otherwise(0)).alias("null_distance"),
        sum(when(col("passenger_count").isNull(), 1).otherwise(0)).alias("null_passengers"),
        sum(when(col("passenger_count") == 0, 1).otherwise(0)).alias("zero_passengers"),
    )

    # Converting Spark Rows to Python dictionary

    result = agg.collect()[0].asDict()

    # Checking range check

    logger.info("DURATION RANGE")
    logger.info(f"min_duration: {result['min_duration']}")
    logger.info(f"max_duration: {result['max_duration']}")

    # Checking validation output

    logger.info("VALIDATION CHECKS")
    for key, value in result.items():
        if key not in ["min_duration", "max_duration"]:
            logger.info(f"{key}: {value}")

    # Checking fail conditions

    critical_errors = [
        "negative_duration",
        "invalid_distance",
        "null_duration",
        "null_distance"
    ]

    for key in critical_errors:
        if result[key] > 0:
            raise ValueError(f"Validation failed: {key} detected")

    # Checking for real rows

    logger.info("SAMPLE DATA")
    df.select(
        "pickup_datetime",
        "dropoff_datetime",
        "duration_sec",
        "trip_distance",
        "passenger_count"
    ).show(5)

    logger.info("Validation complete.")
    spark.stop()


if __name__ == "__main__":
    run_validation()