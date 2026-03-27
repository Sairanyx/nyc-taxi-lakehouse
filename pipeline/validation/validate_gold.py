"""Validates the gold layer by checking row counts, nulls, and consistency across aggregations."""

import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from config.spark_config import create_spark
from config.settings import GOLD_PATH
from pyspark.sql.functions import col, sum

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def validate_df(df, name):
    """Prints schema, row count, null counts and a sample for a given DataFrame."""
    logger.info(f"{name}")
    df.printSchema()
    logger.info(f"Row count: {df.count():,}")

    # Counts nulls per column

    df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).show()

    df.show(5)


def run_validation():
    """Reads all gold layer tables and runs basic quality and consistency checks."""
    spark = create_spark("Validating Gold")

    # Hourly demand

    df_hour = spark.read.parquet(f"{GOLD_PATH}/hourly_demand/")
    validate_df(df_hour, "Hourly demand")
    df_hour.select("hour").distinct().orderBy("hour").show(24)

    # Borough demand (excluding unknown zones)

    df_borough = spark.read.parquet(f"{GOLD_PATH}/demand_by_borough/")
    df_borough = df_borough.filter(~col("Borough").isin("Unknown", "N/A"))
    validate_df(df_borough, "Demand by borough")

    # Popular routes

    df_routes = spark.read.parquet(f"{GOLD_PATH}/popular_routes/")
    validate_df(df_routes, "Popular routes")
    df_routes.filter(col("ride_count") <= 0).show()

    # Average metrics per month

    df_duration = spark.read.parquet(f"{GOLD_PATH}/avg_duration/")
    validate_df(df_duration, "Average duration")
    logger.info(f"Months in avg_duration: {df_duration.count()}")

    df_distance = spark.read.parquet(f"{GOLD_PATH}/avg_distance/")
    validate_df(df_distance, "Average distance")
    logger.info(f"Months in avg_distance: {df_distance.count()}")

    df_passengers = spark.read.parquet(f"{GOLD_PATH}/avg_passengers/")
    validate_df(df_passengers, "Average passengers")
    logger.info(f"Months in avg_passengers: {df_passengers.count()}")

    # Final consistency check so totals should be similar across hourly, borough and routes

    logger.info("FINAL CONSISTENCY CHECK")
    total_hour    = df_hour.agg(sum("ride_count")).first()[0]
    total_borough = df_borough.agg(sum("ride_count")).first()[0]
    total_routes  = df_routes.agg(sum("ride_count")).first()[0]

    logger.info(f"Hourly total:  {total_hour:,}")
    logger.info(f"Borough total: {total_borough:,}")
    logger.info(f"Routes total:  {total_routes:,}")

    spark.stop()


if __name__ == "__main__":
    run_validation()