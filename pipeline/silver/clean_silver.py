"""Cleans and transforms bronze layer data into the silver layer."""

import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from config.spark_config import create_spark
from config.settings import BRONZE_PATH, SILVER_PATH
from pyspark.sql.functions import col, unix_timestamp, when

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def run_cleaning():
    """Reads bronze data month by month, applies cleaning steps and writes to silver."""
    spark = create_spark("Silver Cleaning")
    logger.info("Loading Bronze data...")

    # Processes one month at a time so there are no memory issues

    months = [f"2025-{m:02d}" for m in range(1, 13)]
    total_rows = 0

    for i, month in enumerate(months):
        try:
            df = spark.read.parquet(BRONZE_PATH)

            # Filters to current month using date boundaries

            year, mon = month.split("-")
            next_month = f"{year}-{int(mon)+1:02d}-01" if int(mon) < 12 else f"{int(year)+1}-01-01"
            df = df.filter(
                (col("tpep_pickup_datetime") >= f"{month}-01") &
                (col("tpep_pickup_datetime") < next_month)
            )

            # 1. Renames columns to consistent snake_case names

            df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
                   .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
                   .withColumnRenamed("PULocationID", "pickup_location_id") \
                   .withColumnRenamed("DOLocationID", "dropoff_location_id")

            # 2. Creates unix timestamps for duration calculation

            df = df.withColumn("pickup_ts", unix_timestamp(col("pickup_datetime")))
            df = df.withColumn("dropoff_ts", unix_timestamp(col("dropoff_datetime")))

            # 3. Fixs trips where dropoff is slightly before pickup (data entry error)

            df = df.withColumn(
                "dropoff_datetime",
                when(
                    (col("dropoff_ts") < col("pickup_ts")) &
                    (col("pickup_ts") - col("dropoff_ts") < 60),
                    col("pickup_datetime")
                ).otherwise(col("dropoff_datetime"))
            )

            # Recalculates the dropoff_ts after the fix

            df = df.withColumn("dropoff_ts", unix_timestamp(col("dropoff_datetime")))

            # 4. Calculates the trip duration in seconds

            df = df.withColumn(
                "duration_sec",
                col("dropoff_ts") - col("pickup_ts")
            )

            # 5. Fills the missing passenger counts with 0 instead of dropping rows

            df = df.fillna({"passenger_count": 0})

            # 6. Removes invalid rows

            df = df.filter(col("duration_sec") >= 0)
            df = df.filter(col("trip_distance") >= 0)

            # 7. Removes outliers (trips under 1 min, over 2 hours or unrealistic distances)

            df = df.filter(
                (col("duration_sec") >= 60) & (col("duration_sec") < 7200)
            )
            df = df.filter(
                (col("trip_distance") > 0) & (col("trip_distance") < 30)
            )

            # 8. Removes duplicate trips

            df = df.dropDuplicates([
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_location_id",
                "dropoff_location_id"
            ])

            # 9. Drops that helper timestamp columns thats's no longer needed

            df = df.drop("pickup_ts", "dropoff_ts")

            # 10. Repartitions and caches before counting and writing

            df = df.repartition(4).cache()

            mode = "overwrite" if i == 0 else "append"
            count = df.count()
            df.write.mode(mode).parquet(SILVER_PATH)
            total_rows += count

            df.unpersist()
            spark.catalog.clearCache()
            logger.info(f"  {month}: written to silver")

        except Exception as e:
            logger.warning(f"  Skipping {month}: {e}")

    logger.info(f"Silver layer complete. Rows: {total_rows:,}")
    spark.stop()


if __name__ == "__main__":
    run_cleaning()