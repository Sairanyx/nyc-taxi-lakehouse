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
    spark = create_spark("Silver Cleaning")
    logger.info("Loading Bronze data...")

    # Processing one month at a time to avoid running out of memory
    
    months = [f"2025-{m:02d}" for m in range(1, 13)]
    total_rows = 0

    for i, month in enumerate(months):
        try:
            df = spark.read.parquet(BRONZE_PATH)
            year, mon = month.split("-")
            next_month = f"{year}-{int(mon)+1:02d}-01" if int(mon) < 12 else f"{int(year)+1}-01-01"
            df = df.filter(
                (col("tpep_pickup_datetime") >= f"{month}-01") &
                (col("tpep_pickup_datetime") < next_month)
            )

            # 1. Renaming columns

            df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
                   .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
                   .withColumnRenamed("PULocationID", "pickup_location_id") \
                   .withColumnRenamed("DOLocationID", "dropoff_location_id")

            # 2. Creating timestamps

            df = df.withColumn("pickup_ts", unix_timestamp(col("pickup_datetime")))
            df = df.withColumn("dropoff_ts", unix_timestamp(col("dropoff_datetime")))

            # 3. Fixing durations

            df = df.withColumn(
                "dropoff_datetime",
                when(
                    (col("dropoff_ts") < col("pickup_ts")) &
                    (col("pickup_ts") - col("dropoff_ts") < 60),
                    col("pickup_datetime")
                ).otherwise(col("dropoff_datetime"))
            )
            # Recomputing dropoff_ts after fix

            df = df.withColumn("dropoff_ts", unix_timestamp(col("dropoff_datetime")))

            # 4. Computing duration

            df = df.withColumn(
                "duration_sec",
                col("dropoff_ts") - col("pickup_ts")
            )

            # 5. Handling missing passengers

            df = df.fillna({"passenger_count": 0})

            # 6. Removing the invalid data
            # severe + medium negative durations

            df = df.filter(col("duration_sec") >= 0)

            # impossible values

            df = df.filter(col("trip_distance") >= 0)

            # 7. Removing the outliers

            df = df.filter(
                (col("duration_sec") >= 60) & (col("duration_sec") < 7200)
            )
            df = df.filter(
                (col("trip_distance") > 0) & (col("trip_distance") < 30)
            )

            # 8. Removing the duplicates

            df = df.dropDuplicates([
                "pickup_datetime",
                "dropoff_datetime",
                "pickup_location_id",
                "dropoff_location_id"
            ])

            # 9. Droping columns

            df = df.drop("pickup_ts", "dropoff_ts")

            # 10. Repartition

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