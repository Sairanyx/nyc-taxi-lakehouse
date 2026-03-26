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
    df = spark.read.parquet(BRONZE_PATH)

    logger.info("Sample BEFORE cleaning:")
    df.show(5)


    # 1. Rename columns

    df = df.withColumnRenamed("tpep_pickup_datetime", "pickup_datetime") \
           .withColumnRenamed("tpep_dropoff_datetime", "dropoff_datetime") \
           .withColumnRenamed("PULocationID", "pickup_location_id") \
           .withColumnRenamed("DOLocationID", "dropoff_location_id")


    # 2. Create timestamps

    df = df.withColumn("pickup_ts", unix_timestamp(col("pickup_datetime")))
    df = df.withColumn("dropoff_ts", unix_timestamp(col("dropoff_datetime")))

    # 3. Fix durations 

    df = df.withColumn(
        "dropoff_datetime",
        when(
            (col("dropoff_ts") < col("pickup_ts")) &
            (col("pickup_ts") - col("dropoff_ts") < 60),
            col("pickup_datetime")
        ).otherwise(col("dropoff_datetime"))
    )

    # recompute dropoff_ts after fix

    df = df.withColumn("dropoff_ts", unix_timestamp(col("dropoff_datetime")))


    # 4. Compute duration

    df = df.withColumn(
        "duration_sec",
        col("dropoff_ts") - col("pickup_ts")
    )


    # 5. Handle missing passengers (DO NOT drop)

    df = df.fillna({"passenger_count": 0})


    # 6. Remove INVALID data

    # severe + medium negative durations

    df = df.filter(col("duration_sec") >= 0)

    # impossible values

    df = df.filter(col("trip_distance") >= 0)
    

    # 7. Remove OUTLIERS 

    df = df.filter(
        (col("duration_sec") >= 60) & (col("duration_sec") < 7200)
    )
    df = df.filter(
        (col("trip_distance") > 0) & (col("trip_distance") < 30)
    )

    # 8. Remove duplicates

    df = df.dropDuplicates([
        "pickup_datetime",
        "dropoff_datetime",
        "pickup_location_id",
        "dropoff_location_id"
    ])

    # 9. Drop columns

    df = df.drop("pickup_ts", "dropoff_ts")

    # 10. Repartition 

    df = df.repartition(4)
    logger.info("Writing Silver layer...")
    df.write.mode("overwrite").parquet(SILVER_PATH)
    silver_count = df.count()
    logger.info(f"Silver layer completed. Rows: {silver_count:,}")

    spark.stop()

if __name__ == "__main__":
    run_cleaning()