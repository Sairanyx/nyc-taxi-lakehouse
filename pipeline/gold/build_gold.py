"""Builds the gold layer by aggregating silver data and joining with taxi zone names."""

import sys
import os
import logging

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from config.spark_config import create_spark
from config.settings import SILVER_PATH, GOLD_PATH, REFERENCE_PATH
from pyspark.sql.functions import col, hour, count, avg, month

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    """Reads silver data, joins with taxi zones and writes 6 analytics aggregations to gold."""
    spark = create_spark("Gold Build")

    # Loads cleaned silver data

    logger.info("Loading Silver data...")
    df = spark.read.parquet(SILVER_PATH)
    df.show(5)

    # Loads the taxi zone lookup CSV for borough/zone name

    logger.info("Loading taxi zones...")
    zones = spark.read.csv(REFERENCE_PATH, header=True, inferSchema=True)
    zones = zones.withColumnRenamed("LocationID", "zone_LocationID")

    # Joins pickup location so it gets pickup borough name

    df = df.join(
        zones,
        df.pickup_location_id == zones.zone_LocationID,
        "left"
    )

    # Joins dropoff location to get dropoff borough name

    zones_drop = zones.select(
        col("zone_LocationID").alias("DO_zone_id"),
        col("Borough").alias("DO_Borough")
    )
    df = df.join(
        zones_drop,
        df.dropoff_location_id == zones_drop.DO_zone_id,
        "left"
    )

    # Q1: Total rides per hour of the day

    hourly = (
        df.filter(col("pickup_datetime").isNotNull())
        .withColumn("hour", hour(col("pickup_datetime")))
        .groupBy("hour")
        .agg(count("*").alias("ride_count"))
        .orderBy("hour")
    )
    hourly.write.mode("overwrite").parquet(f"{GOLD_PATH}/hourly_demand/")

    # Q2: Total rides per pickup borough (taking out the unknown/invalid zones, there were plenty of them)

    borough = (
        df.filter(col("Borough").isNotNull() & ~col("Borough").isin("Unknown", "N/A", "EWR"))
        .groupBy("Borough")
        .agg(count("*").alias("ride_count"))
        .orderBy(col("ride_count").desc())
    )
    borough.write.mode("overwrite").parquet(f"{GOLD_PATH}/demand_by_borough/")

    # Q3: Most popular borough-to-borough routes (taking out the unknown/invalid zones, there were plenty of them)

    routes = (
        df.filter(
            col("Borough").isNotNull() & col("DO_Borough").isNotNull() &
            ~col("Borough").isin("Unknown", "N/A", "EWR") &
            ~col("DO_Borough").isin("Unknown", "N/A", "EWR")
        )
        .groupBy("Borough", "DO_Borough")
        .agg(count("*").alias("ride_count"))
        .orderBy(col("ride_count").desc())
    )
    routes.write.mode("overwrite").parquet(f"{GOLD_PATH}/popular_routes/")

    # Q4: Average trip duration in minutes per month

    avg_duration = (
        df.groupBy(month("pickup_datetime").alias("month"))
        .agg((avg("duration_sec") / 60).alias("avg_duration_min"))
        .orderBy("month")
    )
    avg_duration.write.mode("overwrite").parquet(f"{GOLD_PATH}/avg_duration/")

    # Q5: Average trip distance in miles per month

    avg_distance = (
        df.groupBy(month("pickup_datetime").alias("month"))
        .agg(avg("trip_distance").alias("avg_distance"))
        .orderBy("month")
    )
    avg_distance.write.mode("overwrite").parquet(f"{GOLD_PATH}/avg_distance/")

    # Q6: Average passenger count per trip per month (taking out the trips with 0 passengers)

    avg_passengers = (
        df.filter(col("passenger_count") > 0)
        .groupBy(month("pickup_datetime").alias("month"))
        .agg(avg("passenger_count").alias("avg_passenger_count"))
        .orderBy("month")
    )
    avg_passengers.write.mode("overwrite").parquet(f"{GOLD_PATH}/avg_passengers/")

    logger.info("Gold layer completed.")
    spark.stop()


if __name__ == "__main__":
    main()