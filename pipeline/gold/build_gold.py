from config.spark_config import create_spark
from config.settings import SILVER_PATH, GOLD_PATH, REFERENCE_PATH
from pyspark.sql.functions import col, hour, count, avg, when
import logging
logger = logging.getLogger(__name__)

def main():
    spark = create_spark()

    logger.info("Loading Silver data...")
    df = spark.read.parquet(SILVER_PATH)
    df.show(5)

    logger.info("Loading taxi zones...")
    zones = spark.read.csv(REFERENCE_PATH, header=True, inferSchema=True)

    #  column name 
    zones = zones.withColumnRenamed("LocationID", "zone_LocationID")

    # ENRICH DATA
    df = df.join(
        zones,
        df.pickup_location_id == zones.zone_LocationID,
        "left"
    )

  
    # ADDITIONAL JOIN FOR DROPOFF (needed for routes to be meaningful)
    zones_drop = zones.select(
        col("zone_LocationID").alias("DO_zone_id"),
        col("Borough").alias("DO_Borough")
    )

    df = df.join(
        zones_drop,
        df.dropoff_location_id == zones_drop.DO_zone_id,
        "left"
    )

    # Q1: Ride demand by hour
    hourly = (
        df.filter(col("pickup_datetime").isNotNull())  # ensure valid timestamps
        .withColumn("hour", hour(col("pickup_datetime")))
        .groupBy("hour")
        .agg(count("*").alias("ride_count"))
        .orderBy("hour")
    )

    hourly.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/hourly_demand/"
    )

    # Q2: Ride demand by borough
    borough = (
        df.filter(col("Borough").isNotNull())  # remove null boroughs
        .groupBy("Borough")
        .agg(count("*").alias("ride_count"))
        .orderBy(col("ride_count").desc())
    )

    borough.write.mode("overwrite").parquet(
         f"{GOLD_PATH}/demand_by_borough/"
    )

    # Q3: Most popular routes
    routes = (
        df.filter(
            col("Borough").isNotNull() & col("DO_Borough").isNotNull()
        )  # ensure valid zones
        .groupBy("Borough", "DO_Borough")  # use names instead of IDs
        .agg(count("*").alias("ride_count"))  
        .orderBy(col("ride_count").desc())
    )

    routes.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/popular_routes/"
    )

    # Q4 avg duration
    avg_duration = df.agg(
        (avg("duration_sec") / 60).alias("avg_duration_min")  # convert to minutes
    )

    avg_duration.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/avg_duration/"
    )

    # Q5 avg distance
    avg_distance = df.agg(
        avg("trip_distance").alias("avg_distance")
    )

    avg_distance.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/avg_distance/"
    )

    # avg passengers 
    avg_passengers = df.filter(col("passenger_count") > 0).agg(
        avg("passenger_count").alias("avg_passenger_count")
    )

    avg_passengers.write.mode("overwrite").parquet(
        f"{GOLD_PATH}/avg_passengers/"
    )

    logger.info("Gold layer completed.")


if __name__ == "__main__":
    main()