from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum


def create_spark():
    return (
        SparkSession.builder
        .appName("Validate Gold")
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )


def validate_df(df, name):
    print(f"\n===== {name} =====")
    df.printSchema()
    print("Row count:", df.count())

    df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).show()

    df.show(5)


def run_validation():
    spark = create_spark()

    # hourly demand
    df_hour = spark.read.parquet("s3a://nyc-taxi/gold/hourly_demand/")
    validate_df(df_hour, "Hourly demand")

    df_hour.select("hour").distinct().orderBy("hour").show(24)

    # borough
    df_borough = spark.read.parquet("s3a://nyc-taxi/gold/demand_by_borough/")
    df_borough = df_borough.filter(~col("Borough").isin("Unknown", "N/A"))
    validate_df(df_borough, "Demand by borough")

    # routes
    df_routes = spark.read.parquet("s3a://nyc-taxi/gold/popular_routes/")
    validate_df(df_routes, "Popular routes")

    df_routes.filter(col("ride_count") <= 0).show()

    # summary
    df_summary = spark.read.parquet("s3a://nyc-taxi/gold/summary_metrics/")
    validate_df(df_summary, "Summary metrics")

    df_summary.filter(col("avg_duration_sec") <= 0).show()
    df_summary.filter(col("avg_distance") <= 0).show()
    df_summary.filter(col("avg_passenger_count") <= 0).show()

    
    #  FINAL CONSISTENCY CHECK

    print("\n=== FINAL CONSISTENCY CHECK ===")

    total_hour = df_hour.agg(sum("ride_count")).first()[0]
    total_borough = df_borough.agg(sum("ride_count")).first()[0]
    total_routes = df_routes.agg(sum("ride_count")).first()[0]

    print("Hourly total:", total_hour)
    print("Borough total:", total_borough)
    print("Routes total:", total_routes)


if __name__ == "__main__":
    run_validation()