import pandas as pd
from pathlib import Path


# Paths
SILVER_PATH = Path("storage/taxi/silver/clean_trips.parquet")

GOLD_HOURLY_PATH = Path("storage/taxi/gold/ride_demand_hourly.parquet")
GOLD_ROUTES_PATH = Path("storage/taxi/gold/popular_routes.parquet")
GOLD_ROUTES_BOROUGH_PATH = Path("storage/taxi/gold/popular_routes_borough.parquet")
GOLD_DURATION_PATH = Path("storage/taxi/gold/avg_duration.parquet")
GOLD_PASSENGERS_PATH = Path("storage/taxi/gold/avg_passengers.parquet")
GOLD_PASSENGER_DIST_PATH = Path("storage/taxi/gold/passenger_distribution.parquet")
GOLD_DISTANCE_PATH = Path("storage/taxi/gold/avg_distance.parquet")
GOLD_BOROUGH_DEMAND_PATH = Path("storage/taxi/gold/borough_demand.parquet")



def load_silver():
    return pd.read_parquet(SILVER_PATH)


# -------------------------
# Q1: Ride Demand by Hour
# -------------------------
def transform_hourly_demand(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["hour"] = df["pickup_datetime"].dt.hour

    gold_hourly = (
        df.groupby("hour")
        .agg(
            ride_count=("pickup_datetime", "count"),
        )
        .reset_index()
        .sort_values("hour")
        .reset_index(drop=True)
    )

    return gold_hourly


# Q2A: Zone routes
def transform_popular_routes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df.dropna(subset=["pickup_zone", "dropoff_zone"])
    df = df[df["pickup_zone"] != df["dropoff_zone"]]

    return (
        df.groupby(["pickup_zone", "dropoff_zone"])
        .agg(ride_count=("pickup_datetime", "count"))
        .reset_index()
        .sort_values("ride_count", ascending=False)
        .reset_index(drop=True)
    )


# Q2B: Borough routes
def transform_routes_borough(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df.dropna(subset=["pickup_borough", "dropoff_borough"])
    df = df[df["pickup_borough"] != df["dropoff_borough"]]

    return (
        df.groupby(["pickup_borough", "dropoff_borough"])
        .agg(ride_count=("pickup_datetime", "count"))
        .reset_index()
        .sort_values("ride_count", ascending=False)
        .reset_index(drop=True)
    )



# Q3: avg duration
def transform_avg_duration(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    avg_sec = df["duration_sec"].mean()
    median_sec = df["duration_sec"].median()

    return pd.DataFrame({
        "avg_duration_sec": [avg_sec],
        "median_duration_sec": [median_sec],
        "avg_duration_min": [avg_sec / 60],
        "median_duration_min": [median_sec / 60],
    })

# Q4 A
def transform_avg_passengers(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Remove invalid passenger counts (0 = missing)
    df = df[df["passenger_count"] > 0]

    avg_passengers = df["passenger_count"].mean()


    return pd.DataFrame({
        "avg_passenger_count": [avg_passengers]
    })

# Q4B passenger distibution
def transform_passenger_distribution(df):
        df = df.copy()
        df = df[df["passenger_count"] > 0]

        return (
            df["passenger_count"]
            .value_counts()
            .reset_index()
            .rename(columns={"index": "passenger_count", "passenger_count": "ride_count"})
        )


# Q5 Average Trip Distance
def transform_avg_distance(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Core metrics 
    avg_distance = df["trip_distance"].mean()
    median_distance = df["trip_distance"].median()

    #  Distribution validation 
    skewness = df["trip_distance"].skew()

    # Validation metrics (used for skewness interpretation, not returned)
    percentiles = df["trip_distance"].quantile([0.25, 0.5, 0.75, 0.9, 0.99])

    # Ratio gives quick numerical comparison between mean and median
    mean_median_ratio = avg_distance / median_distance if median_distance != 0 else None

    # Optional: summary stats for debugging/validation (not returned)
    summary = df["trip_distance"].describe()

    #  Debug prints — use only during development ---
    print("Percentiles:\n", percentiles)
    print("Mean/Median ratio:", mean_median_ratio)
    print("Summary:\n", summary)

    # --- 4. Final Gold output (ONLY aggregated results) ---
    return pd.DataFrame({
        "avg_distance_miles": [avg_distance],
        "median_distance_miles": [median_distance],
        "distance_skewness": [skewness]
    })


def transform_borough_demand(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # --- Pickup demand ---
    pickup = (
        df.groupby("pickup_borough")
        .size()
        .reset_index(name="pickup_rides")
        .rename(columns={"pickup_borough": "borough"})
    )

    # --- Dropoff demand ---
    dropoff = (
        df.groupby("dropoff_borough")
        .size()
        .reset_index(name="dropoff_rides")
        .rename(columns={"dropoff_borough": "borough"})
    )

    # --- Merge both ---
    gold_borough = pd.merge(pickup, dropoff, on="borough", how="outer").fillna(0)

    # Convert to int (clean output)
    gold_borough["pickup_rides"] = gold_borough["pickup_rides"].astype(int)
    gold_borough["dropoff_rides"] = gold_borough["dropoff_rides"].astype(int)

    # Sort by total activity (optional but useful)
    gold_borough["total_rides"] = (
        gold_borough["pickup_rides"] + gold_borough["dropoff_rides"]
    )

    # Net flow (movement balance)
    gold_borough["net_flow"] = (
        gold_borough["pickup_rides"] - gold_borough["dropoff_rides"]
    )

    gold_borough = gold_borough.sort_values(
        "total_rides", ascending=False
    ).reset_index(drop=True)

    return gold_borough

# -------------------------
# Save functions
# -------------------------
def save_parquet(df: pd.DataFrame, path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


# -------------------------
# Main pipeline
# -------------------------
def main():
    df = load_silver()

    print(df.columns)

    # Q1
    gold_hourly = transform_hourly_demand(df)
    save_parquet(gold_hourly, GOLD_HOURLY_PATH)

    # Q2A - zone routes
    gold_routes = transform_popular_routes(df)
    save_parquet(gold_routes, GOLD_ROUTES_PATH)

    # Q2B - borough routes
    gold_routes_borough = transform_routes_borough(df)
    save_parquet(gold_routes_borough, GOLD_ROUTES_BOROUGH_PATH)

    # Q3
    gold_duration = transform_avg_duration(df)
    save_parquet(gold_duration, GOLD_DURATION_PATH)

    # Q4A - average passengers
    gold_passengers = transform_avg_passengers(df)
    save_parquet(gold_passengers, GOLD_PASSENGERS_PATH)

    # Q4B - passenger distribution (supporting analysis)
    gold_passenger_dist = transform_passenger_distribution(df)
    save_parquet(gold_passenger_dist, GOLD_PASSENGER_DIST_PATH)

    # Q5 - average trip distance
    gold_distance = transform_avg_distance(df)
    save_parquet(gold_distance, GOLD_DISTANCE_PATH)

    # Q6 - ride demand by borough
    gold_borough = transform_borough_demand(df)
    save_parquet(gold_borough, GOLD_BOROUGH_DEMAND_PATH)


    print("Gold layer created successfully")

    print("\nHourly demand:")
    print(gold_hourly.head())

    print("\nTop routes:")
    print(gold_routes.head())

    print(gold_routes_borough.head())


    print("\nAverage trip duration:")
    print(gold_duration.head())

    print("\nAverage passengers:")
    print(gold_passengers)

    print("\nPassenger distribution:")
    print(gold_passenger_dist.head())

    print("\nAverage trip distance:")
    print(gold_distance)

    print("\nRide demand by borough:")
    print(gold_borough.head())



if __name__ == "__main__":
    main()