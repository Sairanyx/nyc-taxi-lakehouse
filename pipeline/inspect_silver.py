import pandas as pd

def main():
    df = pd.read_parquet("storage/taxi/silver/clean_trips.parquet")

    print("=== DESCRIBE ===")
    print(df["trip_distance"].describe())

    print("\n=== TOP EXTREME VALUES ===")
    print(df.sort_values("trip_distance", ascending=False).head(20))

    print("\n=== QUANTILES ===")
    print(df["trip_distance"].quantile([0.90, 0.95, 0.99, 0.999]))


if __name__ == "__main__":
    main()