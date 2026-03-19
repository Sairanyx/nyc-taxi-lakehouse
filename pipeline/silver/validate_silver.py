from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent

SILVER_PATH = BASE_DIR / "storage" / "taxi" / "silver" / "clean_trips.parquet"


def run_validation():
    print("Loading silver data...")
    df = pd.read_parquet(SILVER_PATH)

    print("\n--- SILVER VALIDATION ---")

    print("Negative duration:", (df["duration_sec"] < 0).sum())
    print("Duplicates:", df.duplicated().sum())
    print("Negative distance:", (df["trip_distance"] < 0).sum())
    print("Negative passengers:", (df["passenger_count"] < 0).sum())


if __name__ == "__main__":
    run_validation()