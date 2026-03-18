from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_PATH = BASE_DIR / "storage" / "taxi" / "raw" / "trips.parquet"

# schema validation
REQUIRED_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "passenger_count",
    "trip_distance",
]

def validate_schema(df):
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")


#null checks
def validate_nulls(df):
    critical_cols = ["pickup_datetime", "dropoff_datetime", "trip_distance"]

    for col in critical_cols:
        if df[col].isna().any():
            raise ValueError(f"Null values found in {col}")
        
def validate_logic(df):

    # --- Duration calculation (once) ---
    duration = (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds()

    # --- Distance checks ---
    if (df["trip_distance"] < 0).any():
        raise ValueError("Negative trip_distance detected")

    zero_distance = df[df["trip_distance"] == 0]
    if not zero_distance.empty:
        print(f"Warning: {len(zero_distance)} trips with zero distance")

    # --- Time checks ---
    small_errors = duration[(duration < 0) & (duration > -60)]
    medium_errors = duration[(duration <= -60) & (duration > -3600)]
    large_errors = duration[duration <= -3600]

    if len(small_errors) > 0:
        print(f"Warning: {len(small_errors)} small time inconsistencies (<1 min)")

    if len(medium_errors) > 0:
        print(f"Warning: {len(medium_errors)} medium time inconsistencies (<1 hour)")

    if len(large_errors) > 0:
        print(f"ERROR: {len(large_errors)} severely invalid time records")

        # 🔴 THIS IS CRITICAL
        if len(large_errors) > 100:
            raise ValueError("Too many severely invalid time records")

    # --- Passenger ---
    if (df["passenger_count"] < 0).any():
        raise ValueError("Negative passenger_count detected")

        # Only fail on serious corruption
        if len(large_errors) > 100:
            raise ValueError("Too many severely invalid time records")

    # --- Passenger ---
    if (df["passenger_count"] < 0).any():
        raise ValueError("Negative passenger_count detected")

    print("\n--- Validation Summary ---")
    print(f"Total rows: {len(df)}")
    print(f"Zero distance: {len(zero_distance)}")
    print(f"Small time errors: {len(small_errors)}")
    print(f"Medium time errors: {len(medium_errors)}")
    print(f"Large time errors: {len(large_errors)}")

    
#range checks
def validate_ranges(df):
    if (df["trip_distance"] > 100).any():
        print("Warning: unusually large trip distances found")

    if (df["passenger_count"] > 6).any():
        print("Warning: unrealistic passenger counts found")

#duplicate check
def validate_duplicates(df):
    if df.duplicated().any():
        print("Warning: duplicate rows detected")

#orchestrator
def run_validation():
    print("Loading data...")
    df = pd.read_parquet(RAW_PATH)

    print("Running validation...")

    validate_schema(df)
    validate_nulls(df)
    validate_logic(df)
    validate_ranges(df)
    validate_duplicates(df)




    print("Validation completed successfully")

if __name__ == "__main__":
    run_validation()