from pathlib import Path
import pandas as pd

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent

RAW_PATH = BASE_DIR / "storage" / "taxi" / "raw" / "trips.parquet"
SILVER_PATH = BASE_DIR / "storage" / "taxi" / "silver" / "clean_trips.parquet"


# -----------------------------
# LOAD
# -----------------------------
def load_data():
    print("Loading raw data...")
    return pd.read_parquet(RAW_PATH)


# -----------------------------
# FEATURE ENGINEERING
# -----------------------------
def add_duration(df):
    df["duration_sec"] = (
        df["dropoff_datetime"] - df["pickup_datetime"]
    ).dt.total_seconds()
    return df


# -----------------------------
# CLEANING STEPS
# -----------------------------
def drop_invalid_rows(df):
    print("Dropping invalid rows...")

    initial_len = len(df)

    # replace missing passenger_count instead of dropping
    df["passenger_count"] = df["passenger_count"].fillna(0)

    # logic checks
    df = df[df["duration_sec"] >= -3600]
    df = df[df["trip_distance"] >= 0]
    df = df[df["passenger_count"] >= 0]

    print(f"Removed {initial_len - len(df)} invalid rows")
    return df


def drop_medium_time_errors(df):
    print("Dropping medium time inconsistencies...")

    initial_len = len(df)

    mask = (df["duration_sec"] < -60) & (df["duration_sec"] >= -3600)
    df = df[~mask]

    print(f"Removed {initial_len - len(df)} medium error rows")
    return df


def fix_small_time_errors(df):
    print("Fixing small time inconsistencies...")

    mask = (df["duration_sec"] < 0) & (df["duration_sec"] > -60)

    # align timestamps
    df.loc[mask, "dropoff_datetime"] = df.loc[mask, "pickup_datetime"]

    return df


def remove_duplicates(df):
    print("Removing duplicates...")

    initial_len = len(df)

    df = df.drop_duplicates()

    print(f"Removed {initial_len - len(df)} duplicate rows")
    return df


# -----------------------------
# SAVE
# -----------------------------
def save_data(df):
    print("Saving cleaned data...")

    SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(SILVER_PATH, index=False)

    print(f"Saved to {SILVER_PATH}")


# -----------------------------
# ORCHESTRATOR
# -----------------------------
def run_cleaning():
    df = load_data()

    # ensure datetime
    df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"], errors="coerce")
    df["dropoff_datetime"] = pd.to_datetime(df["dropoff_datetime"], errors="coerce")

    df = add_duration(df)

    print("NaN pickup:", df["pickup_datetime"].isna().sum())
    print("NaN dropoff:", df["dropoff_datetime"].isna().sum())
    print("NaN duration:", df["duration_sec"].isna().sum())

    print("NaN trip_distance:", df["trip_distance"].isna().sum())
    print("NaN passenger_count:", df["passenger_count"].isna().sum())

    # BEFORE
    print("\n--- BEFORE CLEANING ---")
    print("Total rows:", len(df))
    print("Severe (< -3600):", (df["duration_sec"] < -3600).sum())
    print("Medium (-60 to -3600):", ((df["duration_sec"] < -60) & (df["duration_sec"] >= -3600)).sum())
    print("Small (-60 to 0):", ((df["duration_sec"] < 0) & (df["duration_sec"] >= -60)).sum())

    # CLEAN
    df = drop_invalid_rows(df)
    df = drop_medium_time_errors(df)
    df = fix_small_time_errors(df)

    # recompute after fixes
    df = add_duration(df)

    df = remove_duplicates(df)

    # SAVE
    save_data(df)

    # AFTER
    print("\n--- AFTER CLEANING ---")
    print("Total rows:", len(df))


# -----------------------------
if __name__ == "__main__":
    run_cleaning()