import pandas as pd
from pathlib import Path
import os



#project root -auto detected
BASE_DIR = Path(__file__).resolve().parent.parent

# data paths
LOCAL_DATA_PATH = BASE_DIR / "data" / "local"
RAW_PATH = BASE_DIR / "storage" / "taxi" / "raw"

def run_ingestion():
    # dynamic path
    file_path = LOCAL_DATA_PATH / "yellow_tripdata_2025-01.parquet"
    

    df = pd.read_parquet(file_path)

    #select only cols needed
    df = df [[
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "passenger_count",
            "trip_distance"
    ]]

    #rename cols (standard schema)
    df = df.rename (columns={
              "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id"
    })  

    os.makedirs(RAW_PATH, exist_ok=True)

    df.to_parquet(RAW_PATH / "trips.parquet", index=False)

    print("RAW layer created with clean schema")

    print("Rows:", len(df))
    print("Columns:", df.columns.tolist())

if __name__ == "__main__":
    run_ingestion()
 