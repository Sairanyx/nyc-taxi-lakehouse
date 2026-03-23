import pandas as pd

df = pd.read_parquet(r"C:\Projects\nyc-taxi-lakehouse\data\local\yellow_tripdata_2025-01.parquet")

print ("\n --- First rows ---")
print(df.head())

print("\n --- Columns ---")
print(df.columns)

print("\n --- Info ---")
print(df.info())


