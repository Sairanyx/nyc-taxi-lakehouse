import pandas as pd

df = pd.read_parquet("storage/taxi/gold/hourly_metrics.parquet")

print(df)
print(df["trip_count"].sum())  # should equal total trips in Silver
print(df.describe())