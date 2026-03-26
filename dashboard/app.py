import sys
import os
import duckdb
import streamlit as st
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.settings import MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET, GOLD_PATH

def get_duckdb():
    con = duckdb.connect()
    con.execute(f"""
        SET s3_endpoint='{MINIO_ENDPOINT.replace("http://", "")}';
        SET s3_access_key_id='{MINIO_ACCESS}';
        SET s3_secret_access_key='{MINIO_SECRET}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con

# Page configurations

st.set_page_config(
    page_title="NYC Taxi Dashboard",
    page_icon="🚕",
    layout="wide"
)

st.title("🚕 NYC Taxi Lakehouse Dashboard")
st.markdown("NYC Yellow Taxi 2025 - Overview")

# Overview metrics

st.header("Overview")

con = get_duckdb()

avg_duration = con.execute(f"""
    SELECT avg_duration_min
    FROM read_parquet('s3://{GOLD_PATH}/gold/avg_duration/*.parquet')
""").fetchone()[0]

avg_distance = con.execute(f"""
    SELECT avg_distance
    FROM read_parquet('s3://{GOLD_PATH}/gold/avg_distance/*.parquet')
""").fetchone()[0]

avg_passengers = con.execute(f"""
    SELECT avg_passenger_count
    FROM read_parquet('s3://{GOLD_PATH}/gold/avg_passengers/*.parquet')
""").fetchone()[0]

col1, col2, col3 = st.columns(3)
col1.metric("Average Trip Duration", f"{round(avg_duration, 1)} min")
col2.metric("Average Trip Distance", f"{round(avg_distance, 1)} miles")
col3.metric("Average Passengers", f"{round(avg_passengers, 2)}")

# Hourly Demand

st.header("Ride Demand by Hour")

df_hourly = con.execute(f"""
    SELECT hour, ride_count
    FROM read_parquet('s3://{GOLD_PATH}/gold/hourly_demand/*.parquet')
    ORDER BY hour
""").df()

st.bar_chart(df_hourly.set_index("hour"))

# Borough demand

st.header("Ride Demand by Borough")

df_borough = con.execute(f"""
    SELECT Borough, ride_count
    FROM read_parquet('s3://{GOLD_PATH}/gold/demand_by_borough/*.parquet')
    ORDER BY ride_count DESC
""").df()

st.bar_chart(df_borough.set_index("Borough"))

# Popular routes

st.header("Most Popular Routes")

df_routes = con.execute(f"""
    SELECT Borough, DO_Borough, ride_count
    FROM read_parquet('s3://{GOLD_PATH}/gold/popular_routes/*.parquet')
    ORDER BY ride_count DESC
    LIMIT 20
""").df()

df_routes.columns = ["From", "To", "Ride Count"]
st.dataframe(df_routes, use_container_width=True)