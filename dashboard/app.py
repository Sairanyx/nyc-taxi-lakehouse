import sys
import os
import duckdb
import streamlit as st
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.settings import MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET, GOLD_PATH_DUCKDB

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

con = get_duckdb()

# Overview metrics

st.header("Monthly Averages")

df_duration = con.execute(f"""
    SELECT month, avg_duration_min
    FROM read_parquet('s3://{GOLD_PATH_DUCKDB}/avg_duration/*.parquet')
    ORDER BY month
""").df()

df_distance = con.execute(f"""
    SELECT month, avg_distance
    FROM read_parquet('s3://{GOLD_PATH_DUCKDB}/avg_distance/*.parquet')
    ORDER BY month
""").df()

df_passengers = con.execute(f"""
    SELECT month, avg_passenger_count
    FROM read_parquet('s3://{GOLD_PATH_DUCKDB}/avg_passengers/*.parquet')
    ORDER BY month
""").df()

col1, col2, col3 = st.columns(3)
with col1:
    st.subheader("Avg Duration (min)")
    st.line_chart(df_duration.set_index("month"))
with col2:
    st.subheader("Avg Distance (miles)")
    st.line_chart(df_distance.set_index("month"))
with col3:
    st.subheader("Avg Passengers")
    st.line_chart(df_passengers.set_index("month"))

# Hourly Demand

st.header("Ride Demand by Hour")

df_hourly = con.execute(f"""
    SELECT hour, ride_count
    FROM read_parquet('s3://{GOLD_PATH_DUCKDB}/hourly_demand/*.parquet')
    ORDER BY hour
""").df()

st.bar_chart(df_hourly.set_index("hour"))

# Borough demand

st.header("Ride Demand by Borough")

df_borough = con.execute(f"""
    SELECT Borough, ride_count
    FROM read_parquet('s3://{GOLD_PATH_DUCKDB}/demand_by_borough/*.parquet')
    ORDER BY ride_count DESC
""").df()

st.bar_chart(df_borough.set_index("Borough"))

# Popular routes

st.header("Most Popular Routes")

df_routes = con.execute(f"""
    SELECT Borough, DO_Borough, ride_count
    FROM read_parquet('s3://{GOLD_PATH_DUCKDB}/popular_routes/*.parquet')
    ORDER BY ride_count DESC
    LIMIT 20
""").df()

df_routes.columns = ["From", "To", "Ride Count"]
st.dataframe(df_routes, use_container_width=True)