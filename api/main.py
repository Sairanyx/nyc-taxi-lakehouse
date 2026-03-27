"""
NYC Taxi Lakehouse - API
Serves aggregated gold layer data from MinIO with DuckDB and FastAPI
"""

import sys
import os
import duckdb
from fastapi import FastAPI

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from config.settings import MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET, GOLD_PATH_DUCKDB

app = FastAPI(
    title="NYC Taxi Lakehouse API",
    description="Endpoints for NYC Yellow Taxi 2025 trip data",
    version="1.0.0"
)

@app.get("/")
def health_check():
    """Returns a status message confirming the API is running."""
    return {"status": "ok", "message": "NYC Taxi Lakehouse API is running"}

def get_duckdb():
    """
    Creates a DuckDB connection that is configured to read parquet files
    from MinIO using S3-compatible API.
    """
    con = duckdb.connect()
    con.execute(f"""
        SET s3_endpoint='{MINIO_ENDPOINT.replace("http://", "")}';
        SET s3_access_key_id='{MINIO_ACCESS}';
        SET s3_secret_access_key='{MINIO_SECRET}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con

@app.get("/hourly-demand")
def get_hourly_demand():
    """Returns total ride count for each hour of the day (0-23)."""
    con = get_duckdb()
    result = con.execute(f"""
        SELECT hour, ride_count
        FROM read_parquet('{GOLD_PATH_DUCKDB}/hourly_demand/*.parquet')
        ORDER BY hour
    """).fetchall()
    return [{"hour": row[0], "ride_count": row[1]} for row in result]

@app.get("/demand-by-borough")
def get_demand_by_borough():
    """Returns total ride count grouped by NYC pickup borough."""
    con = get_duckdb()
    result = con.execute(f"""
        SELECT Borough, ride_count
        FROM read_parquet('{GOLD_PATH_DUCKDB}/demand_by_borough/*.parquet')
        ORDER BY ride_count DESC
    """).fetchall()
    return [{"borough": row[0], "ride_count": row[1]} for row in result]

@app.get("/popular-routes")
def get_popular_routes():
    """Returns the top 20 most popular borough-to-borough routes."""
    con = get_duckdb()
    result = con.execute(f"""
        SELECT Borough, DO_Borough, ride_count
        FROM read_parquet('{GOLD_PATH_DUCKDB}/popular_routes/*.parquet')
        ORDER BY ride_count DESC
        LIMIT 20
    """).fetchall()
    return [{"from": row[0], "to": row[1], "ride_count": row[2]} for row in result]

@app.get("/avg-duration")
def get_avg_duration():
    """Returns average trip duration in minutes for each month."""
    con = get_duckdb()
    result = con.execute(f"""
        SELECT month, avg_duration_min
        FROM read_parquet('{GOLD_PATH_DUCKDB}/avg_duration/*.parquet')
        ORDER BY month
    """).fetchall()
    return [{"month": row[0], "avg_duration_min": round(row[1], 2)} for row in result]

@app.get("/avg-distance")
def get_avg_distance():
    """Returns average trip distance in miles for each month."""
    con = get_duckdb()
    result = con.execute(f"""
        SELECT month, avg_distance
        FROM read_parquet('{GOLD_PATH_DUCKDB}/avg_distance/*.parquet')
        ORDER BY month
    """).fetchall()
    return [{"month": row[0], "avg_distance_miles": round(row[1], 2)} for row in result]

@app.get("/avg-passengers")
def get_avg_passengers():
    """Returns average passenger count per trip for each month."""
    con = get_duckdb()
    result = con.execute(f"""
        SELECT month, avg_passenger_count
        FROM read_parquet('{GOLD_PATH_DUCKDB}/avg_passengers/*.parquet')
        ORDER BY month
    """).fetchall()
    return [{"month": row[0], "avg_passenger_count": round(row[1], 2)} for row in result]
