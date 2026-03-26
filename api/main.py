import sys
import os
import duckdb
from fastapi import FastAPI

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from config.settings import MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET, GOLD_PATH


app = FastAPI(
    title="NYC Taxi Lakehouse API",
    description="Endpoints for NYC Yellow Taxi 2025 trip data",
    version="1.0.0"
)

# Healthcheck

@app.get("/")
def health_check():
    return {"status": "ok", "message": "NYC Taxi Lakehouse API is running"}

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

# Endpoints

# 1. Hourly demand

@app.get("/hourly-demand")
def get_hourly_demand():
    con = get_duckdb()
    result = con.execute(f"""
        SELECT hour, ride_count
        FROM read_parquet('s3://{GOLD_PATH}/gold/hourly_demand/*.parquet')
        ORDER BY hour
    """.fetchall()
    return[{"hour": row[0], "ride_count": row[1]} for row in result])

# 2. Demand by borough

@app.get("/demand-by-borough")
def get_demand_by_borough():
    con = get_duckdb()
    result = con.execute(f"""
        SELECT Borough, ride_count
        FROM read_parquet('s3://{GOLD_PATH}/gold/demand_by_borough/*.parquet')
        ORDER BY ride_count DESC
    """).fetchall()
    return [{"borough": row[0], "ride_count": row[1]} for row in result]

# 3. Top 20 popular routes

@app.get("/popular-routes")
def get_popular_routes():
    con = get_duckdb()
    result = con.execute(f"""
        SELECT Borough, DO_Borough, ride_count
        FROM read_parquet('s3://{GOLD_PATH}/gold/popular_routes/*.parquet')
        ORDER BY ride_count DESC
        LIMIT 20
    """).fetchall()
    return [{"from": row[0], "to": row[1], "ride_count": row[2]} for row in result]

# 4. Average duration

@app.get("/avg-duration")
def get_avg_duration():
    con = get_duckdb()
    result = con.execute(f"""
        SELECT avg_duration_min
        FROM read_parquet('s3://{GOLD_PATH}/gold/avg_duration/*.parquet')
    """).fetchone()
    return {"avg_duration_min": round(result[0], 2)}

# 5. Average distance

@app.get("/avg-distance")
def get_avg_distance():
    con = get_duckdb()
    result = con.execute(f"""
        SELECT avg_distance
        FROM read_parquet('s3://{GOLD_PATH}/gold/avg_distance/*.parquet')
    """).fetchone()
    return {"avg_distance_miles": round(result[0], 2)}

# 6. Average passengers

@app.get("/avg-passengers")
def get_avg_passengers():
    con = get_duckdb()
    result = con.execute(f"""
        SELECT avg_passenger_count
        FROM read_parquet('s3://{GOLD_PATH}/gold/avg_passengers/*.parquet')
    """).fetchone()
    return {"avg_passenger_count": round(result[0], 2)}
