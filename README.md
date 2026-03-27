# nyc-taxi-lakehouse

End-to-end big data pipeline built with Apache Spark and MinIO. Ingests NYC TLC 2025 Yellow Taxi trip data through a medallion architecture (raw → silver → gold), with a FastAPI layer exposing analytical endpoints and a Streamlit dashboard for live data exploration.

> **DEAI - Big Data Engineering | Final Project | 2026**  
> Authors: **Eduard Rednic** & **Zoi Theofilakou**

---

## Overview

This project implements a full data lakehouse pipeline using real-world NYC TLC 2025 Yellow Taxi data (~48 million trips). It demonstrates data ingestion, distributed processing with Apache Spark, layered storage in MinIO, and an analytical API backed by DuckDB. A taxi zone lookup table is joined during processing to map location IDs to human-readable borough and zone names.

---

## Architecture
```
NYC TLC Website (2025 Yellow Taxi data)
     │
     ▼
MinIO (raw/)              ← Original Parquet files, never modified
     │
     ▼
Apache Spark              ← Clean, transform, aggregate
     │
     ├──▶ MinIO (silver/)  ← Cleaned, filtered and deduplicated data
     │
     └──▶ MinIO (gold/)    ← Aggregated analytics tables
                │
                ├──▶ DuckDB
                │       │
                │       ├──▶ FastAPI    ← Analytical REST API
                │       └──▶ Streamlit  ← Interactive dashboard
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Object storage | MinIO (S3-compatible) |
| Data processing | Apache Spark (PySpark) |
| Data format | Parquet |
| Query engine | DuckDB |
| API server | FastAPI + Uvicorn |
| Dashboard | Streamlit |
| Orchestration | Docker Compose |

---

## Data Lake Structure
```
taxi/
├── raw/                        # Original downloaded data, never modified
│   ├── yellow_tripdata_2025-01.parquet
│   ├── ...
│   └── taxi_zone_lookup.csv
├── silver/                     # Cleaned, typed, and deduplicated data
│   └── clean_trips/
└── gold/                       # Aggregated tables ready for analysis
    ├── hourly_demand/
    ├── demand_by_borough/
    ├── popular_routes/
    ├── avg_duration/
    ├── avg_distance/
    └── avg_passengers/
```

---

## API Endpoints

Full documentation available at `http://localhost:8000/docs` once the stack is running.

| Endpoint | Description |
|---|---|
| `GET /hourly-demand` | Total rides per hour of the day (0-23) |
| `GET /demand-by-borough` | Total rides grouped by NYC borough |
| `GET /popular-routes` | Top 20 most travelled borough-to-borough routes |
| `GET /avg-duration` | Average trip duration in minutes per month |
| `GET /avg-distance` | Average trip distance in miles per month |
| `GET /avg-passengers` | Average passenger count per trip per month |

---

## Dashboard

The Streamlit dashboard is available at `http://localhost:8501` and shows interactive charts built directly on top of the gold layer — monthly averages, hourly demand, borough breakdown, and popular routes.

---

## Analysis Questions

1. What are the busiest hours of the day for NYC taxi rides?
2. Which NYC borough has the highest ride demand?
3. What are the most popular pickup-to-dropoff borough routes?
4. How does average trip duration change across the year?
5. How does average trip distance change across the year?
6. How does average passenger count change across the year?

---

## Getting Started

### Prerequisites

- Docker + Docker Compose

### 1. Clone the repo
```bash
git clone https://github.com/Sairanyx/nyc-taxi-lakehouse.git
cd nyc-taxi-lakehouse
```

### 2. Create your .env file
```bash
cp environment/.env.example environment/.env
```

### 3. Start the full stack
```bash
cd environment
docker compose up --build
```

| Service | URL |
|---|---|
| MinIO console | http://localhost:9001 |
| Spark UI | http://localhost:8080 |
| FastAPI docs | http://localhost:8000/docs |
| Streamlit dashboard | http://localhost:8501 |

### 4. Download and ingest the data
```bash
python pipeline/download/download.py
```

### 5. Run the Spark pipeline
```bash
# Enter the Spark container
docker exec -it spark-master bash

# Bronze layer — raw → bronze
python3 /opt/pipeline/bronze/ingest.py

# Silver layer — bronze → silver (clean and transform)
python3 /opt/pipeline/silver/clean_silver.py

# Gold layer — silver → gold (aggregate)
python3 /opt/pipeline/gold/build_gold.py
```

The API and dashboard will automatically serve the gold layer data.

---

## Project Structure
```
nyc-taxi-lakehouse/
├── pipeline/
│   ├── download/
│   │   └── download.py          # Downloads data from TLC and uploads to MinIO
│   ├── bronze/
│   │   └── ingest.py            # Raw → bronze
│   ├── silver/
│   │   └── clean_silver.py      # Bronze → silver (cleaning)
│   ├── gold/
│   │   └── build_gold.py        # Silver → gold (aggregations)
│   └── validation/
│       ├── validate_bronze.py
│       ├── validate_silver.py
│       └── validate_gold.py
├── api/
│   └── main.py                  # FastAPI endpoints
├── dashboard/
│   └── app.py                   # Streamlit dashboard
├── config/
│   ├── settings.py              # Shared paths and credentials
│   └── spark_config.py          # Spark session factory
├── environment/
│   ├── docker-compose.yml
│   ├── Dockerfile.api
│   ├── Dockerfile.dashboard
│   └── Dockerfile.spark
├── data/
│   └── raw/                     # Local staging folder (gitignored)
├── requirements.txt
├── .gitignore
└── README.md
```

---

## License

Project - DEAI Big Data Engineering, 2026, Turku AMK.