# nyc-taxi-lakehouse

End-to-end big data pipeline built with Apache Spark and MinIO. Ingests NYC TLC 2025 High Volume FHV (Uber/Lyft) trip data through a medallion architecture (raw → silver → gold), with a FastAPI layer exposing analytical endpoints and a Streamlit dashboard for live data exploration.

> **DEAI - Big Data Engineering | Project | 2026**  
> Authors: **Eduard Rednic** & **Zoi Theofilakou**

---

## Overview

This project implements a full data lakehouse pipeline using real-world NYC TLC 2025 High Volume FHV data (Uber & Lyft). It demonstrates data ingestion, distributed processing, layered storage, and an analytical API, all built from scratch using industry-standard tools. A zone lookup table is joined during processing to map location IDs to human-readable borough and zone names.

---

## Architecture

```
NYC TLC Data
     │
     ▼
MinIO (raw/)              ← Parquet files, unmodified
     │
     ▼
Data Quality Checks       ← Row counts, null rates, schema validation
     │                       Pipeline halts on failure
     ▼
Apache Spark              ← Clean, transform, aggregate
     │
     ├──▶ MinIO (silver/)   ← Cleaned & joined data
     │
     └──▶ MinIO (gold/)     ← Aggregated analytics tables
                │
                ├──▶ FastAPI server    ← /rides-per-day, /provider-summary, /top-routes
                └──▶ Streamlit app     ← Interactive visual dashboard
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Object storage | MinIO (S3-compatible) |
| Data processing | Apache Spark (PySpark) |
| Data quality | Custom validation layer (PySpark) |
| Data format | Parquet |
| API server | FastAPI + Uvicorn |
| Dashboard | Streamlit |
| Query engine | DuckDB (gold layer reads) |
| Orchestration | Docker Compose |

---

## Data Lake Structure

```
taxi/
├── raw/        # Original downloaded data, never modified
├── silver/     # Cleaned, typed, and joined datasets
└── gold/       # Aggregated tables ready for analysis
```

---

## API Endpoints

Once the server is running, full documentation is available at `http://localhost:8000/docs`.

| Endpoint | Description |
|---|---|
| `GET /something` | ... |
| `GET /something2` | ... |
| `GET /something3` | ... |

---

## Dashboard

The Streamlit dashboard is available at `http://localhost:8501` and provides interactive charts and filters directly on top of the gold layer.

---

## Data Quality Checks

Between the raw and silver layers, the pipeline validates:

- **Row count** — fails if ingested rows fall below expected threshold
- **Null rate** — fails if critical columns (pickup time, location, fare) exceed null tolerance
- **Schema drift** — fails if column names or types differ from expected schema

If any check fails, the pipeline halts and logs the reason before writing to silver. No bad data reaches the gold layer.

---

## Getting Started

### Prerequisites

- Docker + Docker Compose

### Run the full stack

```bash
docker-compose up
```

| Service | URL |
|---|---|
| MinIO console | http://localhost:9001 |
| FastAPI docs | http://localhost:8000/docs |
| Streamlit dashboard | http://localhost:8501 |

### Run the pipeline manually

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Download and ingest raw data into MinIO

python pipeline/ingest.py

# Data quality checks

python pipeline/validate.py

# Run Spark processing (raw → silver → gold)

python pipeline/process.py

# Start the API server

uvicorn api.main:app --reload

# Start the dashboard

streamlit run dashboard/app.py
```

---

## Analysis Questions

1. 
2. 
3. 

---

## Project Structure

```
nyc-taxi-lakehouse/
├── pipeline/
│   ├── ingest.py        # Download data, upload to MinIO raw/
│   ├── validate.py      # Data quality checks between raw → silver
│   └── process.py       # Spark jobs: raw → silver → gold
├── api/
│   └── main.py          # FastAPI server and endpoints
├── dashboard/
│   └── app.py           # Streamlit dashboard
├── data/
│   └── taxi_zone_lookup.csv # NYC TLC zone ID → borough/zone name mapping
├── notebooks/           # Exploratory analysis
├── docker-compose.yml
├── requirements.txt
├── .gitignore
└── README.md
```

---

## License

Project - DEAI Big Data Engineering, 2026, Turku AMK.