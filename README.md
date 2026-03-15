# nyc-taxi-lakehouse

End-to-end big data pipeline built with Apache Spark and MinIO. Ingests NYC TLC trip data through a medallion architecture (raw → silver → gold), with a FastAPI layer exposing analytical endpoints.

> **DEAI - Big Data Engineering | Project | 2026**  
> Authors: **Eduard Rednic** & **Zoi Theofilakou**

---

## Overview

This project implements a full data lakehouse pipeline using real-world NYC taxi trip data. It demonstrates data ingestion, distributed processing, layered storage, and an analytical API, all built from scratch using industry-standard tools.

---

## Architecture

```
NYC TLC Data
     │
     ▼
MinIO (raw/)          ← Parquet files, unmodified
     │
     ▼
Apache Spark          ← Clean, transform, aggregate
     │
     ├──▶ MinIO (silver/)   ← Cleaned & joined data
     │
     └──▶ MinIO (gold/)     ← Aggregated analytics tables
                │
                ▼
           FastAPI server    ← /rides-per-day, /provider-summary, /top-routes
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Object storage | MinIO (S3-compatible) |
| Data processing | Apache Spark (PySpark) |
| Data format | Parquet |
| API server | FastAPI + Uvicorn |
| Query engine | DuckDB (gold layer reads) |

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

## Getting Started

### Prerequisites

- Python 3.10+
- Java 11+ (required for Spark)
- Docker (for running MinIO locally)

### 1. Start MinIO

```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  quay.io/minio/minio server /data --console-address ":9001"
```

### 2. Install dependencies

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3. Run the pipeline

```bash
# Download and ingest raw data into MinIO
python pipeline/ingest.py

# Run Spark processing (raw → silver → gold)
python pipeline/process.py

# Start the API server
uvicorn api.main:app --reload
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
│   └── process.py       # Spark jobs: raw → silver → gold
├── api/
│   └── main.py          # FastAPI server and endpoints
├── notebooks/           # Exploratory analysis
├── requirements.txt
├── .gitignore
└── README.md
```

---

## License

Project - DEAI Big Data Engineering, 2026, Turku AMK.