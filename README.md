# Real-Time Traffic Monitoring Pipeline

A near real-time traffic monitoring system for urban road networks in Hanoi,
Vietnam.

> This project is developed for educational purposes.

---

## Team

- Bui Manh Nam - 23020556
- Chu Anh Truong - 23020577
- Nong Son Tung - 23020571
- Pham Quang Vinh - 23020580
- Nguyen Quang Vinh - 23020579

**University:** VNU University of Engineering and Technology (UET)

---

## Overview

The system monitors traffic flow, congestion levels, and weather conditions
across roads in Hanoi. It provides:

- A dashboard for traffic status, vehicle counts, average speed, flow, and weather
- Historical data browsing by date and road
- Traffic density forecasting using the provided ML artifacts
- An interactive road map with congestion color coding
- Prometheus and Grafana monitoring for pipeline services

**Tech stack:** Apache Kafka, Apache Spark, Delta Lake, Google Cloud Storage,
FastAPI, Next.js 15, Prometheus, Grafana, Docker.

---

## Repository Layout

```text
.
+-- Landing/                 # Fetch/replay raw SUMO and weather data
+-- Bronze/                  # Landing JSON -> Bronze Delta
+-- Silver/                  # Traffic/weather join and aggregation
+-- Gold/                    # Dashboard-ready Delta data
+-- dashboard/
|   +-- backend/             # FastAPI + DuckDB + GCS + ML prediction API
|   +-- frontend/            # Next.js dashboard
+-- monitoring/              # Prometheus/Grafana provisioning
+-- data/                    # Road lookup/reference files
+-- main.py                  # Pipeline orchestrator
+-- backfill.py              # Batch backfill for a missed date
+-- run.sh                   # VM-cluster infrastructure startup script
+-- stop.sh                  # VM-cluster shutdown script
```

---

## Prerequisites

For the full pipeline:

- Java 11 or 17
- Apache Kafka 3.x, preferably KRaft mode
- Apache Spark 3.5.1
- Python 3.11+
- Node.js 18+
- Docker + Docker Compose
- A Google Cloud Storage bucket with write access
- Google Cloud credentials, either ADC or `GOOGLE_APPLICATION_CREDENTIALS`

For dashboard-only usage:

- Python 3.11+
- Node.js 18+
- Access to a GCS bucket that already contains Delta/Parquet output

---

## Environment Variables

Copy the sample env file:

```bash
cp .env.example .env
```

Main pipeline variables:

| Variable | Required | Description |
|---|---:|---|
| `KAFKA_CLUSTER_ID` | VM script only | KRaft cluster ID. Generate with `kafka-storage.sh random-uuid`. |
| `KAFKA_BOOTSTRAP_SERVERS` | Yes | Kafka bootstrap servers, for example `master14:9092` or `localhost:9092`. |
| `SPARK_MASTER` | Yes | Spark master URL, for example `spark://master14:7077` or `spark://localhost:7077`. |
| `GCS_BUCKET_NAME` | Yes | GCS bucket used by the pipeline. |
| `GOOGLE_APPLICATION_CREDENTIALS` | Usually | Path to a service account JSON key. Omit only if ADC is already configured. |
| `SUMO_OSM_FILE` | No | Optional path to the OSM file used by `Landing/retrieve_sumo_traffic.py`. |
| `SPARK_SUBMIT` | No | Override path to `spark-submit` if auto-detection fails. |
| `UVICORN_BIN` | No | Override path to `uvicorn` if auto-detection fails. |
| `NPM_BIN` | No | Override path to `npm` if auto-detection fails. |
| `FRONTEND_DIR` | No | Override path to `dashboard/frontend` if needed. |

Dashboard backend variables:

| Variable | Required | Description |
|---|---:|---|
| `GCS_BUCKET` | Yes | GCS bucket read by the dashboard backend. This is separate from `GCS_BUCKET_NAME`. |
| `GCS_PREFIX` | Yes | Usually `gold` for the main dashboard. The map/forecast endpoints derive `silver` from this prefix. |
| `GCP_PROJECT` | No | GCP project ID used by the storage client. |
| `DUCKDB_PATH` | No | DuckDB database path. Defaults to `:memory:`. |
| `DASHBOARD_CACHE_DIR` | No | Local cache directory for downloaded Parquet files. |
| `CORS_ORIGINS` | No | Allowed frontend origins. Defaults to localhost frontend URLs. |

If you run the full pipeline with `main.py`, root `.env` is loaded by the
pipeline. If you run the dashboard backend separately from `dashboard/backend`,
create `dashboard/backend/.env` for the dashboard-specific variables.

---

## Expected GCS Layout

The code uses this bucket layout:

```text
gs://<bucket>/
+-- output_sumo/                 # Source SUMO XML files, read by retrieve_sumo_traffic.py
+-- simulation/traffic/          # Parsed SUMO JSON by date and interval
+-- landing/traffic/             # Replayed traffic JSON
+-- landing/weather/             # Weather JSON
+-- bronze/traffic/              # Bronze Delta table
+-- bronze/weather/              # Bronze Delta table
+-- silver/                      # Silver Delta table
+-- gold/                        # Gold Delta table
+-- checkpoints/                 # Spark streaming checkpoints
```

The dashboard reads Parquet files from the Delta table directories. Set
`GCS_PREFIX=gold` for the main dashboard view.

---

## Installation

### 1. Clone

```bash
git clone <repo-url>
cd Team13-TrafficFlowPredictionSystem
```

### 2. Python Environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
pip install -r dashboard/backend/requirements.txt
```

On Windows PowerShell:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r dashboard/backend/requirements.txt
```

### 3. Frontend

```bash
cd dashboard/frontend
npm install
npm run build
cd ../..
```

---

## Prepare SUMO Simulation Data

Run this once after SUMO XML output files have been uploaded to
`gs://<bucket>/output_sumo/`.

```bash
python Landing/retrieve_sumo_traffic.py
```

For a date range:

```bash
python Landing/retrieve_sumo_traffic.py --start-date 2026-05-01 --end-date 2026-05-31
```

By default, this script reads:

```text
dashboard/backend/app/utils/maps/nghia_do_cut.osm.xml
```

If your OSM file is stored elsewhere, set:

```bash
SUMO_OSM_FILE=/path/to/nghia_do_cut.osm.xml
```

---

## Running Option A: VM Cluster

Use this option on the original project VM cluster.

### 1. Start Infrastructure

```bash
bash run.sh
```

Important: `run.sh` is written for the project VM cluster and assumes paths and
hosts such as:

```text
/home/dis/Project
/opt/kafka
/opt/spark
master14
worker141
worker142
```

Update these values in `run.sh` before using it on another machine or cluster.

### 2. Start the Pipeline

```bash
source venv/bin/activate
python main.py
```

To use simulation data from a specific date:

```bash
python main.py 2026-05-27
```

Note: this does not replay the entire date from 00:00. The replay scripts use
the current wall-clock time and start from the next 3-minute interval. For
example, if you start at 10:14, replay begins from around 10:15 for the selected
date.

### 3. Stop

Press `Ctrl+C` to stop `main.py`, then run:

```bash
bash stop.sh
```

`stop.sh` is also VM-cluster specific and contains the same kind of hard-coded
paths/hosts as `run.sh`.

---

## Running Option B: Standalone Full Pipeline

Use this option when you want to run outside the original VM scripts.

### 1. Start Kafka Yourself

Start Kafka using your own local or cluster setup. Make sure the value below is
reachable from the machine running `main.py`:

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

The pipeline creates these Kafka topics automatically if the broker is reachable:

```text
traffic_simulate
weather_raw
```

### 2. Start Spark Yourself

Start Spark and set:

```bash
SPARK_MASTER=spark://localhost:7077
```

If you run Spark locally without a standalone master, update `SPARK_MASTER` in
`.env` to match your setup.

### 3. Set GCS and Credentials

```bash
GCS_BUCKET_NAME=your_gcs_bucket_name
GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcp_credentials.json
```

### 4. Run the Pipeline

```bash
source venv/bin/activate
python main.py
```

This starts:

- SUMO traffic replay
- Weather retrieval
- Dashboard backend on port `8001`
- Frontend on port `3000`
- Spark Bronze -> Silver -> Gold streaming pipeline

---

## Running Option C: Dashboard Only

Use this option when the GCS bucket already has `gold/` and `silver/` data and
you only want to view it.

### 1. Backend Environment

Create `dashboard/backend/.env`:

```bash
GCS_BUCKET=your_gcs_bucket_name
GCS_PREFIX=gold
GCP_PROJECT=your_gcp_project_id
GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcp_credentials.json
CORS_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
```

### 2. Start Backend

```bash
cd dashboard/backend
uvicorn app.main:app --host 0.0.0.0 --port 8001
```

API docs:

```text
http://localhost:8001/docs
```

### 3. Start Frontend

In another terminal:

```bash
cd dashboard/frontend
npm run dev
```

Frontend:

```text
http://localhost:3000
```

The frontend automatically calls `http://<current-host>:8001` if
`NEXT_PUBLIC_API_BASE_URL` is not set.

### 4. Optional Docker Dashboard Mode

The dashboard Docker Compose files use backend port `8000`, not `8001`.

```bash
cd dashboard
docker compose -f docker-compose.dev.yml up --build
```

Then open:

```text
http://localhost:3000
```

---

## Web Interfaces

When using the full VM/pipeline mode:

| Interface | URL |
|---|---|
| Dashboard | `http://<host>:3000` |
| API docs | `http://<host>:8001/docs` |
| Spark UI | `http://<host>:8080` |
| Kafka UI | `http://<host>:8085` |
| Grafana | `http://<host>:3001` |
| Prometheus | `http://<host>:9090` |
| Pipeline metrics | `http://<host>:8000/metrics` |

---

## Backfill

Use backfill when the pipeline missed a past date. This reads simulation traffic
directly from GCS, fetches archive weather if needed, then rewrites the matching
Bronze/Silver/Gold date partitions.

```bash
source venv/bin/activate
spark-submit --packages io.delta:delta-spark_2.12:3.2.0 backfill.py 2026-05-27
```

Backfill expects simulation data at:

```text
gs://<bucket>/simulation/traffic/2026-05-27/*.json
```

---

## Demo

| View | Screenshot |
|---|---|
| Main dashboard | ![Main dashboard](docs/dashboard.png) |
| Realtime map | ![Realtime map](docs/visualization_map.png) |
| Congestion forecast map | ![Congestion forecast map](docs/realtime.png) |
| Grafana metrics | ![Grafana metrics](docs/grafana.png) |

---

## Troubleshooting

### Kafka is unreachable

Check `KAFKA_BOOTSTRAP_SERVERS`, broker listeners, firewall rules, and whether
the hostnames used by Kafka are resolvable from the machine running `main.py`.

### Spark cannot find Delta classes

Run Spark jobs with:

```bash
--packages io.delta:delta-spark_2.12:3.2.0
```

`main.py` already adds this package when it launches the Spark pipeline through
`spark-submit`.

### Dashboard says `GCS_BUCKET is not configured`

The dashboard backend uses `GCS_BUCKET`, not `GCS_BUCKET_NAME`. Add this to
`dashboard/backend/.env` when running the dashboard standalone.

### Frontend cannot reach the API

For standalone dashboard mode, the backend normally runs on port `8001`.
Set this if needed:

```bash
NEXT_PUBLIC_API_BASE_URL=http://localhost:8001
```

For dashboard Docker Compose mode, the backend normally runs on port `8000`.

### SUMO retrieval cannot find the OSM file

Set:

```bash
SUMO_OSM_FILE=/path/to/nghia_do_cut.osm.xml
```

### `python main.py 2026-05-27` does not replay from midnight

This is expected. The replay process follows the current wall-clock interval.
Use `backfill.py` if you need to process a full past date in batch mode.
