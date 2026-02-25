# Environmental Sensor Data System

## Overview
This project implements a scalable data pipeline for processing environmental sensor data. It uses **MongoDB (Time Series Collections)** for storage, **Apache Airflow** for orchestration, **Prometheus + Pushgateway** for KPI metrics, and **Grafana** for visualization — all containerized with Docker.

## System Architecture
| Component | Technology | Purpose |
|---|---|---|
| Database | MongoDB 7.0 (Time Series) | Raw sensor readings storage |
| Orchestration | Apache Airflow (CeleryExecutor) | DAG scheduling and monitoring |
| Message Broker | Redis | Celery task queue |
| Metadata Store | PostgreSQL | Airflow internal state |
| KPI Metrics | Prometheus + Pushgateway | Batch metrics ingestion and storage |
| Visualization | Grafana | KPI dashboard |

## Prerequisites
*   Docker
*   Docker Compose

## How to Start the System
1.  **Clone the repository**:
    ```bash
    git clone <repository_url>
    cd BatchProcessingPipeline
    ```

2.  **Start the services**:
    ```bash
    docker-compose up -d
    ```

3.  **Access the UIs**:
    | Service | URL | Credentials |
    |---|---|---|
    | Airflow | http://localhost:8080 | airflow / airflow |
    | Prometheus | http://localhost:9090 | — |
    | Pushgateway | http://localhost:9091 | — |
    | Grafana | http://localhost:3000 | admin / admin |

## How it Works

### 1. Data Ingestion (`sensor_batch_processing` DAG)
Reads the CSV in parallel chunks, cleans it, and inserts rows into MongoDB `sensor_readings` (a Time Series collection).

Cleaning steps:
- Removes duplicates
- Handles missing values
- Coerces data types (numeric, boolean)
- Parses epoch timestamps to `datetime`

### 3. KPI Reporting (`sensor_kpi_reporting` DAG)
Runs daily. Aggregates the previous day's data from MongoDB per device and pushes metrics to the Prometheus Pushgateway.

All metrics are labeled by `device` and `day`.

### 4. Visualization
*   **Prometheus** (`:9090`) — query metrics via PromQL directly
*   **Grafana** (`:3000`) — pre-built **Sensor KPI Dashboard** is provisioned automatically on startup, no manual setup needed

## File Structure
*   `docker-compose.yml` — full infrastructure definition
*   `scripts/batch_loader/` — CSV reading and MongoDB insertion
*   `scripts/data_cleaner/` — data transformation and validation
*   `scripts/kpi_aggregator/` — MongoDB aggregation + Prometheus push
*   `airflow/dags/sensor_batch_dag.py` — ingestion DAG
*   `airflow/dags/kpi_dag.py` — KPI reporting DAG
*   `prometheus/prometheus.yml` — Prometheus scrape config
*   `grafana/provisioning/` — auto-provisioned datasource and dashboard
