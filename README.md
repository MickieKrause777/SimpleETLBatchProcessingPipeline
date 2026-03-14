# Environmental Sensor Data System

## Overview
This project implements a scalable data pipeline for processing environmental sensor data. It uses **MongoDB (Time Series Collections)** for storage, **Apache Airflow** for orchestration, **Prometheus + Pushgateway** for KPI metrics, and **Grafana** and **Streamlit with Plotly** for visualization — all containerized with Docker.

## Early Identification System
The platform includes an early anomaly identification system to proactively detect hardware malfunctions and operational inefficiencies:
*   **Micro-Batching & Custom Intervals:** The KPI aggregator dynamically supports custom time intervals (e.g., hourly runs), enabling faster insights and granular metric tracking.
*   **Statistical Variance Analysis:** Calculates data variance (e.g., Standard Deviation for Temperature, Humidity, and CO) natively in MongoDB to track signal stability over time.
*   **Contextual Anomaly Alerts:** Prometheus rules (`alert_rules.yml`) flag advanced behavioral issues such as stuck sensors (`SensorFrozen`), erratic readings (`SensorErratic`), and inefficient HVAC/Ventilation usage based on cross-referenced occupancy.
*   **Time Series Visualizations:** Streamlit dashboard panels visualize metrics over time using dual Y-axes to rapidly spot trends and correlations (e.g., comparing temp vs. CO levels).

## System Architecture
| Component | Technology | Purpose |
|---|---|---|
| Database | MongoDB 7.0 (Time Series) | Raw sensor readings storage |
| Orchestration | Apache Airflow (CeleryExecutor) | DAG scheduling and monitoring |
| Message Broker | Redis | Celery task queue |
| Metadata Store | PostgreSQL | Airflow internal state |
| KPI Metrics | Prometheus + Pushgateway | Batch metrics ingestion and storage |
| KPI Dashboards | Grafana | KPI dashboards and anomaly monitoring |
| Sensor Visualization | Streamlit | Interactive sensor data and time series dashboards |

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

    | Service | URL                              | Credentials |
    |---|----------------------------------|---|
    | Streamlit IoT Dashboard | http://localhost:8501            | Main system dashboard and sensor visualization |
    | Airflow | http://localhost:8080/auth/login | airflow / airflow |
    | Prometheus | http://localhost:9090            | — |
    | Pushgateway | http://localhost:9091            | — |
    | Grafana | http://localhost:3000            | admin / admin |

## How it Works

### 1. Data Ingestion (`sensor_batch_processing` DAG)
Reads the CSV in parallel chunks, cleans it, and inserts rows into MongoDB `sensor_readings` (a Time Series collection).

Cleaning steps:
- Removes duplicates
- Handles missing values
- Coerces data types (numeric, boolean)
- Parses epoch timestamps to `datetime`

### 2. KPI Reporting
Runs periodically (e.g., daily or hourly). Aggregates the previous time window's data from MongoDB per device and pushes both static bounds and statistical variance metrics to the Prometheus Pushgateway.

All metrics are labeled by `device` and `day`.

### 3. Monitoring and Alerting

Prometheus continuously scrapes metrics from the Pushgateway.

Prometheus alert rules monitor:

- Sensor variance
- Device activity
- Environmental anomalies

Triggered alerts can then be visualized in **Grafana dashboards**.

### 4. Visualization Layer

The system uses **two complementary visualization layers**:

#### Streamlit Dashboard

Used for:

- Sensor time series exploration
- Device-level data analysis
- Operational entry point to system services

#### Grafana

Used for:

- KPI dashboards
- Aggregated monitoring views
- Anomaly detection visualizations

## File Structure
*   `docker-compose.yml` — full infrastructure definition
*   `scripts/batch_loader/` — CSV reading and MongoDB insertion
*   `scripts/data_cleaner/` — data transformation and validation
*   `scripts/kpi_aggregator/` — MongoDB aggregation + Prometheus push
*   `airflow/dags/sensor_batch_dag.py` — ingestion DAG
*   `airflow/dags/kpi_dag.py` — KPI reporting DAG
*   `prometheus/prometheus.yml` — Prometheus scrape config
*   `grafana/provisioning/` — auto-provisioned datasource and dashboard
  * `streamlit/` — main system dashboard and sensor visualization
