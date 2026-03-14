# Environmental Sensor Data System

## Overview
This project implements a scalable data pipeline for processing environmental sensor data. It uses **MongoDB (Time Series Collections)** for storage, **Apache Airflow** for orchestration, **Prometheus + Pushgateway** for KPI metrics, and **Grafana** and **Streamlit with Plotly** for visualization — all containerized with Docker.
The platform now supports **generating anomaly reports as PDF documents for arbitrary time windows**, allowing operational staff to review incidents and environmental irregularities for selected dates.
Future iterations of the system could possibly support **automated report distribution (e.g., via email), predictive anomaly detection models or static site generation (SSG)**.

## Example Use Case
A large **hotel chain** operates hundreds of rooms across multiple locations, each equipped with environmental sensors monitoring:

- Temperature
- Humidity
- CO concentration
- Motion (occupancy)
- Light levels

The system continuously ingests sensor data from all rooms and detects anomalies such as:

- malfunctioning sensors  
- inefficient HVAC usage  
- ventilation inefficiencies  
- unusual environmental fluctuations

Because the system uses **Apache Airflow with the CeleryExecutor**, ingestion and analysis tasks can run in parallel across multiple workers. This allows the system to scale horizontally as the number of monitored rooms increases.

Operational staff can then use the **Streamlit dashboard** to:

- inspect sensor time series
- view detected anomalies
- generate **PDF reports for specific time periods**
- go to the Grafana Dashboards for Daily KPIs

For example:

> A facility manager responsible for several hotel floors can generate a **daily anomaly report** and quickly identify rooms where ventilation systems are inefficient or sensors may require maintenance.

This enables **centralized monitoring with minimal personnel**, even for large distributed sensor networks.

# Early Identification System

The platform includes an early anomaly identification system to proactively detect hardware malfunctions and operational inefficiencies.
* **Micro-Batching & Parallel Processing:**  
  Data ingestion is executed in parallel chunks using Airflow and Celery workers, enabling scalable processing of large sensor datasets.

* **Statistical Variance Analysis:**  
  Sensor variance metrics (e.g., temperature, humidity, CO) are calculated to detect abnormal fluctuations or frozen sensor signals.

* **MongoDB-Based Anomaly Storage:**  
  Instead of external alert systems, anomalies are written directly into the **`sensor_alerts` collection** during ingestion.  
  Each anomaly record contains:

  - device ID
  - timestamp
  - anomaly types
  - anomaly count
  - contextual flags (sensor malfunction, HVAC inefficiency, etc.)
  - batch identifier

  This design allows flexible querying and report generation.

* **Time Series Visualizations:**  
  Streamlit dashboard panels visualize metrics over time using Plotly, allowing operators to rapidly spot correlations and trends.

## System Architecture
| Component            | Technology                        | Purpose                                                    |
|----------------------|-----------------------------------|------------------------------------------------------------|
| Database             | MongoDB 7.0 (Time Series)         | Raw sensor readings storage                                |
| Anomaly Storage      | MongoDB (`sensor_alerts`)         | Detected anomalies and operational alerts                  |
| Error Storage        | MongoDB (`batch_errors`)          | Store errors happening during batch ingestion for recovery |
| Orchestration        | Apache Airflow (CeleryExecutor)   | DAG scheduling and monitoring                              |
| Message Broker       | Redis                             | Celery task queue                                          |
| Metadata Store       | PostgreSQL                        | Airflow internal state                                     |
| KPI Metrics          | Prometheus + Pushgateway          | Batch metrics ingestion and storage                        |
| KPI Dashboards       | Grafana                           | KPI dashboards and anomaly monitoring                      |
| Sensor Visualization | Streamlit                         | Interactive sensor data and time series dashboards         |
| Reporting            | Streamlit / Python PDF generation | Manual anomaly report creation                             |

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
- Validate sensor ranges

### 2. Anomaly Extraction
During batch ingestion:
1. Each record is evaluated for anomalies.
2. Records with `anomaly_count > 0` are extracted.
3. These records are inserted into the `sensor_alerts` collection.

### 3. KPI Reporting
Runs periodically (e.g., daily or hourly). Aggregates the previous time window's data from MongoDB per device and pushes both static bounds and statistical variance metrics to the Prometheus Pushgateway.

All metrics are labeled by `device` and `day`.

### 4. Visualization Layer

The system uses **two complementary visualization layers**:

#### Streamlit Dashboard

Used for:

- Sensor time series exploration
- Device-level data analysis
- Operational entry point to system services
- Anomalies Inspection (Monitoring)
- Generation of PDF reports for selected time windows

#### Grafana

Used for:

- KPI dashboards
- Aggregated monitoring views

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
