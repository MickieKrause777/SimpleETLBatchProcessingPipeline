# Environmental Sensor Data System

## Overview
This project implements a scalable data pipeline for processing environmental sensor data. It uses **MongoDB (Time Series Collections)** for storage, **Apache Airflow** for orchestration, and **Docker** for containerization.

## System Architecture
*   **Database**: MongoDB 7.0 (Time Series)
*   **Orchestration**: Apache Airflow (running with CeleryExecutor)
*   **Message Broker**: Redis (for Celery)
*   **Metadata Store**: PostgreSQL (for Airflow)
*   **Data Processing**: Python scripts (`batch_loader` and `data_cleaner`)

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
    This command downloads the necessary images and starts MongoDB, PostgreSQL, Redis, Airflow Webserver, Scheduler, and Workers.

3.  **Access the Airflow UI**:
    *   Open your browser and navigate to `http://localhost:8080`.
    *   Default credentials: `airflow` / `airflow`.

## How it Works
1.  **Data Ingestion**: The system reads CSV files containing sensor data (CO, humidity, temperature, etc.).
2.  **Data Cleaning**: The `DataCleaner` module performs quality checks:
    *   Removes duplicates.
    *   Handles missing values.
    *   Coerces data types (numeric, boolean).
    *   Parses timestamps.
3.  **Storage**: Cleaned data is batched and inserted into MongoDB. The collection is configured as a **Time Series Collection** for optimized storage and querying of time-based data.
4.  **Automation**: Airflow DAGs (Directed Acyclic Graphs) schedule and monitor these tasks, ensuring reliable data processing.

## File Structure
*   `docker-compose.yml`: Defines the infrastructure services.
*   `scripts/`: Contains the Python logic for loading and cleaning data.
    *   `batch_loader/`: Handles reading CSVs and inserting into MongoDB.
    *   `data_cleaner/`: Implements data transformation logic.
*   `airflow/`: configuration for the Airflow instance.
