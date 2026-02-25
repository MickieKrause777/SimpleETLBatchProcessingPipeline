import os
from datetime import datetime, timedelta, timezone
from airflow.sdk import dag, task
from scripts.kpi_aggregator import KpiAggregator

PUSHGATEWAY_URL = os.getenv('PUSHGATEWAY_URL', 'pushgateway:9091')

default_args = {
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='sensor_kpi_reporting',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['kpi', 'prometheus'],
)
def sensor_kpi_reporting():
    @task.python
    def compute_kpis(**context) -> list:
        # Use fixed date because of the current Dataset
        execution_date: datetime = datetime(2020, 7, 13)
        # Aggregate for the day *before* the execution date (yesterday relative to DAG run)
        target_date = execution_date.replace(tzinfo=timezone.utc) - timedelta(days=1)

        aggregator = KpiAggregator(pushgateway_url=PUSHGATEWAY_URL)
        try:
            results = aggregator.run(target_date=target_date)
        finally:
            aggregator.close()

        return results

    @task.python
    def log_summary(kpi_results: list):
        if not kpi_results:
            print("No KPI results to display.")
            return

        sep = '=' * 70
        print(f"\n{sep}")
        print(f"{'SENSOR KPI SUMMARY':^70}")
        print(sep)
        header = f"{'Device':<25} {'Temp°F':>7} {'Hum%':>7} {'CO':>8} {'LPG':>8} {'Smoke':>8} {'Motion':>7} {'Light':>7} {'Reads':>7}"
        print(header)
        print('-' * 70)
        for row in kpi_results:
            print(
                f"{row['_id']:<25}"
                f"{row.get('avg_temp', 0):>7.2f}"
                f"{row.get('avg_humidity', 0):>7.2f}"
                f"{row.get('avg_co', 0):>8.5f}"
                f"{row.get('avg_lpg', 0):>8.5f}"
                f"{row.get('avg_smoke', 0):>8.5f}"
                f"{row.get('motion_events', 0):>7}"
                f"{row.get('light_events', 0):>7}"
                f"{row.get('reading_count', 0):>7}"
            )
        print(sep)
        print(f"Metrics pushed to Prometheus Pushgateway ✓")
        print(f"{sep}\n")

    kpi_results = compute_kpis()
    log_summary(kpi_results)


sensor_kpi_reporting()
