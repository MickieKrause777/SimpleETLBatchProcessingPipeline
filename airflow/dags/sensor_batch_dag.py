import os
from datetime import datetime, timedelta
from airflow.sdk import dag, task, task_group
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from scripts.batch_loader import BatchLoader, get_csv_row_count

CSV_FILE_PATH = '/opt/airflow/data/iot_telemetry_data.csv'
NUM_WORKERS = 2
BATCH_SIZE = 1000

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
}

@dag(
    dag_id="sensor_batch_processing",
    default_args=default_args,
    catchup=False
)
def sensor_batch_processing():
    @task.python
    def setup_database(**context):
        loader = BatchLoader(batch_size=BATCH_SIZE)
        loader.setup_collection()
        loader.close()
        return "Database setup complete"

    @task.python
    def get_chunk_boundaries(**context):
        if not os.path.exists(CSV_FILE_PATH):
            raise FileNotFoundError(f"CSV file not found: {CSV_FILE_PATH}")

        total_rows = get_csv_row_count(CSV_FILE_PATH)
        chunk_size = total_rows // NUM_WORKERS
        remainder = total_rows % NUM_WORKERS

        chunks = []
        start = 0

        for i in range(NUM_WORKERS):
            extra = 1 if i < remainder else 0
            end = start + chunk_size + extra
            chunks.append({
                'chunk_id': i,
                'start_row': start,
                'end_row': end,
                'worker': f'worker_{i + 1}'
            })
            start = end

        context['ti'].xcom_push(key='total_rows', value=total_rows)
        context['ti'].xcom_push(key='chunks', value=chunks)

        print(f"Total rows: {total_rows}")
        print(f"Chunks: {chunks}")

    @task.python
    def process_chunk(chunk_id: int, **context):
        ti = context['ti']
        chunks = ti.xcom_pull(task_ids='get_chunk_boundaries', key='chunks')

        if chunks is None:
            raise ValueError("No chunk information found in XCom")

        chunk = chunks[chunk_id]
        start_row = chunk['start_row']
        end_row = chunk['end_row']

        print(f"Processing chunk {chunk_id}: rows {start_row} to {end_row}")

        run_id = context['run_id']
        batch_id = f"chunk_{chunk_id}_{run_id}"

        loader = BatchLoader(batch_size=BATCH_SIZE)
        stats = loader.load_csv_chunk(
            filepath=CSV_FILE_PATH,
            start_row=start_row,
            end_row=end_row,
            batch_id=batch_id
        )
        loader.close()

        ti.xcom_push(key=f'chunk_{chunk_id}_stats', value=stats)

    @task_group(group_id="process_chunks")
    def process_chunks_group():
        for i in range(NUM_WORKERS):
            process_chunk(
                chunk_id=i,
                task_id=f"process_chunk_{i}",
                queue="default",
            )

    @task.python
    def aggregate_results(**context):
        ti = context['ti']

        total_stats = {
            'total_rows_read': 0,
            'total_rows_inserted': 0,
            "total_rows_skipped": 0,
            'chunks_processed': 0,
            'cleansing_stats': {
                'duplicates_removed': 0,
                'missing_values_dropped': 0,
                'type_errors_fixed': 0,
            }
        }

        for i in range(NUM_WORKERS):
            stats = ti.xcom_pull(task_ids=f'process_chunks.process_chunk{f"__{i}" if i > 0 else ""}', key=f'chunk_{i}_stats')
            if stats:
                total_stats['total_rows_read'] += stats.get('rows_read', 0)
                total_stats['total_rows_inserted'] += stats.get('rows_inserted', 0)
                total_stats['chunks_processed'] += 1

                cleansing = stats.get('cleansing_stats', {})
                for key in total_stats['cleansing_stats']:
                    total_stats['cleansing_stats'][key] += cleansing.get(key, 0)

        print(f"\n{'=' * 50}")
        print("FINAL AGGREGATED STATISTICS")
        print(f"{'=' * 50}")
        print(f"Total rows read: {total_stats['total_rows_read']}")
        print(f"Total rows inserted: {total_stats['total_rows_inserted']}")
        print(f"Chunks processed: {total_stats['chunks_processed']}")
        print(f"Cleansing stats: {total_stats['cleansing_stats']}")
        print(f"{'=' * 50}\n")

        return total_stats

    # @task.python
    # def move_processed_file(**context):
    #     BatchLoader.move_to_processed(CSV_FILE_PATH)
    #     return f"Moved {CSV_FILE_PATH} to Processed/"

    trigger_sensor_kpi = TriggerDagRunOperator(
        task_id="trigger_sensor_kpi_reporting",
        trigger_dag_id="sensor_kpi_reporting",
        wait_for_completion=True,
        reset_dag_run=True
    )

    calculate_chunks = get_chunk_boundaries()
    setup_db = setup_database()
    process_group = process_chunks_group()
    aggregate = aggregate_results()
    # move_file = move_processed_file()

    [calculate_chunks, setup_db] >> process_group >> aggregate >> trigger_sensor_kpi

sensor_batch_processing()