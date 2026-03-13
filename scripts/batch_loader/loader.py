import shutil
import logging
import os
from scripts.data_cleaner import DataCleaner
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
import pandas as pd
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BatchLoader:
    def __init__(
            self,
            mongo_uri: str = None,
            database: str = None,
            collection: str = 'sensor_readings',
            batch_size: int = 1000
    ):
        self.mongo_uri = mongo_uri or os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
        self.database = database or os.getenv('MONGO_DB', 'sensor_data')
        self.collection_name = collection
        self.batch_size = batch_size
        self.cleaner = DataCleaner()

        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.database]
        self.collection = self.db[self.collection_name]
        logger.info(f"Connected to MongoDB: {self.mongo_uri}, DB: {self.database}")

    def setup_collection(self):
        existing_collections = self.db.list_collection_names()

        if self.collection_name not in existing_collections:
            self.db.create_collection(self.collection_name, timeseries={
                "timeField": "ts",
                "metaField": "device",
                "granularity":"seconds"
            })
            logger.info(f"Created collection: {self.collection_name}")
        else:
            logger.info(f"Collection {self.collection_name} already exists, skipped creation")

        self.collection = self.db[self.collection_name]

        logger.info("Ensured indexes on time series collection")

    def load_csv_chunk(
            self,
            filepath: str,
            start_row: int,
            end_row: int,
            batch_id: str
    ) -> dict:
        logger.info(f"Loading chunk: rows {start_row} to {end_row}")

        nrows = end_row - start_row
        df = pd.read_csv(filepath, skiprows=range(1, start_row + 1), nrows=nrows)

        logger.info(f"Read {len(df)} rows from CSV")

        df = self.cleaner.cleanse(df)

        df['_metadata'] = df.apply(lambda _: {
            'ingested_at': datetime.now(),
            'batch_id': batch_id,
            'source_rows': f'{start_row}-{end_row}'
        }, axis=1)

        alerts_df = df[df['anomaly_count'] > 0]
        if not alerts_df.empty:
            alerts_records = alerts_df.apply(lambda row: {
                'device': row['device'],
                'ts': row['ts'],
                'anomalies': row['anomalies'],
                'anomaly_count': row['anomaly_count'],
                'sensor_malfunction': row.get('sensor_malfunction', False),
                'hvac_waste': row.get('hvac_waste', False),
                'ventilation_ineff': row.get('ventilation_ineff', False),
                'batch_id': batch_id
            }, axis=1).tolist()
            self.db['sensor_alerts'].insert_many(alerts_records)

        records = df.to_dict('records')
        inserted_count = 0

        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            try:
                result = self.collection.insert_many(batch, ordered=False)
                inserted_count += len(result.inserted_ids)
            except BulkWriteError as e:
                inserted_count += e.details.get('nInserted', 0)
                logger.warning(f"Bulk write warning: {e.details.get('writeErrors', [])[:3]}")

        stats = {
            'rows_read': nrows,
            'rows_after_cleansing': len(df),
            'rows_inserted': inserted_count,
            'cleansing_stats': self.cleaner.stats.copy(),
            'batch_id': batch_id
        }

        logger.info(f"Batch complete: {stats}")
        return stats

    def close(self):
        """Close MongoDB connection."""
        self.client.close()
        logger.info("MongoDB connection closed")

    def move_to_processed(filepath: str) -> str:
        src = os.path.abspath(filepath)
        processed_dir = os.path.join(os.path.dirname(src), 'Processed')
        os.makedirs(processed_dir, exist_ok=True)

        dest = os.path.join(processed_dir, os.path.basename(src))
        shutil.move(src, dest)
        logger.info(f"Moved processed file to {dest}")
        return dest

def get_csv_row_count(filepath: str) -> int:
    with open(filepath, 'r') as f:
        return sum(1 for _ in f) - 1  # Subtract header
