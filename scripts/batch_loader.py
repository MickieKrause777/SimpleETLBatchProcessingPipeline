import logging
import os
from data_cleaner import DataCleaner
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

    def create_indexes(self):
        self.collection.create_index('device')
        self.collection.create_index('ts')
        self.collection.create_index([('device', 1), ('ts', -1)])
        logger.info("Created indexes on 'device', 'ts', and compound index")

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

    def load_full_csv(self, filepath: str, batch_id: str = None) -> dict:
        batch_id = batch_id or f"full_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        logger.info(f"Loading full CSV: {filepath}")

        # Read entire file
        df = pd.read_csv(filepath)
        total_rows = len(df)

        logger.info(f"Read {total_rows} rows from CSV")

        df = self.cleaner.cleanse(df)

        df['_metadata'] = df.apply(lambda _: {
            'ingested_at': datetime.now(),
            'batch_id': batch_id
        }, axis=1)

        records = df.to_dict('records')
        inserted_count = 0

        for i in range(0, len(records), self.batch_size):
            batch = records[i:i + self.batch_size]
            try:
                result = self.collection.insert_many(batch, ordered=False)
                inserted_count += len(result.inserted_ids)
                logger.info(f"Inserted batch {i // self.batch_size + 1}: {len(batch)} documents")
            except BulkWriteError as e:
                inserted_count += e.details.get('nInserted', 0)
                logger.warning(f"Bulk write warning: {e.details.get('writeErrors', [])[:3]}")

        stats = {
            'total_rows_read': total_rows,
            'rows_after_cleansing': len(df),
            'rows_inserted': inserted_count,
            'cleansing_stats': self.cleaner.stats.copy(),
            'batch_id': batch_id
        }

        logger.info(f"Full load complete: {stats}")
        return stats

    def close(self):
        """Close MongoDB connection."""
        self.client.close()
        logger.info("MongoDB connection closed")


def get_csv_row_count(filepath: str) -> int:
    with open(filepath, 'r') as f:
        return sum(1 for _ in f) - 1  # Subtract header


if __name__ == '__main__':
    # Example usage for testing
    import sys

    print(sys.argv)
    if len(sys.argv) < 2:
        print("Usage: python batch_loader.py <csv_filepath>")
        sys.exit(1)

    filepath = sys.argv[1]

    loader = BatchLoader()
    loader.create_indexes()
    stats = loader.load_full_csv(filepath)
    loader.close()

    print(f"\nFinal Statistics: {stats}")