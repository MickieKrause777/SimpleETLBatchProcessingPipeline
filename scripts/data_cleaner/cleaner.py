import pandas as pd
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DataCleaner:
    def __init__(self):
        self.stats = {
            'duplicates_removed': 0,
            'missing_values_dropped': 0,
        }

    def cleanse(self, df: pd.DataFrame) -> pd.DataFrame:
        original_count = len(df)

        df = self._remove_duplicates(df)

        df = self._handle_missing_values(df)

        logger.info(f"Cleansing complete: {original_count} -> {len(df)} rows")
        logger.info(f"Stats: {self.stats}")

        return df

    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates(subset=['ts', 'device'], keep='first')
        self.stats['duplicates_removed'] += before - len(df)
        return df

    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.dropna(subset=['ts', 'device'])
        self.stats['missing_values_dropped'] += before - len(df)
        return df