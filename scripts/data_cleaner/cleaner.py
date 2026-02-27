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
            'type_errors_fixed': 0,
            'anomalies_flagged': 0,
        }

    def cleanse(self, df: pd.DataFrame) -> pd.DataFrame:
        original_count = len(df)

        df = self._remove_duplicates(df)

        df = self._handle_missing_values(df)

        df = self._coerce_types(df)

        df = self._parse_timestamps(df)

        df = self._detect_anomalies(df)

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

    def _coerce_types(self, df: pd.DataFrame) -> pd.DataFrame:
        numeric_cols = ['temp', 'humidity', 'co', 'lpg', 'smoke']

        for col in numeric_cols:
            if col in df.columns:
                before_nulls = df[col].isna().sum()
                df[col] = pd.to_numeric(df[col], errors='coerce')
                after_nulls = df[col].isna().sum()
                self.stats['type_errors_fixed'] += int(after_nulls - before_nulls)

        bool_cols = ['light', 'motion']
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].astype(bool)

        return df

    def _parse_timestamps(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'ts' in df.columns:
            try:
                # Try epoch format first (numeric)
                df['ts'] = pd.to_datetime(df['ts'], unit='s', errors='coerce')
            except:
                df['ts'] = pd.to_datetime(df['ts'], errors='coerce')

        return df

    def _detect_anomalies(self, df: pd.DataFrame) -> pd.DataFrame:
        thresholds = {
            'temp': (0, 31.0),      # Typical indoor/environmental range in Celsius
            'humidity': (0.0, 100.0),   # Physical limits
            'co': (None, 0.02),         # High CO (ppm)
            'lpg': (None, 0.02),        # High LPG (ppm)
            'smoke': (None, 0.04)       # High Smoke (ppm)
        }

        anomaly_masks = {}

        for col, (min_val, max_val) in thresholds.items():
            if col not in df.columns:
                continue

            col_mask = pd.Series(False, index=df.index)
            if min_val is not None:
                col_mask = col_mask | (df[col] < min_val)
            if max_val is not None:
                col_mask = col_mask | (df[col] > max_val)

            anomaly_masks[col] = col_mask

        if anomaly_masks:
            mask_df = pd.DataFrame(anomaly_masks)
            # Find which columns violated thresholds for each row
            df['anomalies'] = mask_df.apply(lambda row: row.index[row].tolist(), axis=1)
            df['anomaly_count'] = mask_df.sum(axis=1)
        else:
            df['anomalies'] = [[] for _ in range(len(df))]
            df['anomaly_count'] = 0

        self.stats['anomalies_flagged'] += int((df['anomaly_count'] > 0).sum())
        return df
