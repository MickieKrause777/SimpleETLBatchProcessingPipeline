import logging
import os
from datetime import datetime, timedelta, timezone

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway
from pymongo import MongoClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KpiAggregator:
    def __init__(
            self,
            mongo_uri: str = None,
            database: str = None,
            collection: str = 'sensor_readings',
            pushgateway_url: str = None,
    ):
        self.mongo_uri = mongo_uri or os.getenv('MONGO_URI', 'mongodb://localhost:27017/')
        self.database = database or os.getenv('MONGO_DB', 'sensor_data')
        self.collection_name = collection
        self.pushgateway_url = pushgateway_url or os.getenv('PUSHGATEWAY_URL', 'pushgateway:9091')

        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.database]
        self.collection = self.db[self.collection_name]
        logger.info(f"KpiAggregator connected to MongoDB: {self.mongo_uri}, DB: {self.database}")

    def run(self, start_time: datetime = None, end_time: datetime = None) -> list[dict]:
        if start_time is None and end_time is None:
            # Use fixed date because of the current Dataset
            target_date = datetime(2020, 7, 13).replace(tzinfo=timezone.utc) - timedelta(days=1)
            start_time = target_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
            end_time = start_time + timedelta(days=1)
        elif start_time is None or end_time is None:
            raise ValueError("Must provide both start_time and end_time, or neither")

        day_label = start_time.strftime('%Y-%m-%d')
        # Include hour if the interval is less than a day
        if (end_time - start_time) < timedelta(days=1):
             time_label = start_time.strftime('%Y-%m-%dT%H:00')
        else:
             time_label = day_label

        logger.info(f"Computing KPIs for {time_label} ({start_time} → {end_time})")

        pipeline = [
            {
                '$match': {
                    'ts': {'$gte': start_time, '$lt': end_time}
                }
            },
            {
                '$group': {
                    '_id': '$device',
                    'avg_temp': {'$avg': '$temp'},
                    'stddev_temp': {'$stdDevSamp': '$temp'},
                    'avg_humidity': {'$avg': '$humidity'},
                    'stddev_humidity': {'$stdDevSamp': '$humidity'},
                    'avg_co': {'$avg': '$co'},
                    'stddev_co': {'$stdDevSamp': '$co'},
                    'avg_lpg': {'$avg': '$lpg'},
                    'avg_smoke': {'$avg': '$smoke'},
                    'motion_events': {'$sum': {'$cond': ['$motion', 1, 0]}},
                    'light_events': {'$sum': {'$cond': ['$light', 1, 0]}},
                    'reading_count': {'$sum': 1},
                    'anomaly_total': {'$sum': '$anomaly_count'},
                    'anomalous_readings': {'$sum': {'$cond': [{'$gt': ['$anomaly_count', 0]}, 1, 0]}},
                }
            },
            {'$sort': {'_id': 1}},
        ]

        results = list(self.collection.aggregate(pipeline))
        logger.info(f"Aggregation returned {len(results)} device(s)")

        if not results:
            logger.warning(f"No data found for {time_label} — nothing pushed to Pushgateway")
            return []

        self._push_metrics(results, time_label)
        return results

    def _push_metrics(self, results: list[dict], time_label: str):
        registry = CollectorRegistry()
        label_names = ['device', 'time_window']

        metrics = {
            'sensor_avg_temp': Gauge('sensor_avg_temp', 'Average temperature (°F)', label_names, registry=registry),
            'sensor_stddev_temp': Gauge('sensor_stddev_temp', 'Standard Deviation of temperature', label_names, registry=registry),
            'sensor_avg_humidity': Gauge('sensor_avg_humidity', 'Average humidity (%)', label_names, registry=registry),
            'sensor_stddev_humidity': Gauge('sensor_stddev_humidity', 'Standard Deviation of humidity', label_names, registry=registry),
            'sensor_avg_co': Gauge('sensor_avg_co', 'Average CO level (ppm)', label_names, registry=registry),
            'sensor_stddev_co': Gauge('sensor_stddev_co', 'Standard Deviation of CO level', label_names, registry=registry),
            'sensor_avg_lpg': Gauge('sensor_avg_lpg', 'Average LPG level (ppm)', label_names, registry=registry),
            'sensor_avg_smoke': Gauge('sensor_avg_smoke', 'Average smoke level (ppm)', label_names, registry=registry),
            'sensor_motion_events': Gauge('sensor_motion_events', 'Total motion events', label_names, registry=registry),
            'sensor_light_events': Gauge('sensor_light_events', 'Total light events', label_names, registry=registry),
            'sensor_reading_count': Gauge('sensor_reading_count', 'Total readings ingested', label_names, registry=registry),
            'sensor_anomaly_total': Gauge('sensor_anomaly_total', 'Total anomaly flags', label_names, registry=registry),
            'sensor_anomaly_readings': Gauge('sensor_anomaly_readings', 'Total anomalous readings', label_names, registry=registry),
        }

        for row in results:
            device = row['_id']
            labels = {'device': device, 'time_window': time_label}

            metrics['sensor_avg_temp'].labels(**labels).set(row.get('avg_temp') or 0)
            metrics['sensor_stddev_temp'].labels(**labels).set(row.get('stddev_temp') or 0)
            metrics['sensor_avg_humidity'].labels(**labels).set(row.get('avg_humidity') or 0)
            metrics['sensor_stddev_humidity'].labels(**labels).set(row.get('stddev_humidity') or 0)
            metrics['sensor_avg_co'].labels(**labels).set(row.get('avg_co') or 0)
            metrics['sensor_stddev_co'].labels(**labels).set(row.get('stddev_co') or 0)
            metrics['sensor_avg_lpg'].labels(**labels).set(row.get('avg_lpg') or 0)
            metrics['sensor_avg_smoke'].labels(**labels).set(row.get('avg_smoke') or 0)
            metrics['sensor_motion_events'].labels(**labels).set(row.get('motion_events') or 0)
            metrics['sensor_light_events'].labels(**labels).set(row.get('light_events') or 0)
            metrics['sensor_reading_count'].labels(**labels).set(row.get('reading_count') or 0)
            metrics['sensor_anomaly_total'].labels(**labels).set(row.get('anomaly_total') or 0)
            metrics['sensor_anomaly_readings'].labels(**labels).set(row.get('anomalous_readings') or 0)

        push_to_gateway(self.pushgateway_url, job='sensor_kpi', registry=registry)
        logger.info(f"Pushed KPI metrics for {len(results)} device(s) to Pushgateway at {self.pushgateway_url}")

    def close(self):
        self.client.close()
        logger.info("KpiAggregator MongoDB connection closed")
