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

    def run(self, target_date: datetime = None) -> list[dict]:
        if target_date is None:
            target_date = datetime.now(timezone.utc) - timedelta(days=1)

        day_start = target_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
        day_end = day_start + timedelta(days=1)
        day_label = day_start.strftime('%Y-%m-%d')

        logger.info(f"Computing KPIs for {day_label} ({day_start} → {day_end})")

        pipeline = [
            {
                '$match': {
                    'ts': {'$gte': day_start, '$lt': day_end}
                }
            },
            {
                '$group': {
                    '_id': '$device',
                    'avg_temp': {'$avg': '$temp'},
                    'avg_humidity': {'$avg': '$humidity'},
                    'avg_co': {'$avg': '$co'},
                    'avg_lpg': {'$avg': '$lpg'},
                    'avg_smoke': {'$avg': '$smoke'},
                    'motion_events': {'$sum': {'$cond': ['$motion', 1, 0]}},
                    'light_events': {'$sum': {'$cond': ['$light', 1, 0]}},
                    'reading_count': {'$sum': 1},
                }
            },
            {'$sort': {'_id': 1}},
        ]

        results = list(self.collection.aggregate(pipeline))
        logger.info(f"Aggregation returned {len(results)} device(s)")

        if not results:
            logger.warning(f"No data found for {day_label} — nothing pushed to Pushgateway")
            return []

        self._push_metrics(results, day_label)
        return results

    def _push_metrics(self, results: list[dict], day_label: str):
        registry = CollectorRegistry()
        label_names = ['device', 'day']

        metrics = {
            'sensor_avg_temp': Gauge('sensor_avg_temp', 'Average temperature (°F)', label_names, registry=registry),
            'sensor_avg_humidity': Gauge('sensor_avg_humidity', 'Average humidity (%)', label_names, registry=registry),
            'sensor_avg_co': Gauge('sensor_avg_co', 'Average CO level (ppm)', label_names, registry=registry),
            'sensor_avg_lpg': Gauge('sensor_avg_lpg', 'Average LPG level (ppm)', label_names, registry=registry),
            'sensor_avg_smoke': Gauge('sensor_avg_smoke', 'Average smoke level (ppm)', label_names, registry=registry),
            'sensor_motion_events': Gauge('sensor_motion_events', 'Total motion events', label_names, registry=registry),
            'sensor_light_events': Gauge('sensor_light_events', 'Total light events', label_names, registry=registry),
            'sensor_reading_count': Gauge('sensor_reading_count', 'Total readings ingested', label_names, registry=registry),
        }

        for row in results:
            device = row['_id']
            labels = {'device': device, 'day': day_label}

            metrics['sensor_avg_temp'].labels(**labels).set(row.get('avg_temp') or 0)
            metrics['sensor_avg_humidity'].labels(**labels).set(row.get('avg_humidity') or 0)
            metrics['sensor_avg_co'].labels(**labels).set(row.get('avg_co') or 0)
            metrics['sensor_avg_lpg'].labels(**labels).set(row.get('avg_lpg') or 0)
            metrics['sensor_avg_smoke'].labels(**labels).set(row.get('avg_smoke') or 0)
            metrics['sensor_motion_events'].labels(**labels).set(row.get('motion_events') or 0)
            metrics['sensor_light_events'].labels(**labels).set(row.get('light_events') or 0)
            metrics['sensor_reading_count'].labels(**labels).set(row.get('reading_count') or 0)

        push_to_gateway(self.pushgateway_url, job='sensor_kpi', registry=registry)
        logger.info(f"Pushed KPI metrics for {len(results)} device(s) to Pushgateway at {self.pushgateway_url}")

    def close(self):
        self.client.close()
        logger.info("KpiAggregator MongoDB connection closed")
