import json
import time
import logging

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable


class VisualizationConsumer:
    def __init__(self, brokers, retry_interval=5):
        logging.basicConfig(level=logging.INFO)

        self.brokers = brokers
        self.retry_interval = retry_interval

        self.consumer = self._connect_consumer()
        self.producer = self._connect_producer()

        logging.info("Visualization Consumer initialized.")

    def _connect_consumer(self):
        while True:
            try:
                consumer = KafkaConsumer(
                    "ml-results",
                    bootstrap_servers=self.brokers,
                    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                    group_id="visualization-group",
                    auto_offset_reset="earliest",
                    enable_auto_commit=True,
                )
                logging.info("Connected to Kafka consumer.")
                return consumer
            except NoBrokersAvailable:
                logging.warning(
                    f"Kafka consumer not ready. Retrying in {self.retry_interval}s..."
                )
                time.sleep(self.retry_interval)

    def _connect_producer(self):
        while True:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.brokers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks="all",
                    retries=5,
                )
                logging.info("Connected to Kafka producer.")
                return producer
            except NoBrokersAvailable:
                logging.warning(
                    f"Kafka producer not ready. Retrying in {self.retry_interval}s..."
                )
                time.sleep(self.retry_interval)

    def run(self):
        logging.info("Visualization Consumer started...")

        while True:
            try:
                for msg in self.consumer:
                    payload = msg.value

                    pred = payload["prediction"]
                    prob = payload["probability"]
                    data = payload["original_data"]
                    id_info = payload["id_info"]

                    dashboard_payload = {
                        "time": data.get("time"),
                        "cluster": id_info.get("cluster"),
                        "duration": data.get("duration"),
                        "page_cache_memory": data.get("page_cache_memory"),
                        "cpu_max": data.get("cpu_max"),
                        "cycles_per_instruction": data.get("cycles_per_instruction"),
                        "assigned_memory": data.get("assigned_memory"),
                        "failed": pred,
                        "fail_probability": prob,
                    }

                    self.producer.send("visualization", dashboard_payload)

                    logging.info(f"Sent data with failed tag {dashboard_payload['failed']} in cluster {dashboard_payload['cluster']} to visualization topic.")

            except Exception as e:
                logging.error(f"Kafka or processing error: {e}")
                logging.info("Reconnecting to Kafka...")
                time.sleep(self.retry_interval)
                self.consumer = self._connect_consumer()
                self.producer = self._connect_producer()