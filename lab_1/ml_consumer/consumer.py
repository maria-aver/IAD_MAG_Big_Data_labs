import json
import time
import logging
import pandas as pd
import joblib

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable


class MLConsumer:
    def __init__(self, brokers, retry_interval=5):
        logging.basicConfig(level=logging.INFO)

        self.brokers = brokers
        self.retry_interval = retry_interval

        logging.info("Loading model...")
        self.model = joblib.load("/app/model/random_forest.pkl")

        self.consumer = self._connect_consumer()
        self.producer = self._connect_producer()

        logging.info("ML Consumer initialized.")

    def _connect_consumer(self):
        while True:
            try:
                consumer = KafkaConsumer(
                    "processed-data",
                    bootstrap_servers=self.brokers,
                    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                    group_id="ml-group",
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
        logging.info("ML Consumer started...")

        while True:
            try:
                for msg in self.consumer:
                    payload = msg.value

                    data = payload["features"]
                    id_info = payload["id_info"]

                    df = pd.DataFrame([data])

                    prob = float(self.model.predict_proba(df)[0][1])
                    prediction = int(prob >= 0.5)

                    result = {
                        "prediction": prediction,
                        "probability": prob,
                        "original_data": data,
                        "id_info": id_info,
                    }

                    self.producer.send("ml-results", result)

                    logging.info(
                        f"Prediction sent: {prediction} (prob={prob:.4f})"
                    )

            except Exception as e:
                logging.error(f"Kafka or processing error: {e}")
                logging.info("Reconnecting to Kafka...")
                time.sleep(self.retry_interval)
                self.consumer = self._connect_consumer()
                self.producer = self._connect_producer()