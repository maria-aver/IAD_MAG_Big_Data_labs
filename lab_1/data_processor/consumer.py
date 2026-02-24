import json
import time
import random
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from preprocessing.preprocessing import process_df


class DataProcessor:
    def __init__(self, brokers):
        self.brokers = brokers
        self.retry_interval = random.uniform(0.1, 0.5)

        # Load medians once
        with open("/app/artifacts/medians.json", "r") as f:
            self.medians = json.load(f)

        self.consumer = self._connect_consumer()
        self.producer = self._connect_producer()

    def _connect_consumer(self):
        while True:
            try:
                consumer = KafkaConsumer(
                    "raw-data",
                    bootstrap_servers=self.brokers,
                    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                    group_id="data-processor-group",
                    auto_offset_reset="earliest",
                )
                print("Connected to Kafka consumer.")
                return consumer
            except NoBrokersAvailable:
                print(f"Kafka consumer not ready. Retrying in {self.retry_interval}s...")
                time.sleep(self.retry_interval)

    def _connect_producer(self):
        while True:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.brokers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    retries=5,
                )
                print("Connected to Kafka producer.")
                return producer
            except NoBrokersAvailable:
                print(f"Kafka producer not ready. Retrying in {self.retry_interval}s...")
                time.sleep(self.retry_interval)

    def run(self):
        while True:
            try:
                for msg in self.consumer:
                    raw = msg.value
                    df = pd.DataFrame([raw])

                    processed, id_info = process_df(df, self.medians, train=False)

                    self.producer.send(
                        "processed-data",
                        {
                            "features": processed.to_dict(orient="records")[0],
                            "id_info": id_info,
                        },
                    )
                    print(f"Processed message from machine {id_info.get('machine_id')} in cluster {id_info.get('cluster')} sent to processed-data")

            except Exception as e:
                print(f"Processing error: {e}")
                print("Reconnecting to Kafka...")
                time.sleep(self.retry_interval)
                self.consumer = self._connect_consumer()
                self.producer = self._connect_producer()