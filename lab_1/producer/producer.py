from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time

class RawDataProducer:
    def __init__(self, brokers, retry_interval=5):
        self.brokers = brokers
        self.retry_interval = retry_interval
        self.producer = self._connect()

    def _connect(self):
        while True:
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.brokers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks="all",
                    retries=5
                )
                print("Connected to Kafka brokers.")
                return producer
            except NoBrokersAvailable:
                print(f"Kafka not ready. Retrying in {self.retry_interval} seconds...")
                time.sleep(self.retry_interval)

    def send(self, topic, message):
        try:
            self.producer.send(topic, message)
            self.producer.flush()
        except Exception as e:
            print(f"Send failed: {e}. Reconnecting...")
            self.producer = self._connect()
            self.producer.send(topic, message)
            self.producer.flush()