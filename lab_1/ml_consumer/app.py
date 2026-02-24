from consumer import MLConsumer
import logging

BROKERS = ["broker1:9092", "broker2:9093"]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting ML Consumer service...")

    ml_service = MLConsumer(BROKERS)
    ml_service.run()