import pandas as pd
from producer import RawDataProducer
import time
import signal

BROKERS = ["broker1:9092", "broker2:9092"]
TOPIC = "raw-data"

running = True

def shutdown_handler(signum, frame):
    global running
    print("Shutdown signal received. Stopping producer...")
    running = False

def main():
    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    producer = RawDataProducer(BROKERS)

    print("Loading dataset...")
    df = pd.read_parquet("/app/data/stream_raw.parquet")

    print("Starting streaming simulation...")

    for _, row in df.iterrows():
        if not running:
            break

        producer.send(TOPIC, row.to_dict())
        print(f"User {row['user']} on machine {row['machine_id']} sent data to topic {TOPIC}")
        time.sleep(0.1)  # simulate streaming

    print("Streaming finished or stopped.")
    producer.producer.flush()
    producer.producer.close()
    print("Producer closed cleanly.")

if __name__ == "__main__":
    main()