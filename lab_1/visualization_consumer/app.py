from consumer import VisualizationConsumer

BROKERS = ["broker1:9092", "broker2:9093"]

if __name__ == "__main__":
    consumer = VisualizationConsumer(BROKERS)
    consumer.run()