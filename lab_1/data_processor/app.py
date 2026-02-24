from consumer import DataProcessor

BROKERS = ["broker1:9092", "broker2:9093"]

def main():
    processor = DataProcessor(BROKERS)
    processor.run()

if __name__ == "__main__":
    main()