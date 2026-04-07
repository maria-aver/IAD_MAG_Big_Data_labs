import time
import psutil
import os
import json

class MetricsLogger:
    def __init__(self, log_file="/logs/metrics.json"):
        self.start_time = time.time()
        self.process = psutil.Process(os.getpid())
        self.log_file = log_file

    def log(self, extra=None):
        end_time = time.time()

        data = {
            "execution_time_sec": end_time - self.start_time,
            "ram_usage_mb": self.process.memory_info().rss / 1024 / 1024
        }

        if extra:
            data.update(extra)

        with open(self.log_file, "w") as f:
            json.dump(data, f, indent=4)

        print("METRICS:", data)