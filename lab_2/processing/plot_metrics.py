from pathlib import Path
import json
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent.parent
RESULTS_DIR = BASE_DIR / "results" if (BASE_DIR / "results").exists() else BASE_DIR

# === загрузка метрик ===
files = {
    "1 Node": "metrics_1_node_spark.json",
    "1 Node Opt": "metrics_1_node_spark_opt.json",
    "3 Node": "metrics_3_node_spark.json",
    "3 Node Opt": "metrics_3_node_spark_opt.json",
}

labels = []
execution_time = []
ram_usage = []
partitions = []

for label, filename in files.items():
    path = RESULTS_DIR / filename

    with open(path) as f:
        data = json.load(f)

    labels.append(label)
    execution_time.append(data["execution_time_sec"])
    ram_usage.append(data["ram_usage_mb"])
    partitions.append(data["partitions"])

# === 1. Execution Time (с zoom) ===
plt.figure()
plt.bar(labels, execution_time)

# zoom по оси Y (чтобы увидеть разницу)
min_time = min(execution_time)
max_time = max(execution_time)
plt.ylim(min_time * 0.98, max_time * 1.02)

# подписи значений
for i, v in enumerate(execution_time):
    plt.text(i, v, f"{v:.3f}", ha='center', va='bottom')

plt.title("Execution Time Comparison (Zoomed)")
plt.xlabel("Experiment")
plt.ylabel("Time (sec)")
plt.xticks(rotation=20)
plt.tight_layout()
plt.savefig(RESULTS_DIR / "execution_time.png")
plt.close()

# === 2. RAM Usage (с zoom) ===
plt.figure()
plt.bar(labels, ram_usage)

min_ram = min(ram_usage)
max_ram = max(ram_usage)
plt.ylim(min_ram * 0.98, max_ram * 1.02)

for i, v in enumerate(ram_usage):
    plt.text(i, v, f"{v:.2f}", ha='center', va='bottom')

plt.title("RAM Usage Comparison (Zoomed)")
plt.xlabel("Experiment")
plt.ylabel("RAM (MB)")
plt.xticks(rotation=20)
plt.tight_layout()
plt.savefig(RESULTS_DIR / "ram_usage.png")
plt.close()

# === 3. Time vs RAM ===
plt.figure()
plt.scatter(execution_time, ram_usage)

for i, label in enumerate(labels):
    plt.annotate(label, (execution_time[i], ram_usage[i]))

plt.title("Execution Time vs RAM Usage")
plt.xlabel("Time (sec)")
plt.ylabel("RAM (MB)")
plt.tight_layout()
plt.savefig(RESULTS_DIR / "time_vs_ram.png")
plt.close()

# === 4. Relative Time (очень полезный график!) ===
baseline = execution_time[0]  # 1 Node Spark

relative_time = [t / baseline for t in execution_time]

plt.figure()
plt.bar(labels, relative_time)

for i, v in enumerate(relative_time):
    plt.text(i, v, f"{v:.3f}", ha='center', va='bottom')

plt.title("Relative Execution Time (baseline = 1 Node)")
plt.xlabel("Experiment")
plt.ylabel("Relative Time")
plt.xticks(rotation=20)
plt.tight_layout()
plt.savefig(RESULTS_DIR / "relative_time.png")
plt.close()

print("Plots saved to:", RESULTS_DIR)