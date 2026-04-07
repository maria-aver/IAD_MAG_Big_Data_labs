from pyspark.sql import SparkSession
from utils import MetricsLogger

logger = MetricsLogger()

spark = SparkSession.builder \
    .appName("SparkLab") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.driver.memory", "1g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

# Убираем мусорные логи
spark.sparkContext.setLogLevel("ERROR")

# === 1. Чтение ===
df = spark.read.csv(
    "hdfs://namenode:9000/data/stackoverflow_100k.csv",
    header=True,
    inferSchema=True
)

print("Schema:")
df.printSchema()

# === 2. Базовые метрики ===
count = df.count()
partitions = df.rdd.getNumPartitions()

print("Rows:", count)
print("Partitions:", partitions)

# === 3. Обработка (сделай НЕ тривиально) ===

# пример: фильтрация + агрегация
df_filtered = df.filter(df["Country"].isNotNull())

result = df_filtered.groupBy("Country").count()

result.show(10)

# === 4. Запись результата ===
result.write.mode("overwrite").csv("hdfs://namenode:9000/results/basic")

# === 5. Логирование ===
logger.log({
    "rows": count,
    "partitions": partitions
})

spark.stop()