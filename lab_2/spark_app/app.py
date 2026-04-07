from pyspark.sql import SparkSession
from pyspark.sql.functions import avg
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

# # === OPTIMIZATION ===
# df = df.repartition(6)
# df.cache()

# # прогрев кеша
# df.count()

# === 2. Базовые метрики ===
count = df.count()
partitions = df.rdd.getNumPartitions()

print("Rows:", count)
print("Partitions:", partitions)

# === 3. Обработка ===

# Фильтрация
df_filtered = df.filter(df["Country"].isNotNull())

# --- 1. Количество разработчиков по странам ---
country_counts = df_filtered.groupBy("Country").count()
country_counts.show(10)

# --- 2. Средняя зарплата по странам ---
salary_stats = df_filtered.groupBy("Country") \
    .agg(avg("ConvertedCompYearly").alias("avg_salary"))

salary_stats.orderBy("avg_salary", ascending=False).show(10)

# --- 3. Распределение удалённой работы ---
remote_stats = df.groupBy("RemoteWork").count()
remote_stats.show()

# === 4. Сохранение результатов ===

country_counts.write.mode("overwrite") \
    .csv("hdfs://namenode:9000/results/country_counts")

salary_stats.write.mode("overwrite") \
    .csv("hdfs://namenode:9000/results/salary_stats")

remote_stats.write.mode("overwrite") \
    .csv("hdfs://namenode:9000/results/remote_stats")

# === 5. Логирование ===
logger.log({
    "rows": count,
    "partitions": partitions
})

spark.stop()