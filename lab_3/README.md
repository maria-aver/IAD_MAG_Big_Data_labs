# Лабораторная работа 3. Lakehouse на Polars + Delta Lake

## Описание проекта

В рамках лабораторной работы реализован lakehouse pipeline для анализа и прогнозирования задержек авиарейсов на датасете US Flight Delays.

Архитектура проекта построена по слоям:

- Bronze — сырые данные
- Silver — очищенные и подготовленные данные
- Gold — аналитические витрины и feature table для ML

Используемые технологии:

- Python
- Polars Lazy API
- Delta Lake (delta-rs)
- MLflow
- Docker Compose

---

## Структура проекта

```text
src/
├── 00_split_raw_data.py
├── 01_bronze.py
├── 02_silver.py
├── 03_gold.py
├── 04_delta_ops.py
├── 05_train_ml.py
├── config.py
```

---

## Назначение файлов

### 00_split_raw_data.py

Подготовка исходного CSV файла.

Исходный датасет содержал только данные за январь 2024 года. Поэтому разбиение по годам не имело смысла.

Для имитации инкрементальной загрузки данные были разделены на дневные батчи:

```text
flights_2024_01_01.csv
flights_2024_01_02.csv
...
flights_2024_01_31.csv
```

---

### 01_bronze.py

Загрузка сырых данных в Bronze слой.

Каждый дневной CSV файл загружается отдельным append batch в Delta таблицу:

```text
lakehouse/bronze/flights
```

Это создаёт историю версий Delta Lake.

---

### 02_silver.py

Создание Silver слоя.

Выполняется:

- удаление отменённых рейсов
- удаление diverted рейсов
- удаление пропусков
- фильтрация выбросов ARR_DELAY
- нормализация категорий
- генерация признаков:
  - hour
  - day_of_week
  - season
  - route
  - is_delayed
  - flight_id

Результат сохраняется в:

```text
lakehouse/silver/flights
```

---

### 03_gold.py

Создание Gold слоя.

Формируются две витрины:

#### 1. Аналитическая витрина

```text
lakehouse/gold/flight_delay_aggregates
```

Содержит агрегаты:

- средняя задержка
- медианная задержка
- delay rate
- число рейсов

По измерениям:

- origin
- airline
- hour
- season

#### 2. Feature table

```text
lakehouse/gold/flight_delay_features
```

Используется для ML моделей.

---

### 04_delta_ops.py

Демонстрация возможностей Delta Lake:

- MERGE
- History
- Time Travel
- OPTIMIZE / Compact
- VACUUM

---

### 05_train_ml.py

Обучение моделей машинного обучения.

#### Регрессия

Цель:

```text
ARR_DELAY
```

Модели:

- LinearRegression
- RandomForestRegressor

#### Классификация

Цель:

```text
is_delayed
```

Модели:

- LogisticRegression
- RandomForestClassifier

Результаты логируются в MLflow.

---

## Использованные partition strategy

## Почему не year/month как в условии

В условии предлагалось:

- Bronze загружать по годам
- Silver partition_by = year/month

Однако используемая версия датасета содержала только один месяц:

```text
January 2024
```

Поэтому year/month partition дал бы всего одну партицию и не имел бы практического смысла.

---

## Принятое решение

### Bronze

Использовано разбиение по дням:

```text
31 daily batches
```

Это имитирует поступление данных частями.

### Silver

Использовано partitioning:

```python
partition_by=["year", "month", "day"]
```

Структура:

```text
year=2024/month=1/day=1
year=2024/month=1/day=2
...
year=2024/month=1/day=31
```

Это лучше соответствует реальным данным и позволяет фильтровать запросы по дням.

### Gold Feature Table

Также используется:

```python
partition_by=["year", "month", "day"]
```

---

## Пример Polars Lazy Query

```python
q = (
    pl.scan_delta(SILVER_PATH)
      .filter(pl.col("year") == 2024)
      .filter(pl.col("month") == 1)
      .select(["origin", "airline", "arr_delay"])
      .group_by(["origin", "airline"])
      .agg(pl.col("arr_delay").mean())
)
```

### Explain plan

```text
PROJECT 5/20 COLUMNS
SELECTION: year=2024 AND month=1
```

Это демонстрирует:

- projection pushdown
- predicate pushdown

---

## Запуск проекта

## Локальный запуск

```bash
pip install -r requirements.txt

python src/00_split_raw_data.py
python src/01_bronze.py
python src/02_silver.py
python src/03_gold.py
python src/04_delta_ops.py
python src/05_train_ml.py
```

---

## Docker запуск

```bash
docker compose up --build
```

После запуска MLflow доступен:

```text
http://localhost:5000
```

---

## Полученные результаты

Реализован полноценный lakehouse pipeline:

- Bronze слой
- Silver слой
- Gold слой
- Delta Lake versioning
- MERGE updates
- Time Travel
- Vacuum / Optimize
- ML pipeline
- MLflow experiment tracking
- Docker deployment

---

## Вывод

В работе показан полный цикл обработки больших данных:

- ingestion
- cleaning
- feature engineering
- analytics
- machine learning
- experiment tracking

Архитектура адаптирована под реальные ограничения датасета и сохраняет смысл классического Bronze / Silver / Gold подхода.
