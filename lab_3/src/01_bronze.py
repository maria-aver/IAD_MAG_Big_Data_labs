import polars as pl
from deltalake import write_deltalake

from config import RAW_DATA_DIR, BRONZE_PATH


def extract_batch_date_from_filename(file_path):
    """
    Expected filename:
    flights_2024_01_01.csv
    """
    parts = file_path.stem.split("_")

    year = int(parts[-3])
    month = int(parts[-2])
    day = int(parts[-1])

    return year, month, day


def load_csv_to_bronze():
    csv_files = sorted(RAW_DATA_DIR.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {RAW_DATA_DIR}")

    BRONZE_PATH.parent.mkdir(parents=True, exist_ok=True)

    for file_path in csv_files:
        year, month, day = extract_batch_date_from_filename(file_path)

        print(f"Loading {file_path.name} to bronze Delta table...")

        df = pl.read_csv(
            file_path,
            infer_schema_length=10_000,
            ignore_errors=True,
        )

        df = df.with_columns(
            pl.lit(year).alias("source_year"),
            pl.lit(month).alias("source_month"),
            pl.lit(day).alias("source_day"),
        )

        write_deltalake(
            table_or_uri=str(BRONZE_PATH),
            data=df.to_arrow(),
            mode="append",
        )

        print(f"Loaded batch {year}-{month:02d}-{day:02d}: {df.shape[0]} rows")

    print(f"Bronze table saved to: {BRONZE_PATH}")


if __name__ == "__main__":
    load_csv_to_bronze()