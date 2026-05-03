import polars as pl
from deltalake import write_deltalake

from config import RAW_DATA_DIR, BRONZE_PATH


def extract_year_from_filename(file_path):
    """
    Expected filename examples:
    flights_2018.csv
    2018.csv
    US_flights_2020.csv
    """
    name = file_path.stem

    for part in name.split("_"):
        if part.isdigit() and len(part) == 4:
            return int(part)

    raise ValueError(f"Cannot extract year from filename: {file_path.name}")


def load_csv_to_bronze():
    csv_files = sorted(RAW_DATA_DIR.glob("*.csv"))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {RAW_DATA_DIR}")

    BRONZE_PATH.parent.mkdir(parents=True, exist_ok=True)

    for file_path in csv_files:
        year = extract_year_from_filename(file_path)

        print(f"Loading {file_path.name} to bronze Delta table...")

        df = pl.read_csv(
            file_path,
            infer_schema_length=10_000,
            ignore_errors=True,
        )

        df = df.with_columns(
            pl.lit(year).alias("source_year")
        )

        write_deltalake(
            table_or_uri=str(BRONZE_PATH),
            data=df.to_arrow(),
            mode="append",
        )

        print(f"Loaded year {year}: {df.shape[0]} rows")

    print(f"Bronze table saved to: {BRONZE_PATH}")


if __name__ == "__main__":
    load_csv_to_bronze()