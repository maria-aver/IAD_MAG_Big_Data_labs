import zipfile
from pathlib import Path

import polars as pl

from config import PROJECT_ROOT


DATA_DIR = PROJECT_ROOT / "data"
ZIP_PATH = DATA_DIR / "flight_data_2018_2024.csv.zip"
RAW_DIR = DATA_DIR / "raw"


def find_date_column(columns):
    candidates = [
        "FL_DATE",
        "FlightDate",
        "flight_date",
        "Date",
        "date",
    ]

    for col in candidates:
        if col in columns:
            return col

    raise ValueError(f"Cannot find flight date column. Available columns: {columns}")


def main():
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(ZIP_PATH, "r") as z:
        csv_files = [name for name in z.namelist() if name.endswith(".csv")]

        if len(csv_files) != 1:
            raise ValueError(f"Expected one CSV inside zip, found: {csv_files}")

        csv_name = csv_files[0]
        print(f"Reading {csv_name} from {ZIP_PATH.name}...")

        with z.open(csv_name) as f:
            df = pl.read_csv(
                f,
                infer_schema_length=10_000,
                ignore_errors=True,
            )

    date_col = find_date_column(df.columns)
    print(f"Using date column: {date_col}")

    df = df.with_columns(
        pl.col(date_col)
        .str.strptime(pl.Date, strict=False)
        .alias("_flight_date")
    )

    dates = (
        df.select("_flight_date")
        .drop_nulls()
        .unique()
        .sort("_flight_date")
        .to_series()
        .to_list()
    )

    print(f"Found {len(dates)} daily batches")

    for flight_date in dates:
        date_str = flight_date.strftime("%Y_%m_%d")
        out_path = RAW_DIR / f"flights_{date_str}.csv"

        day_df = (
            df.filter(pl.col("_flight_date") == flight_date)
            .drop("_flight_date")
        )

        day_df.write_csv(out_path)

        print(f"Saved {out_path.name}: {day_df.height} rows")

    print("Done.")


if __name__ == "__main__":
    main()