import polars as pl
from deltalake import DeltaTable

from config import SILVER_PATH, GOLD_FEATURES_PATH


def show_history(table_path, table_name):
    print(f"\n=== History: {table_name} ===")
    dt = DeltaTable(str(table_path))
    history = dt.history()

    for item in history:
        print(item)


def time_travel_example():
    print("\n=== Time travel example: silver version 0 ===")

    df_v0 = pl.scan_delta(
        str(SILVER_PATH),
        version=0,
    )

    print(df_v0.select(pl.len()).collect())
    print(df_v0.head(5).collect())


def optimize_example():
    print("\n=== Optimize / compaction: silver ===")

    dt = DeltaTable(str(SILVER_PATH))

    result = dt.optimize.compact()

    print(result)


def vacuum_example():
    print("\n=== Vacuum: gold features ===")

    dt = DeltaTable(str(GOLD_FEATURES_PATH))

    result = dt.vacuum(
        retention_hours=168,
        dry_run=True,
    )

    print("Files that would be removed:")
    print(result)


def main():
    show_history(SILVER_PATH, "silver.flights")
    show_history(GOLD_FEATURES_PATH, "gold.flight_delay_features")

    time_travel_example()
    optimize_example()
    vacuum_example()

    print("\nDelta operations completed.")


if __name__ == "__main__":
    main()