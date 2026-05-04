import polars as pl
from deltalake import write_deltalake

from config import SILVER_PATH, GOLD_AGG_PATH, GOLD_FEATURES_PATH


def build_aggregates():
    print("Building gold aggregates...")

    silver = pl.scan_delta(str(SILVER_PATH))

    agg_df = (
        silver
        .group_by(["origin", "airline_code", "hour", "season"])
        .agg(
            pl.len().alias("flights_count"),
            pl.col("arr_delay").mean().alias("avg_arr_delay"),
            pl.col("arr_delay").median().alias("median_arr_delay"),
            pl.col("dep_delay").mean().alias("avg_dep_delay"),
            pl.col("is_delayed").mean().alias("delay_rate"),
            pl.col("distance").mean().alias("avg_distance"),
        )
        .sort(["origin", "airline_code", "hour", "season"])
        .collect()
    )

    GOLD_AGG_PATH.parent.mkdir(parents=True, exist_ok=True)

    write_deltalake(
        table_or_uri=str(GOLD_AGG_PATH),
        data=agg_df.to_arrow(),
        mode="overwrite",
    )

    print(f"Gold aggregates saved to: {GOLD_AGG_PATH}")
    print(f"Rows: {agg_df.height}")


def build_features():
    print("Building gold ML feature table...")

    silver = pl.scan_delta(str(SILVER_PATH))

    features_df = (
        silver
        .select(
            [
                "flight_id",
                "flight_date",
                "year",
                "month",
                "day",
                "day_of_week",
                "season",
                "hour",
                "airline_code",
                "origin",
                "dest",
                "route",
                "distance",
                "arr_delay",
                "is_delayed",
            ]
        )
        .filter(pl.col("arr_delay").is_not_null())
        .filter(pl.col("is_delayed").is_not_null())
        .filter(pl.col("distance").is_not_null())
        .collect()
    )

    write_deltalake(
        table_or_uri=str(GOLD_FEATURES_PATH),
        data=features_df.to_arrow(),
        mode="overwrite",
        partition_by=["year", "month", "day"],
    )

    print(f"Gold features saved to: {GOLD_FEATURES_PATH}")
    print(f"Rows: {features_df.height}")


def main():
    build_aggregates()
    build_features()
    print("Gold layer completed.")


if __name__ == "__main__":
    main()