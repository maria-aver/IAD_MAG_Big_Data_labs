import polars as pl
from deltalake import DeltaTable, write_deltalake

from config import BRONZE_PATH, SILVER_PATH


REQUIRED_COLUMNS = [
    "Year",
    "Month",
    "DayofMonth",
    "DayOfWeek",
    "FlightDate",
    "Operating_Airline ",
    "IATA_Code_Operating_Airline",
    "Flight_Number_Operating_Airline",
    "Origin",
    "OriginCityName",
    "Dest",
    "DestCityName",
    "CRSDepTime",
    "DepDelay",
    "ArrDelay",
    "Cancelled",
    "Diverted",
    "Distance",
    "source_year",
    "source_month",
    "source_day",
]


def season_expr():
    return (
        pl.when(pl.col("month").is_in([12, 1, 2]))
        .then(pl.lit("winter"))
        .when(pl.col("month").is_in([3, 4, 5]))
        .then(pl.lit("spring"))
        .when(pl.col("month").is_in([6, 7, 8]))
        .then(pl.lit("summer"))
        .otherwise(pl.lit("autumn"))
    )


def main():
    print("Reading bronze table...")

    bronze = pl.scan_delta(str(BRONZE_PATH))

    silver_lf = (
        bronze
        .select(REQUIRED_COLUMNS)
        .with_columns(
            pl.col("FlightDate").str.strptime(pl.Date, strict=False).alias("flight_date"),
            pl.col("CRSDepTime").cast(pl.Int32, strict=False).alias("crs_dep_time"),
            pl.col("DepDelay").cast(pl.Float64, strict=False).alias("dep_delay"),
            pl.col("ArrDelay").cast(pl.Float64, strict=False).alias("arr_delay"),
            pl.col("Cancelled").cast(pl.Float64, strict=False).alias("cancelled"),
            pl.col("Diverted").cast(pl.Float64, strict=False).alias("diverted"),
            pl.col("Distance").cast(pl.Float64, strict=False).alias("distance"),
            pl.col("Origin").str.to_uppercase().alias("origin"),
            pl.col("Dest").str.to_uppercase().alias("dest"),
            pl.col("IATA_Code_Operating_Airline").str.to_uppercase().alias("airline_code"),
            pl.col("Operating_Airline ").str.strip_chars().alias("airline"),
            pl.col("Flight_Number_Operating_Airline").cast(pl.Int32, strict=False).alias("flight_number"),
        )
        .filter(pl.col("cancelled") == 0)
        .filter(pl.col("diverted") == 0)
        .filter(pl.col("arr_delay").is_not_null())
        .filter(pl.col("dep_delay").is_not_null())
        .filter(pl.col("distance").is_not_null())
        .filter(pl.col("flight_date").is_not_null())
        .filter(pl.col("crs_dep_time").is_not_null())
        .filter(pl.col("arr_delay").is_between(-60, 300))
        .with_columns(
            pl.col("flight_date").dt.year().alias("year"),
            pl.col("flight_date").dt.month().alias("month"),
            pl.col("flight_date").dt.day().alias("day"),
            pl.col("flight_date").dt.weekday().alias("day_of_week"),
            (pl.col("crs_dep_time") // 100).alias("hour"),
            (pl.col("origin") + pl.lit("_") + pl.col("dest")).alias("route"),
            (pl.col("arr_delay") > 15).cast(pl.Int8).alias("is_delayed"),
        )
        .with_columns(
            season_expr().alias("season")
        )
        .with_columns(
            (
                pl.col("flight_date").cast(pl.Utf8)
                + pl.lit("_")
                + pl.col("airline_code")
                + pl.lit("_")
                + pl.col("flight_number").cast(pl.Utf8)
                + pl.lit("_")
                + pl.col("origin")
                + pl.lit("_")
                + pl.col("dest")
                + pl.lit("_")
                + pl.col("crs_dep_time").cast(pl.Utf8)
            ).alias("flight_id")
        )
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
                "airline",
                "airline_code",
                "flight_number",
                "origin",
                "dest",
                "route",
                "dep_delay",
                "arr_delay",
                "distance",
                "is_delayed",
                "source_year",
                "source_month",
                "source_day",
            ]
        )
    )

    print("Collecting silver dataframe...")
    silver_df = silver_lf.collect()
    silver_df = silver_df.unique(subset=["flight_id"], keep="last")

    print(f"Silver rows: {silver_df.height}")
    print(f"Silver columns: {silver_df.columns}")

    SILVER_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not SILVER_PATH.exists():
        print("Silver table does not exist. Creating new Delta table...")

        write_deltalake(
            table_or_uri=str(SILVER_PATH),
            data=silver_df.to_arrow(),
            mode="overwrite",
            partition_by=["year", "month", "day"],
        )

    else:
        print("Silver table exists. Running MERGE...")

        delta_table = DeltaTable(str(SILVER_PATH))

        (
            delta_table.merge(
                source=silver_df.to_arrow(),
                predicate="target.flight_id = source.flight_id",
                source_alias="source",
                target_alias="target",
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )

    print(f"Silver table saved to: {SILVER_PATH}")


if __name__ == "__main__":
    main()