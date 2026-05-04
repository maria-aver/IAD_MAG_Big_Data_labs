import polars as pl

from config import SILVER_PATH


df = pl.scan_delta(str(SILVER_PATH))

print(df.select(pl.len()).collect())
print(df.head(5).collect())
print(df.collect_schema())

query = (
    df
    .filter(pl.col("year") == 2024)
    .filter(pl.col("month") == 1)
    .select(["year", "month", "origin", "airline", "arr_delay"])
    .group_by(["origin", "airline"])
    .agg(pl.col("arr_delay").mean().alias("avg_arr_delay"))
)

print(query.explain())