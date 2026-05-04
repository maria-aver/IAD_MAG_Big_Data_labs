import polars as pl

from config import BRONZE_PATH


df = pl.scan_delta(str(BRONZE_PATH))

print("Bronze columns:")
for col in df.collect_schema().names():
    print(col)