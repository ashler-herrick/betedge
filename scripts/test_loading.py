import polars as pl

from betedge_processing.loading import glob_eod, load_eod_data

patterns = glob_eod("AAPL", start_yearmo=202301, end_yearmo=202301)

df = load_eod_data(patterns, raise_err=False)

print(df.schema)

print((df.filter(pl.col("expiration") == 0)).select("date").unique().sort(by=["date"]))
