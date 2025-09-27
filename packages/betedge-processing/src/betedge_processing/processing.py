import polars as pl


def calc_mid_and_spread(df: pl.LazyFrame) -> pl.LazyFrame:
    df = df.filter(
    (pl.col("bid_size") > 0) & (pl.col("ask_size") > 0)
                ).with_columns([
                    ((pl.col("bid") + pl.col("ask"))/2).alias("mid"),
                    (pl.col("bid") - pl.col("ask")).abs().alias("spread"),
                ])

    return df


def join_stock(df: pl.LazyFrame) -> pl.LazyFrame:
    stock = df.filter(pl.col("expiration") == 0)
    joined = df.filter(pl.col("expiration") != 0).join(
    stock, on=["ms_of_day", "date"]
    )   
    return joined


def calc_dte(df: pl.LazyFrame) -> pl.LazyFrame:
    result = df.with_columns([
    (
        pl.col("end_date").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d") - 
        pl.col("start_date").cast(pl.Utf8).str.strptime(pl.Date, "%Y%m%d")
    ).dt.total_days().alias("days_between")
    ])