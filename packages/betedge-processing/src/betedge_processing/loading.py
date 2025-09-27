from typing import Optional, List
import logging

import polars as pl

from betedge_data.client.minio import MinIOConfig, get_minio_storage_options

logger = logging.getLogger()


def glob_eod(
    ticker: str,
    start_yearmo: Optional[int] = None,
    end_yearmo: Optional[int] = None,
    get_all: bool = False,
    ext: str = "parquet",
    bucket: str = "betedge-data",
) -> List[str]:
    """
    Generate glob patterns for EOD options data.

    Args:
        ticker: Stock symbol (e.g., "SPY")
        start_yearmo: Start year-month as YYYYMM (e.g., 202401)
        end_yearmo: End year-month as YYYYMM (e.g., 202403)
        get_all: If True, get all data for ticker
        ext: File extension

    Returns:
        List of glob patterns
    """
    if not start_yearmo and not end_yearmo and get_all == False:
        raise ValueError("At least start_yearmo must be provided if get_all is False.")

    base_path = f"{bucket}/historical-options/eod/{ticker}"

    if get_all:
        return [f"{base_path}/*/*/data.{ext}"]

    # Handle single yearmo
    if start_yearmo and not end_yearmo:
        year = start_yearmo // 100
        month = start_yearmo % 100
        return [f"{base_path}/{year}/{month:02d}/data.{ext}"]

    # Handle range
    if start_yearmo and end_yearmo:
        patterns = []
        current = start_yearmo
        end = end_yearmo or start_yearmo
        while current <= end:
            year = current // 100
            month = current % 100
            patterns.append(f"{base_path}/{year}/{month:02d}/data.{ext}")

            # Increment month
            if month == 12:
                current = (year + 1) * 100 + 1
            else:
                current = year * 100 + (month + 1)

        return patterns

    return []


def load_eod_data(patterns: List[str], raise_err: bool = True) -> pl.DataFrame:
    opts = get_minio_storage_options(MinIOConfig())

    if len(patterns) == 1:
        return pl.read_parquet(f"s3://{patterns[0]}", storage_options=opts)

    else:
        dfs = []
        for pattern in patterns:
            try:
                dfs.append(pl.read_parquet(f"s3://{pattern}", storage_options=opts))
            except OSError as e:
                logger.error(
                    f"Encountered OSError for pattern {pattern}, file likely missing from ObjectStore."
                )
                if raise_err:
                    raise e

        return pl.concat(dfs)
