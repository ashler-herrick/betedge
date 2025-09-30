import logging
import time
from datetime import datetime
from typing import Optional, Dict
from dataclasses import dataclass

import pyarrow as pa

from betedge_data.job import HTTPJob
from betedge_data.exceptions import NoDataAvailableError

logger = logging.getLogger(__name__)


@dataclass
class EarningsRecord:
    """Normalized earnings record."""

    # Core fields
    date: str
    symbol: str
    name: str
    time: Optional[str]

    # Financial fields (nullable for missing data)
    eps: Optional[float]
    eps_forecast: Optional[float]
    surprise_pct: Optional[float]
    market_cap: Optional[int]
    # Additional fields
    fiscal_quarter_ending: Optional[str]
    num_estimates: Optional[int]


def normalize_earnings_record(row: Dict, date_str: str) -> EarningsRecord:
    """
    Normalize a single earnings record from API response.

    Args:
        row: Raw earnings data row
        date_str: Date in YYYY-MM-DD format

    Returns:
        Normalized EarningsRecord
    """
    return EarningsRecord(
        date=date_str,
        symbol=row.get("symbol", "").strip(),
        name=row.get("name", "").strip(),
        time=_parse_time(row.get("time", "")),
        eps=_parse_currency(row.get("eps", "")),
        eps_forecast=_parse_currency(row.get("epsForecast", "")),
        surprise_pct=_parse_percentage(row.get("surprise", "")),
        market_cap=_parse_market_cap(row.get("marketCap", "")),
        fiscal_quarter_ending=row.get("fiscalQuarterEnding", "").strip() or None,
        num_estimates=_parse_int(row.get("noOfEsts", "")),
    )


def _parse_currency(value: str) -> Optional[float]:
    """Parse currency values like '$0.56', '($2.55)', 'N/A', or empty string."""
    if not value or value == "N/A":
        return None

    try:
        # Remove $ and commas, handle parentheses for negative values
        cleaned = value.strip().replace("$", "").replace(",", "")

        if cleaned.startswith("(") and cleaned.endswith(")"):
            # Negative value in parentheses
            cleaned = "-" + cleaned[1:-1]

        return float(cleaned)
    except (ValueError, AttributeError):
        return None


def _parse_percentage(value: str) -> Optional[float]:
    """Parse percentage values like '12', 'N/A', or empty string."""
    if not value or value == "N/A":
        return None

    try:
        return float(str(value).strip())
    except (ValueError, AttributeError):
        return None


def _parse_market_cap(value: str) -> Optional[int]:
    """Parse market cap values like '$899,395,987', 'N/A', or empty string."""
    if not value or value == "N/A":
        return None

    try:
        # Remove $ and commas
        cleaned = str(value).strip().replace("$", "").replace(",", "")
        return int(float(cleaned))
    except (ValueError, AttributeError):
        return None


def _parse_int(value: str) -> Optional[int]:
    """Parse integer values, handling 'N/A' and empty strings."""
    if not value or value == "N/A":
        return None

    try:
        return int(str(value).strip())
    except (ValueError, AttributeError):
        return None


def _parse_time(value: str) -> Optional[str]:
    """Parse time values, handling 'time-not-supplied' and empty strings."""
    if not value or value == "time-not-supplied":
        return None

    try:
        return str(value).strip()
    except (ValueError, AttributeError):
        return None


def transform_date_string(date_str: str) -> str:
    """
    Transform a date string from format "Mon, Sep 29, 2025" to "2025-09-29"
    """
    try:
        # Parse the input format
        parsed_date = datetime.strptime(date_str, "%a, %b %d, %Y")

        # Return in ISO format (YYYY-MM-DD)
        return parsed_date.strftime("%Y-%m-%d")

    except ValueError as e:
        raise ValueError(f"Could not parse date string '{date_str}': {e}")


def process_earnings(http_result: HTTPJob) -> pa.Table:
    """
    Process earnings data from HTTP result.

    Args:
        http_result: HTTPJob containing JSON earnings data

    Returns:
        PyArrow table with normalized earnings data
    """
    start_time = time.time()

    if not (json_data := http_result.json):
        raise ValueError(f"No JSON data available in HTTP result {HTTPJob}")

    json_data = dict(json_data)

    earnings_data = json_data["data"]
    if not earnings_data:
        raise NoDataAvailableError

    date_str = transform_date_string(earnings_data["asOf"])

    # Normalize the data
    normalize_start = time.time()
    records = []
    rows = earnings_data["rows"]
    if not rows:
        raise NoDataAvailableError

    for row in earnings_data["rows"]:
        record = normalize_earnings_record(row, date_str)
        records.append(record)

    normalize_duration_ms = (time.time() - normalize_start) * 1000
    logger.debug(
        f"Normalized {len(records)} earnings records in {normalize_duration_ms:.1f}ms"
    )

    data = {
        "date": [r.date for r in records],
        "symbol": [r.symbol for r in records],
        "name": [r.name for r in records],
        "time": [r.time for r in records],
        "eps": [r.eps for r in records],
        "eps_forecast": [r.eps_forecast for r in records],
        "surprise_pct": [r.surprise_pct for r in records],
        "market_cap": [r.market_cap for r in records],
        "fiscal_quarter_ending": [r.fiscal_quarter_ending for r in records],
        "num_estimates": [r.num_estimates for r in records],
    }

    # Define schema with proper data types
    schema = pa.schema(
        [
            pa.field("date", pa.string()),
            pa.field("symbol", pa.string()),
            pa.field("name", pa.string()),
            pa.field("time", pa.string()),
            pa.field("eps", pa.float64()),
            pa.field("eps_forecast", pa.float64()),
            pa.field("surprise_pct", pa.float64()),
            pa.field("market_cap", pa.int64()),
            pa.field("fiscal_quarter_ending", pa.string()),
            pa.field("num_estimates", pa.int64()),
        ]
    )

    # Create PyArrow table
    table = pa.table(data, schema=schema)

    duration_ms = (time.time() - start_time) * 1000
    row_count = len(table)
    logger.info(
        f"Earnings processing completed: {row_count} records for {date_str} in {duration_ms:.1f}ms"
    )

    return table
