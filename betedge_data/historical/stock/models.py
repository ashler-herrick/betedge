"""
Data models for historical stock data.
"""

from dataclasses import dataclass
from typing import Optional

from betedge_data.historical.config import HistoricalClientConfig

config = HistoricalClientConfig()

@dataclass
class HistoricalStockRequest:
    """Request parameters for historical data."""

    ticker: str
    start_date: str
    end_date: str
    interval: int = 60_000
    rth: bool = True
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    venue: Optional[str] = None
