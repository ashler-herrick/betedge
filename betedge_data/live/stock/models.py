"""
Data models for live stock data.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class LiveQuoteRequest:
    """Request parameters for live quote data."""

    ticker: str
    start_date: str
    end_date: str
    ivl: int
    use_csv: bool = False
    venue: Optional[str] = None
    rth: bool = True
