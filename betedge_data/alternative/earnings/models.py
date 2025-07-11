"""
Data models for earnings data.
"""

from typing import Optional
from datetime import datetime

from pydantic import BaseModel, Field, field_validator
from betedge_data.common.interface import IRequest


class EarningsRequest(BaseModel, IRequest):
    """Request parameters for monthly earnings data."""

    # Required fields
    year: int = Field(..., ge=2020, le=2030, description="Year for earnings data collection")
    month: int = Field(..., ge=1, le=12, description="Month for earnings data collection (1-12)")

    # Optional fields with defaults
    return_format: str = Field(default="parquet", description="Return format: parquet")

    @field_validator("return_format")
    @classmethod
    def validate_return_format(cls, v: str) -> str:
        """Validate return format is supported."""
        if v not in ["parquet"]:
            raise ValueError(f"return_format must be 'parquet', got '{v}'")
        return v

    def generate_object_key(self) -> str:
        """
        Generate MinIO object key for earnings data.

        Returns:
            Object key for the earnings data file
        """
        return f"earnings/{self.year}/{self.month:02d}/data.{self.return_format}"


class EarningsRecord(BaseModel):
    """Normalized earnings record."""

    # Core fields
    date: str = Field(..., description="Date in YYYY-MM-DD format")
    symbol: str = Field(..., description="Stock symbol")
    name: str = Field(..., description="Company name")
    time: Optional[str] = Field(default=None, description="Earnings announcement time")

    # Financial fields (nullable for missing data)
    eps: Optional[float] = Field(default=None, description="Earnings per share")
    eps_forecast: Optional[float] = Field(default=None, description="Consensus EPS forecast")
    surprise_pct: Optional[float] = Field(default=None, description="Earnings surprise percentage")
    market_cap: Optional[int] = Field(default=None, description="Market capitalization in dollars")

    # Additional fields
    fiscal_quarter_ending: Optional[str] = Field(default=None, description="Fiscal quarter ending (e.g., 'Mar/2025')")
    num_estimates: Optional[int] = Field(default=None, description="Number of analyst estimates")

    @field_validator("symbol")
    @classmethod
    def validate_symbol(cls, v: str) -> str:
        """Validate and normalize symbol."""
        return v.upper().strip()

    @field_validator("date")
    @classmethod
    def validate_date_format(cls, v: str) -> str:
        """Validate date is in YYYY-MM-DD format."""
        try:
            datetime.strptime(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Date must be in YYYY-MM-DD format, got '{v}'")
        return v

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate company name."""
        return v.strip()
