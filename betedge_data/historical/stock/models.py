"""
Data models for historical option data.
"""

from pydantic import BaseModel, Field, field_validator
from betedge_data.historical.utils import interval_ms_to_string
from datetime import datetime

from betedge_data.common.interface import IRequest


class HistStockRequest(BaseModel, IRequest):
    """Request parameters for historical option data."""

    # Required fields (no defaults)
    root: str = Field(..., description="Security symbol")
    date: int = Field(..., description="The date in YYYYMMDD")

    # Optional fields (with defaults)
    interval: int = Field(default=60_000, ge=0, description="Interval in milliseconds")
    return_format: str = Field(default="parquet", description="Return format: parquet or ipc")
    endpoint: str = Field(default="quote", description="Endpoint to map to: options include 'quote', 'ohlc'")

    @field_validator("date")
    @classmethod
    def validate_date_format(cls, v: int) -> int:
        """Validate date is in YYYYMMDD format."""
        date_str = str(v)
        if len(date_str) != 8:
            raise ValueError(f"Date must be 8 digits (YYYYMMDD), got {v}")

        try:
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])

            if not (1 <= month <= 12):
                raise ValueError(f"Invalid month in date {v}")
            if not (1 <= day <= 31):
                raise ValueError(f"Invalid day in date {v}")
            if not (1990 <= year <= 2050):
                raise ValueError(f"Year must be between 1990-2050, got {year}")

        except ValueError as e:
            raise ValueError(f"Invalid date format {v}: {e}")

        return v

    @field_validator("interval")
    @classmethod
    def validate_interval(cls, v: int) -> int:
        """Validate interval constraints per ThetaData API."""
        if v < 60000:
            raise ValueError(f"Intervals under 60000ms (1 minute) are not officially supported, got {v}")
        return v

    @field_validator("return_format")
    @classmethod
    def validate_return_format(cls, v: str) -> str:
        """Validate return format is supported."""
        if v not in ["parquet", "ipc"]:
            raise ValueError(f"return_format must be 'parquet' or 'ipc', got '{v}'")
        return v

    @field_validator("endpoint")
    @classmethod
    def validate_endpoint(cls, v: str) -> str:
        """Validate endpoint is supported."""
        if v not in ["quote", "ohlc"]:
            raise ValueError(f"endpoint must be 'quote' or 'ohlc', got '{v}'")
        return v

    def generate_object_key(self) -> str:
        """
        Generate MinIO object keys for historical stock data.

        Returns list of object keys for each date in the request.
        Format: historical-stock/quote/{root}/{year}/{month:02d}/{day:02d}/{interval_str}/data.parquet

        Returns:
            Object key
        """
        date_obj = datetime.strptime(str(self.date), "%Y%m%d")

        # Convert interval to human-readable format
        interval_str = interval_ms_to_string(self.interval)

        # Generate object key following the established pattern
        base_path = f"historical-stock/{self.endpoint}/{self.root}"
        date_path = f"{date_obj.year}/{date_obj.month:02d}/{date_obj.day:02d}"
        object_key = f"{base_path}/{date_path}/{interval_str}/data.{self.return_format}"

        return object_key
