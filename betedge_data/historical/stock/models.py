"""
Data models for historical option data.
"""

from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator
from betedge_data.historical.utils import interval_ms_to_string
from datetime import datetime

from betedge_data.common.interface import IRequest


class HistStockRequest(BaseModel, IRequest):
    """Request parameters for historical stock data."""

    # Required fields
    root: str = Field(..., description="Security symbol")

    # Date fields (one of these must be provided)
    date: Optional[int] = Field(None, description="The date in YYYYMMDD format (for quote/ohlc/single-day EOD)")
    year: Optional[int] = Field(None, description="The year for EOD data (alternative to date)")

    # Optional fields (with defaults)
    interval: int = Field(default=60_000, ge=0, description="Interval in milliseconds")
    return_format: str = Field(default="parquet", description="Return format: parquet or ipc")
    endpoint: str = Field(default="quote", description="Endpoint: 'quote', 'ohlc', or 'eod'")

    @field_validator("date")
    @classmethod
    def validate_date_format(cls, v: Optional[int]) -> Optional[int]:
        """Validate date is in YYYYMMDD format."""
        if v is None:
            return v

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

    @field_validator("year")
    @classmethod
    def validate_year(cls, v: Optional[int]) -> Optional[int]:
        """Validate year is within reasonable bounds."""
        if v is None:
            return v
        if not (2020 <= v <= 2030):
            raise ValueError(f"Year must be between 2020-2030, got {v}")
        return v

    @model_validator(mode="after")
    def validate_date_or_year(self) -> "HistStockRequest":
        """Ensure either date or year is provided, and validate consistency with endpoint."""
        if self.date is None and self.year is None:
            raise ValueError("Either 'date' or 'year' must be provided")

        if self.date is not None and self.year is not None:
            raise ValueError("Provide either 'date' or 'year', not both")

        # For EOD endpoint, prefer year but allow date
        if self.endpoint == "eod" and self.date is not None:
            # Extract year from date for consistency
            date_str = str(self.date)
            extracted_year = int(date_str[:4])
            if self.year is None:
                self.year = extracted_year

        # For non-EOD endpoints, require date
        if self.endpoint in ["quote", "ohlc"] and self.date is None:
            raise ValueError(f"Endpoint '{self.endpoint}' requires 'date' field")

        return self

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
        if v not in ["quote", "ohlc", "eod"]:
            raise ValueError(f"endpoint must be 'quote', 'ohlc', or 'eod', got '{v}'")
        return v

    def get_processing_year(self) -> int:
        """Get the year for processing (from date or year field)."""
        if self.year is not None:
            return self.year
        elif self.date is not None:
            date_str = str(self.date)
            return int(date_str[:4])
        else:
            raise ValueError("No date or year available")

    def get_date_range_for_eod(self) -> tuple[int, int]:
        """Get start and end dates for EOD processing."""
        year = self.get_processing_year()
        start_date = int(f"{year}0101")
        end_date = int(f"{year + 1}0101")
        return start_date, end_date

    def generate_object_key(self) -> str:
        """Generate MinIO object keys for historical stock data."""
        if self.endpoint == "eod":
            # EOD uses yearly aggregation format
            year = self.get_processing_year()
            return f"historical-stock/eod/{self.root}/{year}/data.{self.return_format}"
        else:
            # Regular quote/ohlc endpoints use daily format with intervals
            if self.date is None:
                raise ValueError("Date is required for non-EOD endpoints")
            date_obj = datetime.strptime(str(self.date), "%Y%m%d")
            interval_str = interval_ms_to_string(self.interval)
            base_path = f"historical-stock/{self.endpoint}/{self.root}"
            date_path = f"{date_obj.year}/{date_obj.month:02d}/{date_obj.day:02d}"
            return f"{base_path}/{date_path}/{interval_str}/data.{self.return_format}"
