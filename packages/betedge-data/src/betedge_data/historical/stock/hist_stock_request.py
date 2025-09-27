"""
Data models for historical option data.
"""

from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator
from betedge_data.historical.utils import interval_ms_to_string
from betedge_data.historical.config import HistoricalConfig
from datetime import datetime
from urllib.parse import urlencode


class HistStockRequest(BaseModel):
    """Request parameters for historical stock data."""

    # Required fields
    root: str = Field(..., description="Security symbol")

    # Date fields (one of these must be provided)
    date: Optional[int] = Field(
        None, description="The date in YYYYMMDD format (for quote/ohlc/single-day EOD)"
    )
    year: Optional[int] = Field(
        None, description="The year for EOD data (alternative to date)"
    )

    data_schema: str = Field(
        ..., description="Data data_schema type: 'quote', 'ohlc', or 'eod'"
    )
    # Optional fields (with defaults)
    interval: int = Field(default=60_000, ge=0, description="Interval in milliseconds")
    return_format: str = Field(
        default="parquet", description="Return format: parquet or ipc"
    )

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

        # For EOD data_schema, prefer year but allow date
        if self.data_schema == "eod" and self.date is not None:
            # Extract year from date for consistency
            date_str = str(self.date)
            extracted_year = int(date_str[:4])
            if self.year is None:
                self.year = extracted_year

        # For non-EOD data_schemas, require date
        if self.data_schema in ["quote", "ohlc"] and self.date is None:
            raise ValueError(f"data_schema '{self.data_schema}' requires 'date' field")

        return self

    @field_validator("interval")
    @classmethod
    def validate_interval(cls, v: int) -> int:
        """Validate interval constraints per ThetaData API."""
        if v < 60000:
            raise ValueError(
                f"Intervals under 60000ms (1 minute) are not officially supported, got {v}"
            )
        return v

    @field_validator("return_format")
    @classmethod
    def validate_return_format(cls, v: str) -> str:
        """Validate return format is supported."""
        if v not in ["parquet", "ipc"]:
            raise ValueError(f"return_format must be 'parquet' or 'ipc', got '{v}'")
        return v

    @field_validator("data_schema")
    @classmethod
    def validate_data_schema(cls, v: str) -> str:
        """Validate data_schema is supported."""
        if v not in ["quote", "ohlc", "eod"]:
            raise ValueError(
                f"data_schema must be 'quote', 'ohlc', or 'eod', got '{v}'"
            )
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
        if self.data_schema == "eod":
            # EOD uses yearly aggregation format
            year = self.get_processing_year()
            return f"historical-stock/eod/{self.root}/{year}/data.{self.return_format}"
        else:
            # Regular quote/ohlc endpoints use daily format with intervals
            if self.date is None:
                raise ValueError("Date is required for non-EOD endpoints")
            date_obj = datetime.strptime(str(self.date), "%Y%m%d")
            interval_str = interval_ms_to_string(self.interval)
            base_path = (
                f"historical-stock/{self.data_schema}/{interval_str}/{self.root}"
            )
            date_path = f"{date_obj.year}/{date_obj.month:02d}/{date_obj.day:02d}"
            return f"{base_path}/{date_path}/data.{self.return_format}"

    def get_url(self) -> str:
        """Build URL for ThetaData historical stock endpoint."""
        config = HistoricalConfig()

        if self.data_schema == "eod":
            # EOD endpoint uses year range
            start_date, end_date = self.get_date_range_for_eod()
            params = {
                "root": self.root,
                "start_date": start_date,
                "end_date": end_date,
            }
            base_url = f"{config.base_url}/hist/stock/eod"
        else:
            # Regular endpoints use single date + interval
            params = {
                "root": self.root,
                "start_date": self.date,
                "end_date": self.date,
                "ivl": self.interval,
            }
            base_url = f"{config.base_url}/hist/stock/{self.data_schema}"

        return f"{base_url}?{urlencode(params)}"
