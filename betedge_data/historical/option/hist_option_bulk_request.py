"""
Data models for historical option data.
"""

from typing import Optional
from pydantic import BaseModel, Field, field_validator, model_validator
from betedge_data.historical.utils import interval_ms_to_string, expiration_to_string
from betedge_data.historical.config import get_hist_client_config
from datetime import datetime
from urllib.parse import urlencode

CONFIG = get_hist_client_config()


class HistOptionBulkRequest(BaseModel):
    """Request parameters for historical option data."""

    # Required fields
    root: str = Field(..., description="Security symbol")
    data_schema: str = Field(..., description="Data schema type: options include 'quote', 'eod'")

    # Date fields (one of these must be provided)
    date: Optional[int] = Field(None, description="The date in YYYYMMDD format (for quote/single-day EOD)")
    yearmo: Optional[int] = Field(None, description="Year-month in YYYYMM format for EOD data")

    # Optional fields (with defaults)
    interval: int = Field(default=900_000, ge=0, description="Interval in milliseconds")
    return_format: str = Field(default="parquet", description="Return format: parquet or ipc")

    # Default, non configurable
    exp: int = 0

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

    @field_validator("yearmo")
    @classmethod
    def validate_yearmo(cls, v: Optional[int]) -> Optional[int]:
        """Validate yearmo is in YYYYMM format."""
        if v is None:
            return v

        yearmo_str = str(v)
        if len(yearmo_str) != 6:
            raise ValueError(f"Yearmo must be 6 digits (YYYYMM), got {v}")

        try:
            year = int(yearmo_str[:4])
            month = int(yearmo_str[4:6])

            if not (1 <= month <= 12):
                raise ValueError(f"Invalid month in yearmo {v}")
            if not (2020 <= year <= 2030):
                raise ValueError(f"Year must be between 2020-2030, got {year}")

        except ValueError as e:
            raise ValueError(f"Invalid yearmo format {v}: {e}")

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

    @field_validator("data_schema")
    @classmethod
    def validate_data_schema(cls, v: str) -> str:
        """Validate data_schema is supported."""
        if v not in ["quote", "ohlc", "eod"]:
            raise ValueError(f"data_schema must be 'quote', 'ohlc', or 'eod', got '{v}'")
        return v

    @model_validator(mode="after")
    def validate_date_or_yearmo(self) -> "HistOptionBulkRequest":
        """Ensure either date or yearmo is provided, and validate consistency with data_schema."""
        if self.date is None and self.yearmo is None:
            raise ValueError("Either 'date' or 'yearmo' must be provided")

        if self.date is not None and self.yearmo is not None:
            raise ValueError("Provide either 'date' or 'yearmo', not both")

        # For EOD data_schema, prefer yearmo but allow date
        if self.data_schema == "eod" and self.date is not None:
            # Extract yearmo from date for consistency
            date_str = str(self.date)
            extracted_yearmo = int(date_str[:6])
            if self.yearmo is None:
                self.yearmo = extracted_yearmo

        # For non-EOD data_schemas, require date
        if self.data_schema in ["quote", "ohlc"] and self.date is None:
            raise ValueError(f"data_schema '{self.data_schema}' requires 'date' field")

        return self

    def get_processing_yearmo(self) -> int:
        """Get the year-month for processing (from date or yearmo field)."""
        if self.yearmo is not None:
            return self.yearmo
        elif self.date is not None:
            date_str = str(self.date)
            return int(date_str[:6])
        else:
            raise ValueError("No date or yearmo available")

    def get_date_range_for_eod(self) -> tuple[int, int]:
        """Get start and end dates for EOD processing (full month)."""
        yearmo = self.get_processing_yearmo()
        year = yearmo // 100
        month = yearmo % 100

        start_date = int(f"{year}{month:02d}01")

        # Calculate end date (last day of month)
        if month == 12:
            next_month_start = int(f"{year + 1}0101")
        else:
            next_month_start = int(f"{year}{month + 1:02d}01")

        # End date is day before next month starts
        end_date = next_month_start - 1

        return start_date, end_date

    def get_url(self) -> str:
        """Build URL for request"""
        base_url = f"{CONFIG.base_url}/bulk_hist/option/{self.data_schema}"
        if self.data_schema == "eod":
            start_date, end_date = self.get_date_range_for_eod()
            params = {
                "root": self.root,
                "exp": self.exp,
                "start_date": start_date,
                "end_date": end_date,
            }

        else:
            params = {
                "root": self.root,
                "exp": self.exp,
                "start_date": self.date,
                "end_date": self.date,
                "ilvl": self.interval,
            }
        return f"{base_url}?{urlencode(params)}"

    def generate_object_key(self) -> str:
        """Generate MinIO object keys for historical option data."""
        if self.data_schema == "eod":
            # EOD uses monthly aggregation format
            yearmo = self.get_processing_yearmo()
            year = yearmo // 100
            month = yearmo % 100
            return f"historical-options/eod/{self.root}/{year}/{month:02d}/data.{self.return_format}"
        else:
            # Regular quote/ohlc endpoints use daily format with intervals
            if self.date is None:
                raise ValueError("Date is required for non-EOD endpoints")
            exp_str = expiration_to_string(self.exp)
            date_obj = datetime.strptime(str(self.date), "%Y%m%d")
            interval_str = interval_ms_to_string(self.interval)
            base_path = f"historical-options/{self.data_schema}/{interval_str}/{exp_str}/{self.root}"
            date_path = f"{date_obj.year}/{date_obj.month:02d}/{date_obj.day:02d}"
            object_key = f"{base_path}/{date_path}/data.{self.return_format}"
            return object_key
