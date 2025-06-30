"""
Data models for historical option data.
"""

from typing import Optional

from pydantic import BaseModel, Field, field_validator, model_validator
from betedge_data.historical.config import HistoricalClientConfig

config = HistoricalClientConfig()


class HistoricalOptionRequest(BaseModel):
    """Request parameters for historical option data."""

    # Required fields (no defaults)
    root: str = Field(..., min_length=1, max_length=20, description="Security symbol")
    start_date: int = Field(..., ge=19900101, le=20501231, description="Start date in YYYYMMDD format")
    end_date: int = Field(..., ge=19900101, le=20501231, description="End date in YYYYMMDD format")
    max_dte: int = Field(..., ge=0, le=365, description="Maximum days to expiration")
    base_pct: float = Field(..., gt=0.0, le=1.0, description="Moneyness percentage (0.1 = 10%)")

    # Optional fields (with defaults)
    exp: int = Field(default=0, ge=0, description="Expiration date in YYYYMMDD format, 0 for all")
    interval: int = Field(default=900_000, ge=0, description="Interval in milliseconds")
    start_time: Optional[int] = Field(default=None, ge=0, le=86400000, description="Start time in ms since midnight ET")
    end_time: Optional[int] = Field(default=None, ge=0, le=86400000, description="End time in ms since midnight ET")
    use_csv: bool = Field(default=False, description="Return CSV format")
    pretty_time: bool = Field(default=False, description="Use human-readable timestamps")
    return_format: str = Field(default="parquet", description="Return format: parquet or ipc")

    @field_validator("root")
    @classmethod
    def validate_root(cls, v: str) -> str:
        """Validate security symbol format."""
        if not v.isalnum() and not all(c.isalnum() or c in "-_." for c in v):
            raise ValueError(f"Invalid root symbol format: {v}")
        return v.upper()

    @field_validator("start_date", "end_date")
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

    @field_validator("exp")
    @classmethod
    def validate_expiration(cls, v: int) -> int:
        """Validate expiration format."""
        if v == 0:
            return v  # 0 means all expirations

        # Validate YYYYMMDD format for specific expiration
        date_str = str(v)
        if len(date_str) != 8:
            raise ValueError(f"Expiration must be 8 digits (YYYYMMDD) or 0 for all, got {v}")

        try:
            year = int(date_str[:4])
            month = int(date_str[4:6])
            day = int(date_str[6:8])

            if not (1 <= month <= 12):
                raise ValueError(f"Invalid month in expiration {v}")
            if not (1 <= day <= 31):
                raise ValueError(f"Invalid day in expiration {v}")
            if not (2020 <= year <= 2030):
                raise ValueError(f"Expiration year must be between 2020-2030, got {year}")

        except ValueError as e:
            raise ValueError(f"Invalid expiration format {v}: {e}")

        return v

    @field_validator("interval")
    @classmethod
    def validate_interval(cls, v: int) -> int:
        """Validate interval constraints per ThetaData API."""
        if v > 0 and v < 60000:
            raise ValueError(f"Intervals under 60000ms (1 minute) are not officially supported, got {v}")
        return v

    @field_validator("return_format")
    @classmethod
    def validate_return_format(cls, v: str) -> str:
        """Validate return format is supported."""
        if v not in ["parquet", "ipc"]:
            raise ValueError(f"return_format must be 'parquet' or 'ipc', got '{v}'")
        return v

    @model_validator(mode="after")
    def validate_cross_fields(self) -> "HistoricalOptionRequest":
        """Validate cross-field constraints."""
        # Date range validation
        if self.start_date > self.end_date:
            raise ValueError(f"start_date ({self.start_date}) must be <= end_date ({self.end_date})")

        # exp=0 constraints per ThetaData API
        if self.exp == 0:
            if self.start_date != self.end_date:
                raise ValueError(
                    f"When exp=0 (all expirations), start_date must equal end_date. Got {self.start_date} != {self.end_date}"
                )

            if self.interval > 0 and self.interval < 60000:
                raise ValueError(f"When exp=0, minimum interval is 60000ms (1 minute), got {self.interval}")

        # Time range validation
        if self.start_time is not None and self.end_time is not None:
            if self.start_time >= self.end_time:
                raise ValueError(f"start_time ({self.start_time}) must be < end_time ({self.end_time})")

        # Date range recommendation (warning via exception for now)
        if self.start_date != self.end_date and self.exp != 0:
            # Calculate days difference
            start_str = str(self.start_date)
            end_str = str(self.end_date)

            try:
                from datetime import datetime

                start_dt = datetime.strptime(start_str, "%Y%m%d")
                end_dt = datetime.strptime(end_str, "%Y%m%d")
                days_diff = (end_dt - start_dt).days

                if days_diff > 7:
                    raise ValueError(
                        "Date ranges over 7 days may cause performance issues. Consider splitting request into smaller ranges."
                    )
            except ValueError as e:
                if "Date ranges over" in str(e):
                    raise e
                # Ignore datetime parsing errors, rely on field validators

        return self
