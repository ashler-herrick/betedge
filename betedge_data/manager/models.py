"""
Pydantic models for data processing API requests and responses.
"""

from datetime import datetime, timedelta
from typing import List, Literal, Optional, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, field_validator


class HistoricalOptionRequest(BaseModel):
    """Request model for historical option data endpoint."""

    root: str = Field(..., description="Option root symbol (e.g., 'AAPL')")
    exp: Union[str, List[str], int] = Field(
        0, description="Single expiration (e.g., '20231117'), list of expirations, or 0 for all expirations"
    )
    start_date: str = Field(..., description="Start date in YYYYMMDD format", pattern=r"^\d{8}$")
    end_date: str = Field(..., description="End date in YYYYMMDD format", pattern=r"^\d{8}$")
    max_dte: int = Field(30, description="Maximum days to expiration filter", gt=0, le=365)
    base_pct: float = Field(
        0.1, description="Base percentage for dynamic moneyness thresholds (e.g., 0.1 for 10%)", gt=0, le=1.0
    )
    interval: int = Field(900_000, description="Interval in milliseconds (default 900000 for 15 minutes)", ge=0)
    start_time: Optional[int] = Field(None, description="Start time in milliseconds since midnight ET", ge=0)
    end_time: Optional[int] = Field(None, description="End time in milliseconds since midnight ET", ge=0)
    max_workers: Optional[int] = Field(
        None, description="Maximum number of threads for multiple expirations", gt=0, le=20
    )

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, v, info):
        """Validate that start_date <= end_date."""
        start_date = info.data.get("start_date")
        if start_date and v < start_date:
            raise ValueError("end_date must be >= start_date")
        return v

    @field_validator("exp")
    @classmethod
    def validate_expiration_format(cls, v):
        """Validate expiration date format."""
        if isinstance(v, int):
            if v != 0:
                raise ValueError("Integer expiration must be 0 (for all expirations)")
            return v

        if isinstance(v, str):
            expirations = [v]
        else:
            expirations = v

        for exp in expirations:
            if not (len(exp) == 8 and exp.isdigit()):
                raise ValueError(f"Invalid expiration format: {exp}. Expected YYYYMMDD")
        return v


class DataProcessingResponse(BaseModel):
    """Standard response model for all data processing endpoints."""

    status: Literal["success", "error"] = Field(..., description="Processing status")
    request_id: UUID = Field(default_factory=uuid4, description="Unique request identifier")
    message: str = Field(..., description="Human-readable status message")
    data_type: Optional[Literal["stream", "parquet", "json"]] = Field(None, description="Type of data processed")
    storage_location: Optional[str] = Field(
        None, description="Storage location where data was published (S3 bucket/prefix or Kafka topic)"
    )
    records_count: Optional[int] = Field(None, description="Number of records processed", ge=0)
    processing_time_ms: Optional[int] = Field(None, description="Processing time in milliseconds", ge=0)
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Response timestamp in UTC")


class ErrorResponse(BaseModel):
    """Error response model with detailed information."""

    status: Literal["error"] = "error"
    request_id: UUID = Field(default_factory=uuid4, description="Unique request identifier")
    error_code: str = Field(..., description="Machine-readable error code")
    message: str = Field(..., description="Human-readable error message")
    details: Optional[str] = Field(None, description="Additional error details")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Error timestamp in UTC")


# Utility function for date generation


def generate_date_list(start_date: str, end_date: str) -> List[str]:
    """
    Generate list of dates between start and end (inclusive).

    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format

    Returns:
        List of dates in YYYYMMDD format
    """
    # Parse dates
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")

    # Generate date list
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    return dates
