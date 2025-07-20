"""
Pydantic models for data processing API requests and responses.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel, Field, field_validator
from betedge_data.manager.utils import generate_month_list, generate_trading_date_list
from betedge_data.historical.option.models import HistOptionBulkRequest
from betedge_data.historical.stock.models import HistStockRequest
from betedge_data.alternative.earnings.models import EarningsRequest
from betedge_data.historical.option.client import HistoricalOptionClient
from betedge_data.historical.stock.client import HistoricalStockClient
from betedge_data.alternative.earnings.client import EarningsClient
from betedge_data.common.interface import IClient, IRequest


class ExternalBaseRequest(BaseModel, ABC):
    """Abstract base class for all data processing requests."""

    @abstractmethod
    def get_subrequests(self) -> List[IRequest]:
        """
        Generate request objects that can be passed the ThetaData API.

        Returns:
            List of Request objects
        """
        pass

    @abstractmethod
    def get_client(self) -> IClient:
        """
        Get the client for this request type.
        """
        pass


class ExternalHistoricalOptionRequest(ExternalBaseRequest):
    """Request model for historical option data endpoint."""

    root: str = Field(..., description="Option root symbol (e.g., 'AAPL')")
    start_date: str = Field(..., description="Start date in YYYYMMDD format", pattern=r"^\d{8}$")
    end_date: str = Field(..., description="End date in YYYYMMDD format", pattern=r"^\d{8}$")
    schema: str = Field(default="quote", description="Data schema type: options include 'quote', 'eod'")
    interval: int = Field(900_000, description="Interval in milliseconds (default 900000 for 15 minutes)", ge=0)
    return_format: str = Field("parquet", description="Return format for the request, either parquet or ipc.")
    start_time: Optional[int] = Field(None, description="Start time in milliseconds since midnight ET", ge=0)
    end_time: Optional[int] = Field(None, description="End time in milliseconds since midnight ET", ge=0)

    # Pre-computed requests attribute
    requests: List[HistOptionBulkRequest] = Field(default_factory=list, init=False, repr=False)

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_date_format(cls, v):
        """Validate date format is YYYYMMDD."""
        try:
            datetime.strptime(v, "%Y%m%d")
        except ValueError:
            raise ValueError(f"Invalid date format: {v}. Expected YYYYMMDD format")
        return v

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, v, info):
        """Validate that start_date <= end_date."""
        start_date = info.data.get("start_date")
        if start_date and v < start_date:
            raise ValueError("end_date must be >= start_date")
        return v

    @field_validator("schema")
    @classmethod
    def validate_schema(cls, v: str) -> str:
        """Validate schema is supported."""
        if v not in ["quote", "eod"]:
            raise ValueError(f"schema must be 'quote', 'ohlc', or 'eod', got '{v}'")
        return v

    def __init__(self, **data):
        """Initialize and pre-compute the requests list."""
        super().__init__(**data)
        self.requests = self._compute_requests()

    def _compute_requests(self) -> List[HistOptionBulkRequest]:
        """Compute the list of requests."""
        if self.schema == "eod":
            # For EOD, create one request per month (due to option data size)
            start_yearmo = int(self.start_date[:6])  # YYYYMM from YYYYMMDD
            end_yearmo = int(self.end_date[:6])

            requests = []
            current_yearmo = start_yearmo
            while current_yearmo <= end_yearmo:
                # Create EOD request using yearmo parameter
                year = current_yearmo // 100
                month = current_yearmo % 100
                requests.append(
                    HistOptionBulkRequest(
                        root=self.root,
                        yearmo=current_yearmo,
                        interval=self.interval,  # Not used for EOD
                        return_format=self.return_format,
                        schema=self.schema,
                    )
                )
                # Increment to next month
                if month == 12:
                    current_yearmo = (year + 1) * 100 + 1
                else:
                    current_yearmo = year * 100 + (month + 1)
            return requests
        else:
            # For regular endpoints, use trading days only
            dates = generate_trading_date_list(self.start_date, self.end_date)
            return [
                HistOptionBulkRequest(
                    root=self.root,
                    date=date,
                    interval=self.interval,
                    return_format=self.return_format,
                    schema=self.schema,
                )
                for date in dates
            ]

    def get_subrequests(self) -> List[HistOptionBulkRequest]:
        """
        Return pre-computed requests.

        For regular endpoints: one request per trading day.
        For EOD endpoint: one request per month.

        Returns:
            List of HistOptionBulkRequest objects
        """
        return self.requests

    def get_client(self):
        """Return HistoricalOptionClient instance for this request type."""
        return HistoricalOptionClient()


class ExternalHistoricalStockRequest(ExternalBaseRequest):
    """Request model for historical stock data endpoint."""

    root: str = Field(..., description="Stock symbol (e.g., 'AAPL')")
    start_date: str = Field(..., description="Start date in YYYYMMDD format", pattern=r"^\d{8}$")
    end_date: str = Field(..., description="End date in YYYYMMDD format", pattern=r"^\d{8}$")
    schema: str = Field(default="quote", description="Data schema type: options include 'quote', 'ohlc', 'eod'")
    interval: int = Field(60_000, description="Interval in milliseconds (default 60000 for 1 minute)", ge=0)
    return_format: str = Field("parquet", description="Return format for the request, either parquet or ipc.")
    start_time: Optional[int] = Field(None, description="Start time in milliseconds since midnight ET", ge=0)
    end_time: Optional[int] = Field(None, description="End time in milliseconds since midnight ET", ge=0)

    # Pre-computed requests attribute
    requests: List[HistStockRequest] = Field(default_factory=list, init=False, repr=False)

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_date_format(cls, v):
        """Validate date format is YYYYMMDD."""
        try:
            datetime.strptime(v, "%Y%m%d")
        except ValueError:
            raise ValueError(f"Invalid date format: {v}. Expected YYYYMMDD format")
        return v

    @field_validator("schema")
    @classmethod
    def validate_schema(cls, v: str) -> str:
        """Validate schema is supported."""
        if v not in ["quote", "ohlc", "eod"]:
            raise ValueError(f"schema must be 'quote', 'ohlc', or 'eod', got '{v}'")
        return v

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, v, info):
        """Validate that start_date <= end_date."""
        start_date = info.data.get("start_date")
        if start_date and v < start_date:
            raise ValueError("end_date must be >= start_date")
        return v

    def __init__(self, **data):
        """Initialize and pre-compute the requests list."""
        super().__init__(**data)
        self.requests = self._compute_requests()

    def _compute_requests(self) -> List[HistStockRequest]:
        """Compute the list of requests."""
        if self.schema == "eod":
            # For EOD, create one request per year to leverage yearly aggregation
            start_year = int(self.start_date[:4])
            end_year = int(self.end_date[:4])

            requests = []
            for year in range(start_year, end_year + 1):
                # Use Jan 1st of each year as the date for EOD requests
                requests.append(
                    HistStockRequest(
                        root=self.root,
                        date=int(f"{year}0101"),
                        interval=self.interval,  # Not used for EOD but required by model
                        return_format=self.return_format,
                        schema=self.schema,
                    )
                )
            return requests
        else:
            # For regular endpoints, use trading days only
            dates = generate_trading_date_list(self.start_date, self.end_date)
            return [
                HistStockRequest(
                    root=self.root,
                    date=date,
                    interval=self.interval,
                    return_format=self.return_format,
                    schema=self.schema,
                )
                for date in dates
            ]

    def get_subrequests(self) -> List[HistStockRequest]:
        """
        Return pre-computed requests.

        For regular endpoints: one request per trading day.
        For EOD endpoint: one request per year.

        Returns:
            List of HistStockRequest objects
        """
        return self.requests

    def get_client(self):
        """Return HistoricalStockClient instance for this request type."""
        return HistoricalStockClient()


class ExternalEarningsRequest(ExternalBaseRequest):
    """Request model for earnings data endpoint."""

    start_date: str = Field(..., description="Start date in YYYYMM format", pattern=r"^\d{6}$")
    end_date: str = Field(..., description="End date in YYYYMM format", pattern=r"^\d{6}$")
    return_format: str = Field("parquet", description="Return format (currently only parquet supported)")

    # Pre-computed requests attribute
    requests: List[EarningsRequest] = Field(default_factory=list, init=False, repr=False)

    @field_validator("end_date")
    @classmethod
    def validate_date_range(cls, v, info):
        """Validate that start_date <= end_date."""
        start_date = info.data.get("start_date")
        if start_date and v < start_date:
            raise ValueError("end_date must be >= start_date")
        return v

    @field_validator("return_format")
    @classmethod
    def validate_return_format(cls, v):
        """Validate return format is supported."""
        if v.lower() != "parquet":
            raise ValueError("Only 'parquet' format is currently supported")
        return v.lower()

    @field_validator("start_date", "end_date")
    @classmethod
    def validate_date_format(cls, v):
        """Validate date format is YYYYMM."""
        try:
            datetime.strptime(v, "%Y%m")
        except ValueError:
            raise ValueError(f"Invalid date format: {v}. Expected YYYYMM format")
        return v

    def __init__(self, **data):
        """Initialize and pre-compute the requests list."""
        super().__init__(**data)
        self.requests = self._compute_requests()

    def _compute_requests(self) -> List[EarningsRequest]:
        """Compute the list of requests."""
        dates = generate_month_list(self.start_date, self.end_date)
        return [EarningsRequest(year=date[0], month=date[1], return_format=self.return_format) for date in dates]

    def get_subrequests(self) -> List[EarningsRequest]:
        """
        Return pre-computed requests.

        Returns:
            List of EarningsRequest objects
        """
        return self.requests

    def get_client(self):
        """Return EarningsClient instance for this request type."""
        return EarningsClient()
