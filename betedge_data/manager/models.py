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
    endpoint: str = Field(default="quote", description="Endpoint to map to: options include 'quote', 'ohlc'")
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

    def __init__(self, **data):
        """Initialize and pre-compute the requests list."""
        super().__init__(**data)
        self.requests = self._compute_requests()

    def _compute_requests(self) -> List[HistOptionBulkRequest]:
        """Compute the list of requests for trading days only."""
        dates = generate_trading_date_list(self.start_date, self.end_date)
        return [
            HistOptionBulkRequest(
                root=self.root,
                date=date,
                interval=self.interval,
                return_format=self.return_format,
                endpoint=self.endpoint,
            )
            for date in dates
        ]

    def get_subrequests(self) -> List[HistOptionBulkRequest]:
        """
        Return pre-computed requests for trading days only.

        Returns:
            List of HistOptionBulkRequest objects for trading days
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
    endpoint: str = Field(default="quote", description="Endpoint to map to: options include 'quote', 'ohlc'")
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
        """Compute the list of requests for trading days only."""
        dates = generate_trading_date_list(self.start_date, self.end_date)
        return [
            HistStockRequest(
                root=self.root,
                date=date,
                interval=self.interval,
                return_format=self.return_format,
                endpoint=self.endpoint,
            )
            for date in dates
        ]

    def get_subrequests(self) -> List[HistStockRequest]:
        """
        Return pre-computed requests for trading days only.

        Returns:
            List of HistStockRequest objects for trading days
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
        return [
            EarningsRequest(year=date[0], month=date[1], return_format=self.return_format) 
            for date in dates
        ]

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
