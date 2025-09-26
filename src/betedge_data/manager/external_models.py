"""
Request models for BetEdge data processing client.

This module provides Pydantic models for requesting historical options, stocks,
and earnings data through the BetEdge data processing system. These models handle
validation, date range processing, and automatic request generation.

Example Usage:
    ```python
    from betedge_data.manager.external_models import (
        ExternalHistoricalOptionRequest,
        ExternalHistoricalStockRequest,
        ExternalEarningsRequest
    )
    
    # Request historical option data
    option_request = ExternalHistoricalOptionRequest(
        root="AAPL",
        start_date="20240101",
        end_date="20240131", 
        data_schema="quote",
        interval=60000  # 1 minute
    )
    
    # Request historical stock data  
    stock_request = ExternalHistoricalStockRequest(
        root="AAPL",
        start_date="20240101",
        end_date="20240131",
        data_schema="ohlc"
    )
    
    # Request earnings data
    earnings_request = ExternalEarningsRequest(
        start_date="202401",
        end_date="202403"
    )
    ```
"""

from datetime import datetime
from typing import Optional, List, Sequence

from pydantic import BaseModel, Field, field_validator
from betedge_data.manager.utils import generate_month_list, generate_trading_date_list
from betedge_data.historical.option.hist_option_bulk_request import HistOptionBulkRequest
from betedge_data.historical.stock.hist_stock_request import HistStockRequest
from betedge_data.alternative.earnings.earnings_request import EarningsRequest
from betedge_data.historical.option.option_client import HistoricalOptionClient
from betedge_data.historical.stock.stock_client import HistoricalStockClient
from betedge_data.alternative.earnings.earnings_client import EarningsClient

Client = HistoricalOptionClient | HistoricalStockClient | EarningsClient
Request = HistOptionBulkRequest | HistStockRequest | EarningsRequest


class ExternalBaseRequest(BaseModel):
    """
    Base class for all data processing requests.
    
    This abstract base class provides common functionality for all external request types
    including force refresh capabilities and standardized interfaces for generating
    subrequests and obtaining appropriate clients.
    
    Attributes:
        force_refresh: When True, existing files in storage will be reprocessed and 
                      overwritten. When False (default), existing files are skipped
                      to avoid redundant processing.
    
    Abstract Methods:
        get_subrequests(): Must return a sequence of individual API request objects
                          that will be processed to fulfill this external request.
        
        get_client(): Must return the appropriate client instance for processing
                     this request type (e.g., HistoricalOptionClient, HistoricalStockClient).
    
    Note:
        This class should not be instantiated directly. Use one of the concrete
        subclasses like ExternalHistoricalOptionRequest, ExternalHistoricalStockRequest,
        or ExternalEarningsRequest.
    """

    force_refresh: bool = Field(False, description="If True, reprocess existing files (overwrite)")

    def get_subrequests(self) -> Sequence[Request]:
        """
        Generate request objects that can be passed the ThetaData API.

        Returns:
            Sequence of Request objects
        """
        raise NotImplementedError("Subclasses must implement get_subrequests")

    def get_client(self) -> Client:
        """
        Get the client for this request type.
        """
        raise NotImplementedError("Subclasses must implement get_client")


class ExternalHistoricalOptionRequest(ExternalBaseRequest):
    """
    Request model for historical options data.
    
    This class handles requests for historical options data across date ranges,
    automatically splitting large requests into optimal chunks and handling
    different data schemas with appropriate aggregation strategies.
    
    Data Schema Behaviors:
        - 'quote': Tick-by-tick quote data, split by trading days
        - 'eod': End-of-day aggregated data, split by months for efficiency
    
    Date Processing:
        - Uses only trading days (excludes weekends/holidays) for quote data
        - For EOD data, aggregates by month due to large data volumes
        - Automatically converts YYYYMMDD date strings to appropriate ranges
    
    Example Usage:
        ```python
        # Request intraday options quotes
        request = ExternalHistoricalOptionRequest(
            root="AAPL",
            start_date="20240101", 
            end_date="20240131",
            data_schema="quote",
            interval=900000,  # 15 minutes in milliseconds
            force_refresh=False
        )
        
        # Request end-of-day options data  
        eod_request = ExternalHistoricalOptionRequest(
            root="TSLA",
            start_date="20240101",
            end_date="20240331", 
            data_schema="eod"
        )
        ```
    
    Args:
        root: Option root symbol (e.g., 'AAPL', 'TSLA', 'SPY')
        start_date: Start date in YYYYMMDD format (e.g., '20240101')
        end_date: End date in YYYYMMDD format (e.g., '20240131')  
        data_schema: Data type - 'quote' for tick data or 'eod' for end-of-day
        interval: Time interval in milliseconds (default: 900000 = 15 minutes)
        return_format: File format, 'parquet' or 'ipc' (default: 'parquet')
        start_time: Optional start time filter in ms since midnight ET
        end_time: Optional end time filter in ms since midnight ET
        force_refresh: Whether to reprocess existing files (default: False)
    
    Note:
        The request is automatically split into multiple subrequests:
        - Quote data: One request per trading day
        - EOD data: One request per month
        This optimization balances API efficiency with data volume management.
    """

    root: str = Field(..., description="Option root symbol (e.g., 'AAPL')")
    start_date: str = Field(..., description="Start date in YYYYMMDD format", pattern=r"^\d{8}$")
    end_date: str = Field(..., description="End date in YYYYMMDD format", pattern=r"^\d{8}$")
    data_schema: str = Field(..., description="Data schema type: options include 'quote', 'eod'")
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

    @field_validator("data_schema")
    @classmethod
    def validate_data_schema(cls, v: str) -> str:
        """Validate data_schema is supported."""
        if v not in ["quote", "eod"]:
            raise ValueError(f"data_schema must be 'quote', 'ohlc', or 'eod', got '{v}'")
        return v

    def __init__(self, **data):
        """Initialize and pre-compute the requests list."""
        super().__init__(**data)
        self.requests = self._compute_requests()

    def _compute_requests(self) -> List[HistOptionBulkRequest]:
        """Compute the list of requests."""
        if self.data_schema == "eod":
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
                        date=None,  # Explicitly None for EOD (uses yearmo instead)
                        yearmo=current_yearmo,
                        interval=self.interval,  # Not used for EOD
                        return_format=self.return_format,
                        data_schema=self.data_schema,
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
                    yearmo=None,  # Explicitly None for regular requests (uses date instead)
                    interval=self.interval,
                    return_format=self.return_format,
                    data_schema=self.data_schema,
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
    """
    Request model for historical stock data.
    
    This class handles requests for historical stock data across date ranges,
    automatically splitting large requests into optimal chunks based on the
    data schema type and implementing efficient aggregation strategies.
    
    Data Schema Types:
        - 'quote': Tick-by-tick quote data with bid/ask/size information
        - 'ohlc': Open, High, Low, Close price data at specified intervals  
        - 'eod': End-of-day aggregated data with daily OHLC and volume
    
    Date Processing Strategy:
        - Quote/OHLC data: Split by individual trading days for granular control
        - EOD data: Aggregated by year for efficient bulk processing
        - Automatically excludes weekends and market holidays
    
    Example Usage:
        ```python
        # Request intraday stock quotes (1-minute intervals)
        quote_request = ExternalHistoricalStockRequest(
            root="AAPL",
            start_date="20240101",
            end_date="20240131", 
            data_schema="quote",
            interval=60000,  # 1 minute in milliseconds
        )
        
        # Request OHLC bars (5-minute intervals)
        ohlc_request = ExternalHistoricalStockRequest(
            root="TSLA",
            start_date="20240201",
            end_date="20240229",
            data_schema="ohlc", 
            interval=300000,  # 5 minutes
            start_time=34200000,  # 9:30 AM ET in ms
            end_time=57600000     # 4:00 PM ET in ms
        )
        
        # Request end-of-day data (multi-year range)
        eod_request = ExternalHistoricalStockRequest(
            root="SPY",
            start_date="20220101",
            end_date="20240131",
            data_schema="eod"
        )
        ```
    
    Args:
        root: Stock symbol (e.g., 'AAPL', 'TSLA', 'SPY')  
        start_date: Start date in YYYYMMDD format (e.g., '20240101')
        end_date: End date in YYYYMMDD format (e.g., '20240131')
        data_schema: Data type - 'quote', 'ohlc', or 'eod' (default: 'quote')
        interval: Time interval in milliseconds (default: 60000 = 1 minute)
        return_format: File format, 'parquet' or 'ipc' (default: 'parquet')
        start_time: Optional start time filter in ms since midnight ET
        end_time: Optional end time filter in ms since midnight ET
        force_refresh: Whether to reprocess existing files (default: False)
        
    Request Splitting:
        - Quote/OHLC: One request per trading day (optimal for API limits)
        - EOD: One request per year (leverages bulk data efficiency)
        
    Note:
        The interval parameter is ignored for EOD data schema as it provides
        daily aggregates. Time filters (start_time/end_time) only apply to
        intraday data schemas (quote/ohlc).
    """

    root: str = Field(..., description="Stock symbol (e.g., 'AAPL')")
    start_date: str = Field(..., description="Start date in YYYYMMDD format", pattern=r"^\d{8}$")
    end_date: str = Field(..., description="End date in YYYYMMDD format", pattern=r"^\d{8}$")
    data_schema: str = Field(
        default="quote", description="Data data_schema type: options include 'quote', 'ohlc', 'eod'"
    )
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

    @field_validator("data_schema")
    @classmethod
    def validate_data_schema(cls, v: str) -> str:
        """Validate data_schema is supported."""
        if v not in ["quote", "ohlc", "eod"]:
            raise ValueError(f"data_schema must be 'quote', 'ohlc', or 'eod', got '{v}'")
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
        if self.data_schema == "eod":
            # For EOD, create one request per year to leverage yearly aggregation
            start_year = int(self.start_date[:4])
            end_year = int(self.end_date[:4])

            requests = []
            for year in range(start_year, end_year + 1):
                # Use Jan 1st of each year as the date for EOD requests
                requests.append(
                    HistStockRequest(
                        root=self.root,
                        date=None,  # Explicitly None for EOD (uses year instead)
                        year=year,
                        data_schema=self.data_schema,
                        interval=self.interval,  # Not used for EOD but required by model
                        return_format=self.return_format,
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
                    year=None,  # Explicitly None for regular requests (uses date instead)
                    interval=self.interval,
                    return_format=self.return_format,
                    data_schema=self.data_schema,
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
    """
    Request model for earnings data.
    
    This class handles requests for earnings data across monthly date ranges.
    Earnings data includes company financial reporting information, earnings
    announcements, and related fundamental data aggregated by month.
    
    Data Characteristics:
        - Monthly aggregation of earnings announcements and financial reports
        - Covers all publicly traded companies with available earnings data
        - Includes earnings dates, estimates, actual results, and surprises
        - Currently only available in Parquet format
    
    Date Processing:
        - Uses YYYYMM format for monthly granularity (e.g., '202401' for January 2024)
        - Automatically generates one request per month in the specified range
        - Includes all trading days within each month period
    
    Example Usage:
        ```python
        # Request single month of earnings data
        single_month = ExternalEarningsRequest(
            start_date="202401",  # January 2024
            end_date="202401"
        )
        
        # Request quarterly earnings data
        quarterly_request = ExternalEarningsRequest(
            start_date="202401",  # Q1 2024
            end_date="202403",
            force_refresh=True  # Reprocess existing files
        )
        
        # Request full year of earnings data  
        annual_request = ExternalEarningsRequest(
            start_date="202301",  # Full year 2023
            end_date="202312"
        )
        ```
    
    Args:
        start_date: Start month in YYYYMM format (e.g., '202401' for January 2024)
        end_date: End month in YYYYMM format (e.g., '202403' for March 2024)
        return_format: File format - currently only 'parquet' is supported
        force_refresh: Whether to reprocess existing files (default: False)
    
    Request Splitting:
        Each month in the date range generates one individual request, allowing
        for efficient parallel processing and granular progress tracking.
    
    Note:
        Unlike stock and options data, earnings data does not support:
        - Interval specifications (data is inherently event-based)
        - Time filtering (events occur throughout trading days)  
        - Alternative return formats (only Parquet currently supported)
        
    The date format differs from stock/options requests (YYYYMM vs YYYYMMDD)
    to reflect the monthly aggregation nature of earnings data.
    """

    start_date: str = Field(..., description="Start date in YYYYMM format", pattern=r"^\d{6}$")
    end_date: str = Field(..., description="End date in YYYYMM format", pattern=r"^\d{6}$")
    return_format: str = Field("parquet", description="Return format (currently only parquet supported)")

    # Pre-computed requests attribute
    requests: List[EarningsRequest] = Field(default_factory=list, init=False, repr=False)

    def __init__(self, **data):
        """Initialize and pre-compute the requests list."""
        super().__init__(**data)
        self.requests = self._compute_requests()

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
