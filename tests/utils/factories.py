"""
Test data factories for creating complex test objects.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import random
import string

from betedge_data.manager.models import (
    HistoricalOptionRequest,
    DataProcessingResponse,
    ErrorResponse,
)
from betedge_data.storage.config import MinIOPublishConfig


class HistoricalRequestFactory:
    """Factory for creating historical request objects."""

    @staticmethod
    def create_option_request(
        root: str = "AAPL", days_back: int = 5, exp: int = 0, **kwargs
    ) -> HistoricalOptionRequest:
        """Create a historical option request."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        defaults = {
            "root": root,
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d"),
            "exp": exp,
            "max_dte": 30,
            "base_pct": 0.1,
            "interval": 900000,  # 15 minutes
            "start_time": None,
            "end_time": None,
        }
        defaults.update(kwargs)

        return HistoricalOptionRequest(**defaults)

    @staticmethod
    def create_stock_request(ticker: str = "AAPL", days_back: int = 5, **kwargs) -> Dict[str, Any]:
        """Create a historical stock request (as dict since no model exists)."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)

        defaults = {
            "ticker": ticker,
            "start_date": start_date.strftime("%Y%m%d"),
            "end_date": end_date.strftime("%Y%m%d"),
            "data_type": "ohlc",
            "interval": 60000,  # 1 minute
            "rth": True,
            "start_time": None,
            "end_time": None,
        }
        defaults.update(kwargs)

        return defaults

    @staticmethod
    def create_batch_requests(symbols: List[str], request_type: str = "option", **kwargs) -> List[Any]:
        """Create multiple requests for different symbols."""
        requests = []

        for symbol in symbols:
            if request_type == "option":
                request = HistoricalRequestFactory.create_option_request(root=symbol, **kwargs)
            else:
                request = HistoricalRequestFactory.create_stock_request(ticker=symbol, **kwargs)
            requests.append(request)

        return requests


class ResponseFactory:
    """Factory for creating API response objects."""

    @staticmethod
    def create_success_response(
        request_id: str = None, records_count: int = 1000, processing_time_ms: int = 1500
    ) -> DataProcessingResponse:
        """Create a successful processing response."""
        if request_id is None:
            request_id = ResponseFactory._generate_uuid()

        return DataProcessingResponse(
            status="success",
            request_id=request_id,
            message="Successfully processed option data",
            data_type="parquet",
            storage_location="s3://test-bucket/historical-options/quote/AAPL/",
            records_count=records_count,
            processing_time_ms=processing_time_ms,
        )

    @staticmethod
    def create_error_response(
        request_id: str = None, error_code: str = "PROCESSING_ERROR", message: str = "Test error message"
    ) -> ErrorResponse:
        """Create an error response."""
        if request_id is None:
            request_id = ResponseFactory._generate_uuid()

        return ErrorResponse(
            request_id=request_id, error_code=error_code, message=message, details="Test error details"
        )

    @staticmethod
    def _generate_uuid() -> str:
        """Generate a test UUID."""
        import uuid

        return str(uuid.uuid4())


class MinIOObjectFactory:
    """Factory for creating MinIO-related test objects."""

    @staticmethod
    def create_publish_config(
        root: str = "AAPL", date: str = None, interval: int = 900000, **kwargs
    ) -> MinIOPublishConfig:
        """Create a MinIO publish configuration."""
        if date is None:
            date = datetime.now().strftime("%Y%m%d")

        defaults = {
            "schema": "quote",
            "root": root,
            "date": date,
            "interval": interval,
            "exp": "0",
            "filter_type": "filtered",
        }
        defaults.update(kwargs)

        return MinIOPublishConfig(**defaults)

    @staticmethod
    def create_object_list(
        num_objects: int, root: str = "AAPL", schema: str = "quote", start_date: str = "20231113"
    ) -> List[Dict[str, Any]]:
        """Create a list of MinIO object metadata."""
        objects = []
        start = datetime.strptime(start_date, "%Y%m%d")

        for i in range(num_objects):
            date = start + timedelta(days=i)

            # Vary intervals and expirations
            intervals = ["15m", "30m", "1h", "1d"]
            expirations = ["all", "20231117", "20231124", "20231201"]

            obj_path = f"historical-options/{schema}/{root}/{date.year}/{date.month:02d}/{date.day:02d}/{random.choice(intervals)}/{random.choice(expirations)}/filtered/data.parquet"

            obj = {
                "object_name": obj_path,
                "size": random.randint(1024, 1024 * 1024),  # 1KB to 1MB
                "last_modified": date,
                "etag": f"etag-{i}-{''.join(random.choices(string.ascii_lowercase, k=8))}",
            }
            objects.append(obj)

        return objects


class ParquetDataFactory:
    """Factory for creating test Parquet data."""

    @staticmethod
    def create_quote_data(num_rows: int = 100, root: str = "AAPL", date: int = 20231117) -> Dict[str, List[Any]]:
        """Create quote data for Parquet tables."""
        base_price = 150.0
        data = {
            "ms_of_day": [],
            "bid_price": [],
            "ask_price": [],
            "bid_size": [],
            "ask_size": [],
            "root": [],
            "expiration": [],
            "strike": [],
            "right": [],
        }

        for i in range(num_rows):
            ms_of_day = 34200000 + (i * 1000)  # 1 second intervals
            price_drift = random.uniform(-0.5, 0.5)

            data["ms_of_day"].append(ms_of_day)
            data["bid_price"].append(base_price + price_drift - 0.05)
            data["ask_price"].append(base_price + price_drift + 0.05)
            data["bid_size"].append(random.randint(1, 1000))
            data["ask_size"].append(random.randint(1, 1000))
            data["root"].append(root)
            data["expiration"].append(date)
            data["strike"].append(1500000)  # $150 strike
            data["right"].append("C")

        return data

    @staticmethod
    def create_ohlc_data(
        num_rows: int = 100, root: str = "AAPL", date: int = 20231117, interval_ms: int = 60000
    ) -> Dict[str, List[Any]]:
        """Create OHLC data for Parquet tables."""
        base_price = 150.0
        data = {"ms_of_day": [], "open": [], "high": [], "low": [], "close": [], "volume": [], "root": [], "date": []}

        for i in range(num_rows):
            ms_of_day = 34200000 + (i * interval_ms)

            # Generate realistic OHLC data
            open_price = base_price + random.uniform(-1, 1)
            close_price = open_price + random.uniform(-0.5, 0.5)
            high_price = max(open_price, close_price) + random.uniform(0, 0.3)
            low_price = min(open_price, close_price) - random.uniform(0, 0.3)

            data["ms_of_day"].append(ms_of_day)
            data["open"].append(open_price)
            data["high"].append(high_price)
            data["low"].append(low_price)
            data["close"].append(close_price)
            data["volume"].append(random.randint(10000, 1000000))
            data["root"].append(root)
            data["date"].append(date)

            # Update base price for next candle
            base_price = close_price

        return data
