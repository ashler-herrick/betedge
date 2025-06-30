"""
Common test fixtures for unit and integration tests.
"""

import io
from typing import Dict, List, Any
from datetime import datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq


class TestDataGenerator:
    """Generate test data for various components."""

    @staticmethod
    def generate_tick_data(
        num_ticks: int = 100,
        root: str = "AAPL",
        date: str = "20231117",
        start_ms: int = 34200000,  # 9:30 AM
        interval_ms: int = 60000,  # 1 minute
    ) -> List[List[Any]]:
        """Generate test tick data."""
        ticks = []
        for i in range(num_ticks):
            ms_of_day = start_ms + (i * interval_ms)
            base_price = 150.0 + (i * 0.01)  # Slight upward trend

            tick = [
                ms_of_day,
                100 + (i % 50),  # bid_size
                47,  # bid_exchange
                base_price - 0.05,  # bid
                50,  # bid_condition
                100 + (i % 50),  # ask_size
                47,  # ask_exchange
                base_price + 0.05,  # ask
                50,  # ask_condition
                int(date),
            ]
            ticks.append(tick)

        return ticks

    @staticmethod
    def generate_option_response(
        num_contracts: int = 5, ticks_per_contract: int = 10, root: str = "AAPL", expiration: int = 20231117
    ) -> Dict[str, Any]:
        """Generate test option response data."""
        contracts = []

        for i in range(num_contracts):
            strike = 1500000 + (i * 50000)  # $150, $155, $160, etc.
            right = "C" if i % 2 == 0 else "P"

            ticks = TestDataGenerator.generate_tick_data(num_ticks=ticks_per_contract, root=root)

            contract = {
                "ticks": ticks,
                "contract": {"root": root, "expiration": expiration, "strike": strike, "right": right},
            }
            contracts.append(contract)

        return {
            "header": {
                "latency_ms": 100,
                "format": [
                    "ms_of_day",
                    "bid_size",
                    "bid_exchange",
                    "bid",
                    "bid_condition",
                    "ask_size",
                    "ask_exchange",
                    "ask",
                    "ask_condition",
                    "date",
                ],
            },
            "response": contracts,
        }

    @staticmethod
    def generate_parquet_buffer(num_rows: int = 1000, schema: str = "quote") -> io.BytesIO:
        """Generate a Parquet file buffer with test data."""
        if schema == "quote":
            data = {
                "ms_of_day": list(range(34200000, 34200000 + num_rows * 1000, 1000)),
                "bid_price": [149.0 + (i * 0.01) for i in range(num_rows)],
                "ask_price": [149.1 + (i * 0.01) for i in range(num_rows)],
                "bid_size": [100 + (i % 50) for i in range(num_rows)],
                "ask_size": [100 + (i % 50) for i in range(num_rows)],
                "root": ["AAPL"] * num_rows,
                "expiration": [20231117] * num_rows,
                "strike": [1500000] * num_rows,
                "right": ["C"] * num_rows,
            }
        elif schema == "ohlc":
            data = {
                "ms_of_day": list(range(34200000, 34200000 + num_rows * 60000, 60000)),
                "open": [149.0 + (i * 0.01) for i in range(num_rows)],
                "high": [149.5 + (i * 0.01) for i in range(num_rows)],
                "low": [148.5 + (i * 0.01) for i in range(num_rows)],
                "close": [149.2 + (i * 0.01) for i in range(num_rows)],
                "volume": [10000 + (i * 100) for i in range(num_rows)],
                "root": ["AAPL"] * num_rows,
                "date": [20231117] * num_rows,
            }
        else:
            raise ValueError(f"Unknown schema: {schema}")

        table = pa.table(data)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        return buffer

    @staticmethod
    def generate_date_range(start_date: str, end_date: str) -> List[str]:
        """Generate a list of dates between start and end (inclusive)."""
        start = datetime.strptime(start_date, "%Y%m%d")
        end = datetime.strptime(end_date, "%Y%m%d")

        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime("%Y%m%d"))
            current += timedelta(days=1)

        return dates


class MinIOTestHelper:
    """Helper class for MinIO-related tests."""

    @staticmethod
    def create_test_objects(num_objects: int = 10) -> List[Dict[str, Any]]:
        """Create test MinIO object metadata."""
        objects = []

        for i in range(num_objects):
            date = datetime(2023, 11, 13) + timedelta(days=i)
            obj = {
                "object_name": f"historical-options/quote/AAPL/{date.year}/{date.month:02d}/{date.day:02d}/15m/all/filtered/data.parquet",
                "size": 1024 * (i + 1),
                "last_modified": date,
                "etag": f"etag-{i}",
            }
            objects.append(obj)

        return objects

    @staticmethod
    def parse_object_path(path: str) -> Dict[str, str]:
        """Parse MinIO object path to extract metadata."""
        parts = path.split("/")
        if len(parts) >= 9 and parts[0] == "historical-options":
            return {
                "schema": parts[1],
                "root": parts[2],
                "year": parts[3],
                "month": parts[4],
                "day": parts[5],
                "interval": parts[6],
                "exp": parts[7],
                "filter_type": parts[8],
                "filename": parts[9] if len(parts) > 9 else None,
            }
        return {}


class RequestBuilder:
    """Build test request objects."""

    @staticmethod
    def build_historical_option_request(**kwargs) -> Dict[str, Any]:
        """Build a historical option request with defaults."""
        defaults = {
            "root": "AAPL",
            "start_date": "20231113",
            "end_date": "20231117",
            "exp": 0,
            "max_dte": 30,
            "base_pct": 0.1,
            "interval": 900000,
        }
        defaults.update(kwargs)
        return defaults

    @staticmethod
    def build_historical_stock_request(**kwargs) -> Dict[str, Any]:
        """Build a historical stock request with defaults."""
        defaults = {
            "ticker": "AAPL",
            "start_date": "20231113",
            "end_date": "20231117",
            "data_type": "ohlc",
            "interval": 60000,
            "rth": True,
        }
        defaults.update(kwargs)
        return defaults
