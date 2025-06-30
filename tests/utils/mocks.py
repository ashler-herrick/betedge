"""
Mock implementations for external services and dependencies.
"""

import io
import json
from typing import Dict, List, Any, Optional
from unittest.mock import MagicMock
from datetime import datetime

from minio.error import S3Error


class MockMinIOClient:
    """Mock MinIO client with configurable responses."""

    def __init__(
        self, bucket_exists: bool = True, objects: Optional[Dict[str, bytes]] = None, raise_errors: bool = False
    ):
        """
        Initialize mock MinIO client.

        Args:
            bucket_exists: Whether bucket exists
            objects: Dict of object_name -> content mapping
            raise_errors: Whether to raise S3Error on operations
        """
        self.bucket_exists_value = bucket_exists
        self.objects = objects or {}
        self.raise_errors = raise_errors
        self.uploaded_objects = {}
        self.deleted_objects = set()

    def bucket_exists(self, bucket_name: str) -> bool:
        """Mock bucket_exists method."""
        if self.raise_errors:
            raise S3Error("Test error", "test", "test", "test", "test", None)
        return self.bucket_exists_value

    def make_bucket(self, bucket_name: str, location: str = None):
        """Mock make_bucket method."""
        if self.raise_errors:
            raise S3Error("Test error", "test", "test", "test", "test", None)
        self.bucket_exists_value = True

    def put_object(
        self,
        bucket_name: str,
        object_name: str,
        data: io.BytesIO,
        length: int,
        content_type: str = None,
        metadata: Dict = None,
    ):
        """Mock put_object method."""
        if self.raise_errors:
            raise S3Error("Test error", "test", "test", "test", "test", None)

        # Store uploaded data
        data.seek(0)
        self.uploaded_objects[object_name] = {
            "data": data.read(),
            "metadata": metadata,
            "content_type": content_type,
            "length": length,
        }

        # Return mock result
        result = MagicMock()
        result.etag = f"etag-{len(self.uploaded_objects)}"
        return result

    def stat_object(self, bucket_name: str, object_name: str):
        """Mock stat_object method."""
        if self.raise_errors:
            raise S3Error("Test error", "test", "test", "test", "test", None)

        if object_name in self.objects or object_name in self.uploaded_objects:
            result = MagicMock()
            result.size = len(self.objects.get(object_name, b""))
            result.etag = f"etag-{object_name}"
            result.last_modified = datetime.now()
            return result
        else:
            # Use S3Error constructor with proper parameters
            raise S3Error("Object not found", "NoSuchKey", "test", "test", "test", None)

    def list_objects(self, bucket_name: str, prefix: str = None, recursive: bool = False):
        """Mock list_objects method."""
        if self.raise_errors:
            raise S3Error("Test error", "test", "test", "test", "test", None)

        # Filter objects by prefix
        matching_objects = []
        all_objects = list(self.objects.keys()) + list(self.uploaded_objects.keys())

        for obj_name in all_objects:
            if not prefix or obj_name.startswith(prefix):
                if not recursive and prefix:
                    # Non-recursive: only return direct children
                    relative_path = obj_name[len(prefix) :]
                    if "/" in relative_path.strip("/"):
                        continue

                obj = MagicMock()
                obj.object_name = obj_name
                obj.size = len(self.objects.get(obj_name, b""))
                obj.last_modified = datetime.now()
                obj.etag = f"etag-{obj_name}"
                matching_objects.append(obj)

        return matching_objects

    def remove_object(self, bucket_name: str, object_name: str):
        """Mock remove_object method."""
        if self.raise_errors:
            raise S3Error("Test error", "test", "test", "test", "test", None)

        self.deleted_objects.add(object_name)
        if object_name in self.objects:
            del self.objects[object_name]
        if object_name in self.uploaded_objects:
            del self.uploaded_objects[object_name]


class MockThetaClient:
    """Mock ThetaTerminal HTTP client."""

    def __init__(self, responses: Optional[Dict[str, Any]] = None, raise_errors: bool = False, latency_ms: int = 50):
        """
        Initialize mock Theta client.

        Args:
            responses: Dict of endpoint -> response mapping
            raise_errors: Whether to raise HTTP errors
            latency_ms: Simulated latency in responses
        """
        self.responses = responses or {}
        self.raise_errors = raise_errors
        self.latency_ms = latency_ms
        self.request_history = []

    def get(self, url: str, **kwargs):
        """Mock GET request."""
        self.request_history.append({"method": "GET", "url": url, "kwargs": kwargs})

        if self.raise_errors:
            response = MagicMock()
            response.status_code = 500
            response.text = "Internal Server Error"
            response.raise_for_status.side_effect = Exception("HTTP 500")
            return response

        # Parse endpoint from URL
        endpoint = self._extract_endpoint(url)

        # Return configured response or default
        response_data = self.responses.get(endpoint, self._default_response(endpoint))

        response = MagicMock()
        response.status_code = 200
        response.json.return_value = response_data
        response.content = json.dumps(response_data).encode()
        response.text = json.dumps(response_data)
        response.raise_for_status.return_value = None

        return response

    def close(self):
        """Mock close method."""
        pass

    def _extract_endpoint(self, url: str) -> str:
        """Extract endpoint from URL."""
        # Simple extraction - in reality would be more complex
        if "hist/stock/quote" in url:
            return "stock_quote"
        elif "bulk_hist/option/quote" in url:
            return "option_quote"
        elif "hist/stock/ohlc" in url:
            return "stock_ohlc"
        return "unknown"

    def _default_response(self, endpoint: str) -> Dict[str, Any]:
        """Generate default response for endpoint."""
        if endpoint == "stock_quote":
            return {
                "header": {
                    "latency_ms": self.latency_ms,
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
                "response": [[34200000, 100, 47, 149.75, 50, 100, 47, 149.85, 50, 20231117]],
            }
        elif endpoint == "option_quote":
            return {
                "header": {
                    "latency_ms": self.latency_ms,
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
                "response": [
                    {
                        "ticks": [[34200000, 10, 47, 149.50, 50, 10, 47, 150.00, 50, 20231117]],
                        "contract": {"root": "AAPL", "expiration": 20231117, "strike": 1500000, "right": "C"},
                    }
                ],
            }
        return {"header": {"latency_ms": self.latency_ms}, "response": []}


class MockFastParser:
    """Mock Rust parser for unit tests."""

    def __init__(self, return_empty: bool = False, raise_error: bool = False, num_records: int = 100):
        """
        Initialize mock parser.

        Args:
            return_empty: Whether to return empty results
            raise_error: Whether to raise parsing errors
            num_records: Number of records to return
        """
        self.return_empty = return_empty
        self.raise_error = raise_error
        self.num_records = num_records
        self.call_history = []

    def filter_contracts_to_parquet_bytes(
        self, option_json: str, stock_json: str, current_yyyymmdd: int, max_dte: int, base_pct: float
    ) -> bytes:
        """Mock filter_contracts_to_parquet_bytes function."""
        self.call_history.append(
            {
                "method": "filter_contracts_to_parquet_bytes",
                "current_date": current_yyyymmdd,
                "max_dte": max_dte,
                "base_pct": base_pct,
            }
        )

        if self.raise_error:
            raise ValueError("Mock parsing error")

        if self.return_empty:
            # Return minimal valid Parquet file
            import pyarrow as pa
            import pyarrow.parquet as pq

            table = pa.table({"empty": [1]})
            buffer = io.BytesIO()
            pq.write_table(table, buffer)
            return buffer.getvalue()

        # Return mock Parquet data
        return self._generate_mock_parquet()

    def filter_contracts_to_ipc_bytes(
        self, option_json: str, stock_json: str, current_yyyymmdd: int, max_dte: int, base_pct: float
    ) -> bytes:
        """Mock filter_contracts_to_ipc_bytes function."""
        self.call_history.append(
            {
                "method": "filter_contracts_to_ipc_bytes",
                "current_date": current_yyyymmdd,
                "max_dte": max_dte,
                "base_pct": base_pct,
            }
        )

        if self.raise_error:
            raise ValueError("Mock parsing error")

        # Return mock IPC data (simplified)
        return b"ARROW1\x00\x00" + b"\x00" * 100

    def _generate_mock_parquet(self) -> bytes:
        """Generate mock Parquet data."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create sample data
        data = {
            "ms_of_day": list(range(34200000, 34200000 + self.num_records * 1000, 1000)),
            "bid_price": [149.0 + (i * 0.01) for i in range(self.num_records)],
            "ask_price": [149.1 + (i * 0.01) for i in range(self.num_records)],
            "root": ["AAPL"] * self.num_records,
            "expiration": [20231117] * self.num_records,
            "strike": [1500000] * self.num_records,
            "right": ["C"] * self.num_records,
        }

        table = pa.table(data)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        return buffer.getvalue()


class MockAsyncIterator:
    """Mock async iterator for testing streaming responses."""

    def __init__(self, items: List[Any]):
        self.items = items
        self.index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.index >= len(self.items):
            raise StopAsyncIteration
        item = self.items[self.index]
        self.index += 1
        return item
