"""
Shared pytest fixtures and configuration for all tests.
"""

import io
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import List
from unittest.mock import MagicMock

import pytest
import pyarrow as pa
import pyarrow.parquet as pq
from minio import Minio

from betedge_data.storage.config import MinIOConfig
from betedge_data.manager.models import ExternalHistoricalOptionRequest


# ============= MinIO Fixtures =============


@pytest.fixture
def mock_minio_client():
    """Mock MinIO client for unit tests."""
    mock = MagicMock(spec=Minio)

    # Mock bucket operations
    mock.bucket_exists.return_value = True
    mock.make_bucket.return_value = None

    # Mock object operations
    mock.put_object.return_value = MagicMock(etag="test-etag")
    mock.stat_object.return_value = MagicMock(size=1024, etag="test-etag")
    mock.list_objects.return_value = []
    mock.remove_object.return_value = None

    return mock


@pytest.fixture
def minio_config():
    """Test MinIO configuration."""
    return MinIOConfig(
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="test-bucket",
        secure=False,
        region="us-east-1",
    )


# ============= ThetaTerminal Fixtures =============


@pytest.fixture
def mock_theta_response():
    """Mock ThetaTerminal API response."""
    return {
        "header": {
            "latency_ms": 50,
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
            [34200000, 100, 47, 149.75, 50, 100, 47, 149.85, 50, 20231117],
            [35100000, 150, 47, 148.95, 50, 120, 47, 149.05, 50, 20231117],
        ],
    }


@pytest.fixture
def mock_option_response():
    """Mock ThetaTerminal option API response."""
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
        "response": [
            {
                "ticks": [
                    [34200000, 10, 47, 149.50, 50, 10, 47, 150.00, 50, 20231117],
                    [35100000, 13, 47, 148.75, 50, 11, 47, 149.25, 50, 20231117],
                ],
                "contract": {"root": "AAPL", "expiration": 20231117, "strike": 1500000, "right": "C"},
            },
            {
                "ticks": [[34200000, 8, 47, 164.50, 50, 8, 47, 165.00, 50, 20231117]],
                "contract": {"root": "AAPL", "expiration": 20231117, "strike": 1650000, "right": "C"},
            },
        ],
    }


# ============= Parquet Data Fixtures =============


@pytest.fixture
def sample_parquet_data():
    """Create sample Parquet data for testing."""
    # Create sample Arrow table
    data = {
        "ms_of_day": [34200000, 35100000, 36000000],
        "bid_price": [149.75, 148.95, 147.50],
        "ask_price": [149.85, 149.05, 147.60],
        "bid_size": [100, 150, 200],
        "ask_size": [100, 120, 180],
        "root": ["AAPL", "AAPL", "AAPL"],
        "expiration": [20231117, 20231117, 20231117],
        "strike": [1500000, 1500000, 1500000],
        "right": ["C", "C", "C"],
    }

    table = pa.table(data)

    # Write to BytesIO
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    return buffer


@pytest.fixture
def sample_parquet_file(tmp_path):
    """Create a sample Parquet file for testing."""
    data = {
        "ms_of_day": [34200000, 35100000],
        "bid_price": [149.75, 148.95],
        "ask_price": [149.85, 149.05],
        "root": ["AAPL", "AAPL"],
    }

    table = pa.table(data)
    file_path = tmp_path / "test_data.parquet"
    pq.write_table(table, str(file_path))

    return file_path


# ============= Request Model Fixtures =============


@pytest.fixture
def historical_option_request():
    """Sample historical option request."""
    return ExternalHistoricalOptionRequest(
        root="AAPL",
        start_date="20231113",
        end_date="20231117",
        interval=900000,  # 15 minutes
    )


@pytest.fixture
def historical_stock_request():
    """Sample historical stock request data (as dict since no model exists)."""
    return {
        "ticker": "AAPL",
        "start_date": "20231113",
        "end_date": "20231117",
        "data_type": "ohlc",
        "interval": 60000,  # 1 minute
        "rth": True,
    }


# ============= File System Fixtures =============


@pytest.fixture
def temp_directory():
    """Create a temporary directory for file operations."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Mock environment variables for testing."""
    test_env = {
        "MINIO_ENDPOINT": "test-minio:9000",
        "MINIO_ACCESS_KEY": "test-access",
        "MINIO_SECRET_KEY": "test-secret",
        "MINIO_BUCKET": "test-bucket",
        "MINIO_SECURE": "false",
        "MINIO_REGION": "us-test-1",
        "THETA_BASE_URL": "http://test-theta:25510/v2",
        "THETA_TIMEOUT": "30",
    }

    for key, value in test_env.items():
        monkeypatch.setenv(key, value)

    return test_env


@pytest.fixture
def integration_env_vars(monkeypatch):
    """Environment variables for integration tests with local MinIO."""
    integration_env = {
        "MINIO_ENDPOINT": "localhost:9000",
        "MINIO_ACCESS_KEY": "minioadmin",
        "MINIO_SECRET_KEY": "minioadmin123",
        "MINIO_BUCKET": "test-integration-bucket",
        "MINIO_SECURE": "false",
        "MINIO_REGION": "us-east-1",
        "THETA_BASE_URL": "http://localhost:25510/v2",
        "THETA_TIMEOUT": "30",
    }

    for key, value in integration_env.items():
        monkeypatch.setenv(key, value)

    return integration_env


@pytest.fixture
def mock_minio_publisher():
    """Mock MinIOPublisher for unit tests."""
    publisher = MagicMock()
    publisher.file_exists.return_value = False
    publisher.publish = MagicMock()
    publisher.close = MagicMock()
    return publisher


# ============= HTTP Client Fixtures =============


@pytest.fixture
def mock_httpx_client():
    """Mock httpx client for API tests."""
    mock = MagicMock()

    # Mock response
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "ok"}
    mock_response.content = b'{"status": "ok"}'
    mock_response.raise_for_status.return_value = None

    mock.get.return_value = mock_response
    mock.post.return_value = mock_response

    return mock


# ============= Test Data Generators =============


@pytest.fixture
def date_range_generator():
    """Generate date ranges for testing."""

    def generate(start_date: str, num_days: int) -> List[str]:
        from datetime import timedelta

        start = datetime.strptime(start_date, "%Y%m%d")
        dates = []
        for i in range(num_days):
            date = start + timedelta(days=i)
            dates.append(date.strftime("%Y%m%d"))
        return dates

    return generate


# ============= Pytest Configuration =============


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line("markers", "unit: mark test as unit test (no external dependencies)")
    config.addinivalue_line("markers", "integration: mark test as integration test (requires services)")
    config.addinivalue_line("markers", "e2e: mark test as end-to-end test")
    config.addinivalue_line("markers", "slow: mark test as slow running")


# ============= Test Session Fixtures =============


@pytest.fixture(scope="session")
def test_data_dir():
    """Path to test data directory."""
    return Path(__file__).parent / "test_data"


@pytest.fixture(autouse=True)
def cleanup_test_files(request):
    """Automatically clean up test files after each test."""
    created_files = []

    def track_file(filepath):
        created_files.append(filepath)

    request.addfinalizer(lambda: [os.unlink(f) for f in created_files if os.path.exists(f)])

    return track_file
