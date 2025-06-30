#!/usr/bin/env python3
"""
Integration tests for the data processing API.
"""

import logging

import pytest
from fastapi.testclient import TestClient

from betedge_data.manager.api import app
from betedge_data.manager.service import DataProcessingService
from betedge_data.historical.config import HistoricalClientConfig

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


@pytest.fixture
def client():
    """Create test client for the API."""
    # Mock the service to avoid MinIO connection
    from betedge_data.manager import api
    from unittest.mock import AsyncMock, MagicMock
    
    mock_service = MagicMock()
    
    # Make the async method return a proper response
    async def mock_process_request(request, request_id=None):
        from betedge_data.manager.models import DataProcessingResponse
        from uuid import uuid4
        return DataProcessingResponse(
            status="success",
            request_id=uuid4(),
            message="Mock success",
            data_type="parquet",
            storage_location="mock://bucket/path",
            records_count=1000,
            processing_time_ms=1500
        )
    
    mock_service.process_historical_option_request = mock_process_request
    
    # Mock MinIO publisher for storage endpoints
    mock_publisher = MagicMock()
    mock_publisher.get_bucket_stats.return_value = {
        "bucket": "test-bucket",
        "total_files": 0,
        "total_size_bytes": 0,
    }
    mock_service.minio_publisher = mock_publisher
    mock_service.force_refresh = False
    
    api.service = mock_service
    return TestClient(app)


def test_health_check(client):
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "betedge-historical-options"


def test_root_endpoint(client):
    """Test the root endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "BetEdge Historical Options API"
    assert data["version"] == "1.0.0"
    assert "/historical/option" in data["endpoints"]
    assert "/docs" in data.values()
    assert "/health" in data.values()


def test_config_endpoint(client):
    """Test the configuration endpoint."""
    response = client.get("/config")
    assert response.status_code == 200
    data = response.json()
    # Should contain config fields
    expected_fields = [
        "message",
        "storage_type", 
        "storage_path_pattern",
    ]
    for field in expected_fields:
        assert field in data


def test_storage_stats_endpoint(client):
    """Test the storage stats endpoint."""
    response = client.get("/storage/stats")
    # With our mock service, this might return 200 with mock stats
    # or 503 if MinIO is not available
    assert response.status_code in [200, 503]


# Historical stock endpoint removed - no longer exists


def test_historical_option_validation(client):
    """Test historical option request validation."""
    # Test valid request with string dates (manager models)
    valid_request = {
        "root": "AAPL",
        "exp": 0,  # All expirations
        "start_date": "20240101",
        "end_date": "20240101",  # Single day for exp=0
        "max_dte": 30,
        "base_pct": 0.1,
    }

    # This should work with our mock service
    response = client.post("/historical/option", json=valid_request)
    # Should get success with mock
    assert response.status_code in [200, 202]

    # Test invalid date format
    invalid_request = {
        "root": "AAPL",
        "exp": 0,
        "start_date": "2024-01-01",  # Wrong format
        "end_date": "2024-01-01",
        "max_dte": 30,
        "base_pct": 0.1,
    }

    response = client.post("/historical/option", json=invalid_request)
    assert response.status_code == 422  # Validation error


# Live endpoints removed - no longer exist


if __name__ == "__main__":
    # Run tests manually
    from betedge_data.manager import api
    from unittest.mock import MagicMock

    mock_service = MagicMock()
    api.service = mock_service
    client = TestClient(app)

    print("Running API integration tests...")

    tests = [
        ("Health Check", test_health_check),
        ("Root Endpoint", test_root_endpoint),
        ("Config Endpoint", test_config_endpoint),
        ("Storage Stats Endpoint", test_storage_stats_endpoint),
        ("Historical Option Validation", test_historical_option_validation),
    ]

    passed = 0
    total = len(tests)

    for test_name, test_func in tests:
        try:
            test_func(client)
            print(f"✅ {test_name}: PASSED")
            passed += 1
        except Exception as e:
            print(f"❌ {test_name}: FAILED - {e}")

    print(f"\nTest Results: {passed}/{total} passed")

    if passed == total:
        print("🎉 All tests passed!")
    else:
        print(f"❌ {total - passed} test(s) failed")
