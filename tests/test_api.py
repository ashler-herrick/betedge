#!/usr/bin/env python3
"""
Integration tests for the data processing API.
"""

import logging

import pytest
from fastapi.testclient import TestClient

from betedge_data.manager.api import app
from betedge_data.manager.service import DataProcessingService
from betedge_data.config import ThetaClientConfig

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
def client():
    """Create test client for the API."""
    # Initialize service for testing
    from betedge_data.manager import api
    api.service = DataProcessingService(config=ThetaClientConfig())
    return TestClient(app)


def test_health_check(client):
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "betedge-data-processing"


def test_root_endpoint(client):
    """Test the root endpoint."""
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "BetEdge Data Processing API"
    assert data["version"] == "1.0.0"
    assert "/historical/stock" in data["endpoints"]
    assert "/historical/option" in data["endpoints"]
    assert "/live/stock" in data["endpoints"]
    assert "/live/option" in data["endpoints"]


def test_config_endpoint(client):
    """Test the configuration endpoint."""
    response = client.get("/config")
    assert response.status_code == 200
    data = response.json()
    # Should contain config fields
    expected_fields = [
        "base_url", "stock_tier", "option_tier", 
        "stock_interval", "option_interval", "max_concurrent_requests"
    ]
    for field in expected_fields:
        assert field in data


def test_topics_endpoint(client):
    """Test the Kafka topics endpoint."""
    response = client.get("/topics")
    assert response.status_code == 200
    data = response.json()
    assert "topics" in data
    topics = data["topics"]
    expected_topics = ["historical_stock", "historical_option", "live_stock", "live_option"]
    for topic in expected_topics:
        assert topic in topics


def test_historical_stock_validation(client):
    """Test historical stock request validation."""
    # Test valid request
    valid_request = {
        "ticker": "AAPL",
        "start_date": "20240101",
        "end_date": "20240102",
        "data_type": "ohlc",
        "interval": 60000,
        "rth": True
    }
    
    # This will fail at the service level since ThetaTerminal isn't running
    # but should pass validation
    response = client.post("/historical/stock", json=valid_request)
    # Should get 400 or 500, not 422 (validation error)
    assert response.status_code in [400, 500, 502]
    
    # Test invalid request - missing interval for OHLC
    invalid_request = {
        "ticker": "AAPL",
        "start_date": "20240101", 
        "end_date": "20240102",
        "data_type": "ohlc",
        # interval missing - should fail validation
        "rth": True
    }
    
    response = client.post("/historical/stock", json=invalid_request)
    assert response.status_code == 422  # Validation error
    
    # Test invalid date format
    invalid_date_request = {
        "ticker": "AAPL",
        "start_date": "2024-01-01",  # Wrong format
        "end_date": "20240102",
        "data_type": "quote"
    }
    
    response = client.post("/historical/stock", json=invalid_date_request)
    assert response.status_code == 422  # Validation error


def test_historical_option_validation(client):
    """Test historical option request validation."""
    # Test valid request
    valid_request = {
        "root": "AAPL",
        "exp": "20240315",
        "start_date": "20240101",
        "end_date": "20240102",
        "max_dte": 30,
        "base_pct": 0.1
    }
    
    # This will fail at the service level since ThetaTerminal isn't running
    # but should pass validation
    response = client.post("/historical/option", json=valid_request)
    # Should get 400 or 500, not 422 (validation error)
    assert response.status_code in [400, 500, 502]
    
    # Test invalid expiration format
    invalid_request = {
        "root": "AAPL",
        "exp": "2024-03-15",  # Wrong format
        "start_date": "20240101",
        "end_date": "20240102"
    }
    
    response = client.post("/historical/option", json=invalid_request)
    assert response.status_code == 422  # Validation error


def test_live_stock_validation(client):
    """Test live stock request validation."""
    # Test valid request
    valid_request = {
        "ticker": "AAPL",
        "date": "20240315",
        "time_ms": 34200000  # 9:30 AM ET
    }
    
    # This will fail at the service level since ThetaTerminal isn't running
    # but should pass validation
    response = client.post("/live/stock", json=valid_request)
    # Should get 400 or 500, not 422 (validation error)
    assert response.status_code in [400, 500, 502]
    
    # Test invalid time_ms (too high)
    invalid_request = {
        "ticker": "AAPL",
        "date": "20240315",
        "time_ms": 90000000  # More than 24 hours
    }
    
    response = client.post("/live/stock", json=invalid_request)
    assert response.status_code == 422  # Validation error


def test_live_option_not_implemented(client):
    """Test that live option endpoint returns not implemented."""
    request = {
        "root": "AAPL",
        "exp": "20240315",
        "date": "20240315",
        "time_ms": 34200000
    }
    
    response = client.post("/live/option", json=request)
    assert response.status_code == 501  # Not implemented


if __name__ == "__main__":
    # Run tests manually
    from betedge_data.manager import api
    api.service = DataProcessingService(config=ThetaClientConfig())
    client = TestClient(app)
    
    print("Running API integration tests...")
    
    tests = [
        ("Health Check", test_health_check),
        ("Root Endpoint", test_root_endpoint),
        ("Config Endpoint", test_config_endpoint),
        ("Topics Endpoint", test_topics_endpoint),
        ("Historical Stock Validation", test_historical_stock_validation),
        ("Historical Option Validation", test_historical_option_validation),
        ("Live Stock Validation", test_live_stock_validation),
        ("Live Option Not Implemented", test_live_option_not_implemented),
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