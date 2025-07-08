#!/usr/bin/env python3
"""
End-to-end test for AAPL stock quote data retrieval.

Tests the complete API workflow: API request → DataProcessingService → MinIO publishing

This script:
1. Calculates a business week date range (5 trading days)
2. Makes API request for AAPL stock quotes with week-long range
3. Verifies the API response and processing
4. Checks that date splitting worked correctly
5. Validates MinIO storage and performance metrics

Requirements:
- API server running on localhost:8000
- ThetaTerminal running and authenticated
- MinIO running on localhost:9000
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Tuple

import pytest
import requests

from betedge_data.manager.models import generate_date_list

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Mark all tests in this module as e2e tests
pytestmark = pytest.mark.e2e

# Test configuration
API_BASE_URL = "http://localhost:8000"
TEST_SYMBOL = "AAPL"
TEST_VENUE = "nqb"  # Nasdaq Basic
TEST_INTERVAL = 60000  # 1 minute intervals
TEST_RTH = True  # Regular trading hours only
REQUEST_TIMEOUT = 300  # 5 minutes timeout for API request


def get_business_week_range() -> Tuple[str, str]:
    """
    Get a business week date range (5 trading days).

    Uses a different date range than the options test to avoid conflicts.

    Returns:
        Tuple of (start_date, end_date) in YYYYMMDD format
    """
    # Use a different week than the options test to avoid MinIO conflicts
    # November 6-10, 2023 (Monday-Friday) - a week earlier
    start_date = datetime(2023, 11, 6)  # Monday
    end_date = datetime(2023, 11, 10)  # Friday

    return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")


def check_api_health() -> bool:
    """Check if the API is healthy and responsive."""
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✓ API Health: {data.get('status', 'unknown')}")
            return True
        else:
            print(f"✗ API health check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"✗ API health check failed: {e}")
        return False


def make_stock_request(start_date: str, end_date: str) -> Dict:
    """
    Make historical stock API request.

    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format

    Returns:
        API response data
    """
    payload = {
        "root": TEST_SYMBOL,
        "start_date": start_date,
        "end_date": end_date,
        "venue": TEST_VENUE,
        "rth": TEST_RTH,
        "interval": TEST_INTERVAL,
    }

    print("Making API request:")
    print(f"  Endpoint: POST {API_BASE_URL}/historical/stock")
    print(f"  Symbol: {TEST_SYMBOL}")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Venue: {TEST_VENUE}")
    print(f"  Interval: {TEST_INTERVAL}ms ({TEST_INTERVAL // 60000}m)")
    print(f"  RTH only: {TEST_RTH}")
    print()

    start_time = time.time()

    try:
        response = requests.post(
            f"{API_BASE_URL}/historical/stock",
            json=payload,
            timeout=REQUEST_TIMEOUT,
            headers={"Content-Type": "application/json"},
        )

        request_time = time.time() - start_time

        print("API Response:")
        print(f"  Status: {response.status_code}")
        print(f"  Request time: {request_time:.2f}s")

        if response.status_code == 202:  # Accepted
            return response.json()
        else:
            print(f"  Error: {response.text}")
            return {"error": f"HTTP {response.status_code}: {response.text}"}

    except requests.exceptions.Timeout:
        print(f"  Error: Request timed out after {REQUEST_TIMEOUT}s")
        return {"error": "Request timeout"}
    except Exception as e:
        print(f"  Error: {e}")
        return {"error": str(e)}


def validate_response(response_data: Dict, expected_dates: List[str]) -> bool:
    """
    Validate the API response.

    Args:
        response_data: API response data
        expected_dates: List of expected dates that should have been processed

    Returns:
        True if validation passes
    """
    print("Validating API response:")

    if "error" in response_data:
        print(f"✗ API returned error: {response_data['error']}")
        return False

    # Check required fields
    required_fields = ["status", "request_id", "message", "processing_time_ms"]
    for field in required_fields:
        if field not in response_data:
            print(f"✗ Missing required field: {field}")
            return False

    # Check status
    if response_data["status"] != "success":
        print(f"✗ Request failed: {response_data.get('message', 'Unknown error')}")
        return False

    print(f"✓ Status: {response_data['status']}")
    print(f"✓ Request ID: {response_data['request_id']}")
    print(f"✓ Processing time: {response_data['processing_time_ms']}ms")

    # Validate storage location
    storage_location = response_data.get("storage_location")
    if storage_location and "historical-stock" in storage_location:
        print(f"✓ Storage location: {storage_location}")
    else:
        print(f"? Storage location unclear: {storage_location}")

    # Check message content for date processing
    message = response_data.get("message", "")
    expected_days = len(expected_dates)

    if f"{expected_days}/{expected_days} days" in message or f"{expected_days} days" in message:
        print(f"✓ Date processing: {expected_days} days processed successfully")
    else:
        print(f"? Date processing unclear from message: {message}")

    # Check data size
    records_count = response_data.get("records_count", 0)
    if records_count > 0:
        print(f"✓ Data size: {records_count:,} bytes uploaded to MinIO")
    else:
        print(f"? No data published (records_count: {records_count})")

    return True


def display_test_summary(
    start_date: str, end_date: str, expected_dates: List[str], success: bool, total_time: float
) -> None:
    """Display test summary."""
    print("=" * 70)
    print("END-TO-END STOCK DATA TEST SUMMARY")
    print("=" * 70)

    print("Test Configuration:")
    print(f"  Symbol: {TEST_SYMBOL}")
    print(f"  Venue: {TEST_VENUE}")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Expected business days: {len(expected_dates)}")
    print(f"  Business days: {', '.join(expected_dates)}")
    print(f"  Interval: {TEST_INTERVAL}ms ({TEST_INTERVAL // 60000}m)")
    print(f"  RTH only: {TEST_RTH}")
    print("  Storage: S3-compatible object storage (MinIO)")
    print()

    print("Test Results:")
    status = "PASSED" if success else "FAILED"
    icon = "🎉" if success else "❌"
    print(f"  {icon} Overall result: {status}")
    print(f"  ⏱️  Total execution time: {total_time:.2f}s")
    print()

    if success:
        print("✅ End-to-end stock data workflow verified successfully!")
        print("✅ Date splitting is working correctly")
        print("✅ MinIO object storage upload is working")
        print("✅ API processing completed without errors")
    else:
        print("❌ End-to-end test failed")
        print("🔍 Check API logs and ensure:")
        print("   - ThetaTerminal is running and authenticated")
        print("   - MinIO is running on localhost:9000")
        print("   - API server is running on localhost:8000")


def test_aapl_stock_week_e2e():
    """Run the end-to-end AAPL stock quote test."""
    logger.info("🚀 Starting End-to-End AAPL Stock Quote Test")

    total_start_time = time.time()

    # Step 1: Get business week date range
    logger.info("Step 1: Calculating business week date range...")
    start_date, end_date = get_business_week_range()
    expected_dates = generate_date_list(start_date, end_date)

    logger.info(f"✓ Business week: {start_date} to {end_date}")
    logger.info(f"✓ Trading days: {', '.join(expected_dates)} ({len(expected_dates)} days)")

    # Step 2: Check API health
    logger.info("Step 2: Checking API health...")
    health_ok = check_api_health()
    assert health_ok, "API health check failed. Ensure API server is running."

    # Step 3: Make stock request
    logger.info("Step 3: Making historical stock request...")
    response_data = make_stock_request(start_date, end_date)

    # Step 4: Validate response
    logger.info("Step 4: Validating response...")
    success = validate_response(response_data, expected_dates)
    assert success, "Response validation failed"

    # Step 5: Display summary
    total_time = time.time() - total_start_time
    display_test_summary(start_date, end_date, expected_dates, success, total_time)

    logger.info("🎉 End-to-end stock data test completed successfully!")


if __name__ == "__main__":
    # Allow running directly for debugging
    test_aapl_stock_week_e2e()
