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

from betedge_data.manager.utils import generate_date_list
from tests.e2e.utils import (
    validate_async_response,
    validate_job_completion_for_minio,
    display_job_timing_info,
)

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


def validate_response(response_data: Dict, expected_dates: List[int]) -> Tuple[bool, str]:
    """
    Validate the async API response.

    Args:
        response_data: API response data
        expected_dates: List of expected dates as integers in YYYYMMDD format

    Returns:
        Tuple of (validation_success, job_id)
    """
    expected_days = len(expected_dates)
    
    # Use shared async response validation
    success = validate_async_response(response_data, f"stock request for {expected_days} days")
    
    if success:
        print(f"✓ Request accepted for processing {expected_days} trading days")
        print(f"✓ Date range covers: {expected_dates[0]} to {expected_dates[-1]}")
        print("✓ Data will be published to MinIO storage asynchronously")
        return True, response_data.get("job_id", "")
    
    return False, ""


def display_test_summary(
    start_date: str, 
    end_date: str, 
    expected_dates: List[int], 
    success: bool, 
    total_time: float,
    job_data: Dict = None
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
    print(f"  Business days: {', '.join(str(date) for date in expected_dates)}")
    print(f"  Interval: {TEST_INTERVAL}ms ({TEST_INTERVAL // 60000}m)")
    print(f"  RTH only: {TEST_RTH}")
    print("  Storage: S3-compatible object storage (MinIO)")
    print()

    print("Test Results:")
    status = "PASSED" if success else "FAILED"
    icon = "🎉" if success else "❌"
    print(f"  {icon} Overall result: {status}")
    
    # Display job timing info if available
    if job_data:
        display_job_timing_info(job_data, total_time)
    else:
        print(f"  🕐 Total Test Time: {total_time:.1f}s")
    print()

    if success:
        print("✅ End-to-end stock data workflow verified successfully!")
        print("✅ Async background job processing working")
        print("✅ Date splitting is working correctly")
        print("✅ MinIO object storage upload is working")
        print("✅ Job completion tracking working")
    else:
        print("❌ End-to-end test failed")
        print("🔍 Check API logs and ensure:")
        print("   - ThetaTerminal is running and authenticated")
        print("   - MinIO is running on localhost:9000")
        print("   - API server is running on localhost:8000")
        print("   - Background job processing is working correctly")


def test_aapl_stock_week_e2e():
    """Run the end-to-end AAPL stock quote test."""
    logger.info("🚀 Starting End-to-End AAPL Stock Quote Test")

    total_start_time = time.time()

    # Step 1: Get business week date range
    logger.info("Step 1: Calculating business week date range...")
    start_date, end_date = get_business_week_range()
    expected_dates = generate_date_list(start_date, end_date)

    logger.info(f"✓ Business week: {start_date} to {end_date}")
    logger.info(f"✓ Trading days: {', '.join(str(date) for date in expected_dates)} ({len(expected_dates)} days)")

    # Step 2: Check API health
    logger.info("Step 2: Checking API health...")
    health_ok = check_api_health()
    assert health_ok, "API health check failed. Ensure API server is running."

    # Step 3: Make stock request
    logger.info("Step 3: Making historical stock request...")
    response_data = make_stock_request(start_date, end_date)

    # Step 4: Validate async response
    logger.info("Step 4: Validating async response...")
    response_success, job_id = validate_response(response_data, expected_dates)
    assert response_success, "Async response validation failed"
    assert job_id, "No job ID received from async response"

    # Step 5: Wait for background job completion
    logger.info("Step 5: Waiting for background job completion...")
    job_success, job_data = validate_job_completion_for_minio(
        API_BASE_URL, job_id, len(expected_dates)
    )
    assert job_success, "Background job did not complete successfully"

    # Step 6: Display summary
    total_time = time.time() - total_start_time
    display_test_summary(start_date, end_date, expected_dates, True, total_time, job_data)

    logger.info("🎉 End-to-end stock data test completed successfully!")


if __name__ == "__main__":
    # Allow running directly for debugging
    test_aapl_stock_week_e2e()
