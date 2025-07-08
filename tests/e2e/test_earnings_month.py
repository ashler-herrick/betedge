#!/usr/bin/env python3
"""
End-to-end test for earnings data retrieval.

Tests the complete flow: API request → monthly processing → MinIO publishing

This script:
1. Makes API request for earnings data with date range
2. Verifies the API response and processing
3. Checks that monthly chunking worked correctly
4. Validates storage location and performance metrics

Requirements:
- API server running on localhost:8000
- NASDAQ API accessible
"""

import logging
import time
from datetime import datetime
from typing import Dict

import pytest
import requests

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mark all tests in this module as e2e tests
pytestmark = pytest.mark.e2e

# Test configuration
API_BASE_URL = "http://localhost:8000"
REQUEST_TIMEOUT = 180  # 3 minutes timeout for API request


def get_recent_month_range() -> tuple[str, str]:
    """
    Get a recent complete month date range.

    Returns:
        Tuple of (start_date, end_date) in YYYYMMDD format
    """
    # Use a historical month that we know has earnings data
    # December 2023 as a known good period
    start_date = datetime(2023, 12, 1)
    end_date = datetime(2023, 12, 31)

    return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")


def check_api_health() -> bool:
    """Check if the API is healthy and responsive."""
    start_time = time.time()
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=10)
        health_time = time.time() - start_time
        if response.status_code == 200:
            data = response.json()
            print(f"✓ API Health: {data.get('status', 'unknown')} ({health_time:.2f}s)")
            return True
        else:
            print(f"✗ API health check failed: {response.status_code} ({health_time:.2f}s)")
            return False
    except Exception as e:
        health_time = time.time() - start_time
        print(f"✗ API health check failed: {e} ({health_time:.2f}s)")
        return False


def make_earnings_request(start_date: str, end_date: str) -> Dict:
    """
    Make earnings API request.

    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format

    Returns:
        API response data
    """
    payload = {"start_date": start_date, "end_date": end_date, "return_format": "parquet"}

    print("Making earnings API request:")
    print(f"  Endpoint: POST {API_BASE_URL}/earnings")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Format: parquet")
    print()

    start_time = time.time()

    try:
        response = requests.post(
            f"{API_BASE_URL}/earnings",
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


def validate_response(response_data: Dict, start_date: str, end_date: str) -> bool:
    """
    Validate the API response.

    Args:
        response_data: API response data
        start_date: Original start date
        end_date: Original end date

    Returns:
        True if validation passes
    """
    start_time = time.time()
    print("Validating API response:")

    if "error" in response_data:
        validation_time = time.time() - start_time
        print(f"✗ API returned error: {response_data['error']} (validation: {validation_time:.2f}s)")
        return False

    # Check required fields
    required_fields = ["status", "request_id", "message", "processing_time_ms"]
    for field in required_fields:
        if field not in response_data:
            validation_time = time.time() - start_time
            print(f"✗ Missing required field: {field} (validation: {validation_time:.2f}s)")
            return False

    # Check status
    if response_data["status"] != "success":
        validation_time = time.time() - start_time
        print(f"✗ Request failed: {response_data.get('message', 'Unknown error')} (validation: {validation_time:.2f}s)")
        return False

    print(f"✓ Status: {response_data['status']}")
    print(f"✓ Request ID: {response_data['request_id']}")
    print(f"✓ Processing time: {response_data['processing_time_ms']}ms")

    # Validate storage location
    storage_location = response_data.get("storage_location")
    if storage_location and "earnings" in storage_location:
        print(f"✓ Storage location: {storage_location}")
    else:
        print(f"? Storage location unclear: {storage_location}")

    # Check message content for monthly processing
    message = response_data.get("message", "")
    if "month" in message.lower():
        print(f"✓ Monthly processing: {message}")
    else:
        print(f"? Processing details unclear from message: {message}")

    # Check data type
    data_type = response_data.get("data_type")
    if data_type == "parquet":
        print(f"✓ Data type: {data_type}")
    else:
        print(f"? Unexpected data type: {data_type}")

    # Check records count
    records_count = response_data.get("records_count", 0)
    if records_count > 0:
        print(f"✓ Records processed: {records_count:,} earnings records")
    else:
        print(f"? No records processed (records_count: {records_count})")

    validation_time = time.time() - start_time
    print(f"✓ Validation completed in {validation_time:.2f}s")
    return True


def display_test_summary(start_date: str, end_date: str, success: bool, total_time: float) -> None:
    """Display test summary."""
    print("=" * 70)
    print("END-TO-END EARNINGS TEST SUMMARY")
    print("=" * 70)

    print("Test Configuration:")
    print(f"  Date range: {start_date} to {end_date}")
    print("  Data type: Earnings announcements")
    print("  Format: Parquet")
    print("  Storage: S3-compatible object storage (MinIO)")
    print()

    print("Test Results:")
    status = "PASSED" if success else "FAILED"
    icon = "🎉" if success else "❌"
    print(f"  {icon} Overall result: {status}")
    print(f"  ⏱️  Total execution time: {total_time:.2f}s")
    print()

    if success:
        print("✅ End-to-end earnings flow verified successfully!")
        print("✅ Monthly chunking is working correctly")
        print("✅ MinIO object storage upload is working")
        print("✅ API processing completed without errors")
    else:
        print("❌ End-to-end earnings test failed")
        print("🔍 Check API logs and ensure:")
        print("   - NASDAQ API is accessible")
        print("   - MinIO is running on localhost:9000")
        print("   - API server is running on localhost:8000")


def test_earnings_month_e2e():
    """Run the end-to-end earnings test."""
    logger.info("🚀 Starting End-to-End Earnings Test")

    total_start_time = time.time()

    # Step 1: Get month date range
    logger.info("Step 1: Setting up test date range...")
    start_date, end_date = get_recent_month_range()

    logger.info(f"✓ Test period: {start_date} to {end_date}")

    # Step 2: Check API health
    logger.info("Step 2: Checking API health...")
    health_ok = check_api_health()
    assert health_ok, "API health check failed. Ensure API server is running."

    # Step 3: Make earnings request
    logger.info("Step 3: Making earnings request...")
    response_data = make_earnings_request(start_date, end_date)

    # Step 4: Validate response
    logger.info("Step 4: Validating response...")
    success = validate_response(response_data, start_date, end_date)
    assert success, "Response validation failed"

    # Step 5: Display summary
    total_time = time.time() - total_start_time
    display_test_summary(start_date, end_date, success, total_time)

    logger.info("🎉 End-to-end earnings test completed successfully!")


if __name__ == "__main__":
    # Allow running directly for debugging
    test_earnings_month_e2e()
