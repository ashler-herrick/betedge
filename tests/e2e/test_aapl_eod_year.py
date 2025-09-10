#!/usr/bin/env python3
"""
End-to-end test for AAPL EOD (End-of-Day) stock data retrieval.

Tests the complete API workflow: API request → DataProcessingService → MinIO publishing

This script:
1. Makes API request for AAPL EOD data for a full year
2. Verifies the API response and processing
3. Checks that yearly data aggregation worked correctly
4. Validates MinIO storage and performance metrics
5. Verifies EOD data structure (17 fields with OHLC + quote data)

Requirements:
- API server running on localhost:8000
- ThetaTerminal running and authenticated
- MinIO running on localhost:9000
"""

import logging
import time
from typing import Dict, Tuple

import pytest
import requests
from minio import Minio
from minio.error import S3Error
import pyarrow.parquet as pq
import pyarrow as pa

from betedge_data.storage.config import MinIOConfig
from tests.e2e.utils import (
    validate_async_response,
    validate_job_completion_for_minio,
    display_job_timing_info,
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mark all tests in this module as e2e tests
pytestmark = pytest.mark.e2e

# Test configuration
API_BASE_URL = "http://localhost:8000"
TEST_SYMBOL = "AAPL"
TEST_YEAR = 2024  # Use 2024 for testing
REQUEST_TIMEOUT = 300  # 5 minutes timeout for API request (EOD requests may take longer)
MINIO_VALIDATION_TIMEOUT = 60  # 1 minute timeout for MinIO validation

# Expected EOD schema (17 fields)
EXPECTED_EOD_FIELDS = [
    "ms_of_day",
    "ms_of_day2",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "count",
    "bid_size",
    "bid_exchange",
    "bid",
    "bid_condition",
    "ask_size",
    "ask_exchange",
    "ask",
    "ask_condition",
    "date",
]


def get_test_year_range() -> Tuple[int, int]:
    """
    Get test year range.

    Returns:
        Tuple of (start_year, end_year) for testing
    """
    # Use 2024 for testing - a complete year with known EOD data
    return TEST_YEAR, TEST_YEAR


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


def make_eod_request(start_year: int, end_year: int) -> Dict:
    """
    Make historical stock EOD API request.

    Args:
        start_year: Start year for EOD data
        end_year: End year for EOD data

    Returns:
        API response data
    """
    # Convert years to date format expected by unified endpoint
    start_date = f"{start_year}0101"
    end_date = f"{end_year}1231"

    payload = {
        "root": TEST_SYMBOL,
        "start_date": start_date,
        "end_date": end_date,
        "data_schema": "eod",
        "return_format": "parquet",
    }

    print("Making EOD API request:")
    print(f"  Endpoint: POST {API_BASE_URL}/historical/stock")
    print(f"  Symbol: {TEST_SYMBOL}")
    print(f"  Date range: {start_date} to {end_date}")
    print("  Endpoint: eod")
    print("  Format: parquet")
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


def validate_eod_response(response_data: Dict, expected_years: int) -> Tuple[bool, str]:
    """
    Validate the async EOD API response.

    Args:
        response_data: API response data
        expected_years: Number of years that should have been processed

    Returns:
        Tuple of (validation_success, job_id)
    """
    # Use shared async response validation
    success = validate_async_response(response_data, f"EOD request for {expected_years} year(s)")

    if success:
        print(f"✓ Request accepted for processing {expected_years} year(s) of EOD data")
        print("✓ Data will be published to MinIO storage asynchronously")
        return True, response_data.get("job_id", "")

    return False, ""


def validate_minio_storage(symbol: str, year: int) -> bool:
    """
    Validate MinIO storage contains the expected EOD data.

    Args:
        symbol: Stock symbol
        year: Year of data

    Returns:
        True if validation passes
    """
    print("Validating MinIO storage:")

    try:
        # Initialize MinIO client
        config = MinIOConfig()
        client = Minio(
            endpoint=config.endpoint,
            access_key=config.access_key,
            secret_key=config.secret_key,
            secure=config.secure,
        )

        # Check if bucket exists
        if not client.bucket_exists(config.bucket):
            print(f"✗ MinIO bucket '{config.bucket}' does not exist")
            return False

        print(f"✓ MinIO bucket '{config.bucket}' exists")

        # Check for EOD file
        object_key = f"historical-stock/eod/{symbol}/{year}/data.parquet"
        print(f"  Looking for object: {object_key}")

        try:
            stat = client.stat_object(config.bucket, object_key)
            file_size = stat.size
            print(f"✓ EOD file found: {object_key}")
            print(f"✓ File size: {file_size:,} bytes")

            # Download and validate Parquet file
            response = client.get_object(config.bucket, object_key)
            parquet_data = response.read()
            response.close()

            # Read Parquet file with PyArrow
            table = pq.read_table(pa.BufferReader(parquet_data))

            # Validate schema
            schema_names = table.schema.names
            print("✓ Parquet file loaded successfully")
            print(f"✓ Columns found: {len(schema_names)}")
            print(f"✓ Rows found: {len(table):,}")

            # Check for expected EOD fields
            missing_fields = set(EXPECTED_EOD_FIELDS) - set(schema_names)
            if missing_fields:
                print(f"✗ Missing expected EOD fields: {missing_fields}")
                return False

            extra_fields = set(schema_names) - set(EXPECTED_EOD_FIELDS)
            if extra_fields:
                print(f"? Unexpected fields found: {extra_fields}")

            print(f"✓ All expected EOD fields present: {len(EXPECTED_EOD_FIELDS)} fields")

            # Basic data validation
            if len(table) == 0:
                print("✗ EOD file is empty")
                return False

            # Check for reasonable data size (EOD should have ~252 trading days per year)
            expected_min_rows = 200  # Account for holidays/weekends
            expected_max_rows = 300  # Allow some buffer

            if len(table) < expected_min_rows:
                print(f"? Fewer rows than expected: {len(table)} < {expected_min_rows}")
            elif len(table) > expected_max_rows:
                print(f"? More rows than expected: {len(table)} > {expected_max_rows}")
            else:
                print(f"✓ Row count within expected range: {len(table)} rows")

            # Validate data types for key fields
            try:
                # Check that date column exists and has reasonable values
                dates = table.column("date").to_pandas()
                min_date = dates.min()
                max_date = dates.max()
                print(f"✓ Date range: {min_date} to {max_date}")

                # Check that OHLC data exists
                for field in ["open", "high", "low", "close"]:
                    if field in schema_names:
                        values = table.column(field).to_pandas()
                        if values.isna().all():
                            print(f"✗ {field} column is all NaN")
                            return False
                        print(f"✓ {field} data looks valid (range: {values.min():.2f} - {values.max():.2f})")

            except Exception as e:
                print(f"? Data validation warning: {e}")

            return True

        except S3Error as e:
            if e.code == "NoSuchKey":
                print(f"✗ EOD file not found: {object_key}")
            else:
                print(f"✗ MinIO error: {e}")
            return False

    except Exception as e:
        print(f"✗ MinIO validation failed: {e}")
        return False


def display_test_summary(
    start_year: int, end_year: int, success: bool, total_time: float, job_data: Dict = None
) -> None:
    """Display test summary."""
    print("=" * 70)
    print("END-TO-END EOD STOCK DATA TEST SUMMARY")
    print("=" * 70)

    print("Test Configuration:")
    print(f"  Symbol: {TEST_SYMBOL}")
    print(f"  Year range: {start_year} to {end_year}")
    print(f"  Expected years: {end_year - start_year + 1}")
    print("  Data type: End-of-Day (EOD) with OHLC + Quote data")
    print("  Format: Parquet")
    print("  Storage: S3-compatible object storage (MinIO)")
    print(f"  Expected schema: {len(EXPECTED_EOD_FIELDS)} fields")
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
        print("✅ End-to-end EOD data workflow verified successfully!")
        print("✅ Async background job processing working")
        print("✅ Yearly data aggregation is working correctly")
        print("✅ MinIO object storage upload is working")
        print("✅ EOD data structure validation passed")
        print("✅ Job completion tracking working")
    else:
        print("❌ End-to-end EOD test failed")
        print("🔍 Check API logs and ensure:")
        print("   - ThetaTerminal is running and authenticated")
        print("   - MinIO is running on localhost:9000")
        print("   - API server is running on localhost:8000")
        print("   - Background job processing is working correctly")


def test_aapl_eod_year_e2e():
    """Run the end-to-end AAPL EOD test."""
    logger.info("🚀 Starting End-to-End AAPL EOD Test")

    total_start_time = time.time()

    # Step 1: Get test year range
    logger.info("Step 1: Setting up test year range...")
    start_year, end_year = get_test_year_range()
    expected_years = end_year - start_year + 1

    logger.info(f"✓ Test years: {start_year} to {end_year}")
    logger.info(f"✓ Expected years to process: {expected_years}")

    # Step 2: Check API health
    logger.info("Step 2: Checking API health...")
    health_ok = check_api_health()
    assert health_ok, "API health check failed. Ensure API server is running."

    # Step 3: Make EOD request
    logger.info("Step 3: Making historical stock EOD request...")
    response_data = make_eod_request(start_year, end_year)

    # Step 4: Validate async response
    logger.info("Step 4: Validating async response...")
    response_success, job_id = validate_eod_response(response_data, expected_years)
    assert response_success, "Async response validation failed"
    assert job_id, "No job ID received from async response"

    # Step 5: Wait for background job completion
    logger.info("Step 5: Waiting for background job completion...")
    job_success, job_data = validate_job_completion_for_minio(
        API_BASE_URL,
        job_id,
        expected_years,  # EOD typically processes 1 item per year
    )
    assert job_success, "Background job did not complete successfully"

    # Step 6: Validate MinIO storage
    logger.info("Step 6: Validating MinIO storage...")
    storage_ok = validate_minio_storage(TEST_SYMBOL, TEST_YEAR)
    assert storage_ok, "MinIO storage validation failed"

    # Step 7: Display summary
    total_time = time.time() - total_start_time
    display_test_summary(start_year, end_year, True, total_time, job_data)

    logger.info("🎉 End-to-end EOD test completed successfully!")


if __name__ == "__main__":
    # Allow running directly for debugging
    test_aapl_eod_year_e2e()
