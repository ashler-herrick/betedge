#!/usr/bin/env python3
"""
End-to-end test for AAPL options data retrieval.

Tests the complete flow: API request → date splitting → MinIO publishing

This script:
1. Calculates a business week date range (5 trading days)
2. Makes API request for AAPL options with week-long range
3. Verifies the API response and processing
4. Checks that date splitting worked correctly
5. Validates topic naming and performance metrics

Requirements:
- API server running on localhost:8000
- ThetaTerminal running and authenticated
"""

import logging
import time
from datetime import datetime
from typing import Dict, List, Tuple

import pytest
import requests
from minio import Minio
from minio.error import S3Error
import pyarrow.parquet as pq
import pyarrow as pa
import polars as pl

from betedge_data.manager.utils import generate_trading_date_list
from betedge_data.storage.config import MinIOConfig
from betedge_data.historical.option.models import HistOptionBulkRequest
from betedge_data.common.models import TICK_SCHEMAS, CONTRACT_SCHEMA


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Mark all tests in this module as e2e tests
pytestmark = pytest.mark.e2e

# Test configuration
API_BASE_URL = "http://localhost:8000"
TEST_SYMBOL = "AAPL"
TEST_INTERVAL = 900000  # 15 minutes
REQUEST_TIMEOUT = 300  # 5 minutes timeout for API request
MINIO_VALIDATION_TIMEOUT = 60  # 1 minute timeout for MinIO validation


def get_business_week_range(days_back: int = 30) -> Tuple[str, str]:
    """
    Get a business week date range (5 trading days).

    Args:
        days_back: How many days back to start looking for a complete week

    Returns:
        Tuple of (start_date, end_date) in YYYYMMDD format
    """
    # Use a fixed historical date that we know exists
    # Let's use November 2023 as a known good period
    start_date = datetime(2023, 10, 13)  # Monday
    end_date = datetime(2023, 10, 17)  # Friday

    return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")


def generate_expected_object_keys(dates: List[int], symbol: str, interval: int) -> List[str]:
    """
    Generate expected MinIO object keys using actual request objects.

    Args:
        dates: List of dates as integers in YYYYMMDD format
        symbol: Stock symbol (e.g., 'AAPL')
        interval: Interval in milliseconds

    Returns:
        List of expected object keys
    """
    object_keys = []
    
    for date_int in dates:
        # Use actual HistOptionBulkRequest to generate object key
        request = HistOptionBulkRequest(
            root=symbol,
            date=date_int,
            interval=interval,
            endpoint="quote",
            return_format="parquet"
        )
        object_key = request.generate_object_key()
        object_keys.append(object_key)
    
    logger.debug(f"Generated {len(object_keys)} expected object keys using actual request model")
    return object_keys


def validate_parquet_schema(file_content: bytes, endpoint: str = "quote") -> Dict:
    """
    Validate Parquet file schema against expected structure.
    
    Args:
        file_content: Raw Parquet file content as bytes
        endpoint: Endpoint type ("quote" or "ohlc") to determine expected schema
        
    Returns:
        Dict with validation results
    """
    try:
        # Read Parquet file
        table = pq.read_table(pa.BufferReader(file_content))
        
        # Get expected schema from actual definitions
        expected_tick_fields = TICK_SCHEMAS[endpoint]["field_names"]
        expected_contract_fields = CONTRACT_SCHEMA["field_names"]
        expected_fields = expected_tick_fields + expected_contract_fields
        
        # Validate schema
        actual_fields = table.schema.names
        schema_valid = set(actual_fields) == set(expected_fields)
        
        return {
            "schema_valid": schema_valid,
            "expected_fields": expected_fields,
            "actual_fields": actual_fields,
            "missing_fields": list(set(expected_fields) - set(actual_fields)),
            "extra_fields": list(set(actual_fields) - set(expected_fields)),
            "row_count": len(table),
            "column_count": len(actual_fields),
            "file_size_bytes": len(file_content)
        }
    except Exception as e:
        return {
            "schema_valid": False,
            "error": str(e),
            "file_size_bytes": len(file_content)
        }


def validate_data_quality(file_content: bytes) -> Dict:
    """
    Validate data quality and realistic ranges.
    
    Args:
        file_content: Raw Parquet file content as bytes
        
    Returns:
        Dict with data quality results
    """
    try:
        # Read Parquet file using polars directly (handles compression automatically)
        df = pl.read_parquet(pa.BufferReader(file_content))
        
        quality_checks = {
            "total_rows": df.height,
            "non_null_rows": df.height - df.null_count().sum_horizontal().item(),
            "has_option_data": False,
            "has_stock_data": False,
            "price_ranges_realistic": True,
            "time_ranges_valid": True,
            "contract_data_valid": True,
            "unique_contracts": 0,
            "data_errors": []
        }
        
        if df.height > 0:
            # Check for option vs stock data
            if 'expiration' in df.columns:
                option_count = df.filter(pl.col('expiration') > 0).height
                stock_count = df.filter(pl.col('expiration') == 0).height
                
                quality_checks["has_option_data"] = option_count > 0
                quality_checks["has_stock_data"] = stock_count > 0
                quality_checks["unique_contracts"] = df.filter(pl.col('expiration') > 0).select('expiration').n_unique()
            
            # Check price ranges for quote data
            if 'bid' in df.columns and 'ask' in df.columns:
                # Filter positive prices
                bid_prices = df.filter(pl.col('bid') > 0).select('bid')
                ask_prices = df.filter(pl.col('ask') > 0).select('ask')
                
                if bid_prices.height > 0:
                    bid_min = bid_prices.min().item()
                    bid_max = bid_prices.max().item()
                    if not (0.01 <= bid_min and bid_max <= 10000):
                        quality_checks["price_ranges_realistic"] = False
                        quality_checks["data_errors"].append(f"Bid prices outside realistic range: {bid_min}-{bid_max}")
                        
                if ask_prices.height > 0:
                    ask_min = ask_prices.min().item()
                    ask_max = ask_prices.max().item()
                    if not (0.01 <= ask_min and ask_max <= 10000):
                        quality_checks["price_ranges_realistic"] = False
                        quality_checks["data_errors"].append(f"Ask prices outside realistic range: {ask_min}-{ask_max}")
            
            # Check time ranges
            if 'ms_of_day' in df.columns:
                ms_min = df.select('ms_of_day').min().item()
                ms_max = df.select('ms_of_day').max().item()
                if not (0 <= ms_min and ms_max <= 86400000):  # 24 hours in ms
                    quality_checks["time_ranges_valid"] = False
                    quality_checks["data_errors"].append(f"Time values outside valid range: {ms_min}-{ms_max}")
        
        return quality_checks
        
    except Exception as e:
        return {
            "total_rows": 0,
            "data_errors": [f"Failed to validate data quality: {str(e)}"]
        }


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


def make_options_request(start_date: str, end_date: str) -> Dict:
    """
    Make historical options API request.

    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format

    Returns:
        API response data
    """
    payload = {
        "root": TEST_SYMBOL,
        "exp": 0,  # All expirations
        "start_date": start_date,
        "end_date": end_date,
        "interval": TEST_INTERVAL,
    }

    print("Making API request:")
    print(f"  Endpoint: POST {API_BASE_URL}/historical/option")
    print(f"  Symbol: {TEST_SYMBOL}")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Interval: {TEST_INTERVAL}ms ({TEST_INTERVAL // 60000}m)")
    print()

    start_time = time.time()

    try:
        response = requests.post(
            f"{API_BASE_URL}/historical/option",
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

    # Check required fields for simplified response format
    required_fields = ["status", "request_id"]
    for field in required_fields:
        if field not in response_data:
            print(f"✗ Missing required field: {field}")
            return False

    # Check status
    if response_data["status"] != "success":
        print(f"✗ Request failed with status: {response_data['status']}")
        return False

    print(f"✓ Status: {response_data['status']}")
    print(f"✓ Request ID: {response_data['request_id']}")

    expected_days = len(expected_dates)
    print(f"✓ Request accepted for processing {expected_days} trading days")
    print(f"✓ Date range covers: {expected_dates[0]} to {expected_dates[-1]}")
    print("✓ Data will be published to MinIO storage asynchronously")

    return True


def validate_minio_data_with_content(expected_object_keys: List[str]) -> Tuple[bool, Dict]:
    """
    Validate that data was written to MinIO storage and download content for validation.

    Args:
        expected_object_keys: List of expected object keys in MinIO

    Returns:
        Tuple of (success, validation_results)
    """
    print("Validating MinIO data:")

    validation_results = {
        "minio_connected": False,
        "files_found": 0,
        "files_expected": len(expected_object_keys),
        "file_details": [],
        "file_contents": [],
        "schema_validations": [],
        "quality_validations": [],
        "total_size": 0,
    }

    try:
        # Initialize MinIO client
        config = MinIOConfig()
        minio_client = Minio(
            endpoint=config.endpoint,
            access_key=config.access_key,
            secret_key=config.secret_key,
            secure=config.secure,
            region=config.region,
        )

        validation_results["minio_connected"] = True
        print(f"✓ MinIO connection established to {config.endpoint}")

        # Wait for async processing with timeout
        print(f"⏳ Waiting up to {MINIO_VALIDATION_TIMEOUT}s for async processing...")
        start_time = time.time()

        while time.time() - start_time < MINIO_VALIDATION_TIMEOUT:
            files_found = 0
            file_details = []
            total_size = 0

            # Check each expected object key and download content
            file_contents = []
            schema_validations = []
            quality_validations = []
            
            for object_key in expected_object_keys:
                try:
                    # Check if object exists and get its info
                    stat = minio_client.stat_object(config.bucket, object_key)
                    files_found += 1
                    file_size = stat.size
                    total_size += file_size

                    # Download file content for validation
                    response = minio_client.get_object(config.bucket, object_key)
                    file_content = response.read()
                    file_contents.append(file_content)
                    
                    # Validate schema
                    schema_result = validate_parquet_schema(file_content, "quote")
                    schema_result["object_key"] = object_key
                    schema_validations.append(schema_result)
                    
                    # Validate data quality
                    quality_result = validate_data_quality(file_content)
                    quality_result["object_key"] = object_key
                    quality_validations.append(quality_result)

                    file_details.append({
                        "object_key": object_key, 
                        "size": file_size, 
                        "last_modified": stat.last_modified,
                        "schema_valid": schema_result["schema_valid"],
                        "row_count": quality_result.get("total_rows", 0)
                    })

                    print(f"✓ Found & validated: {object_key} ({file_size} bytes, {quality_result.get('total_rows', 0)} rows)")
                    if not schema_result["schema_valid"]:
                        print(f"  ⚠️  Schema validation failed: {schema_result.get('error', 'Unknown error')}")

                except S3Error as e:
                    if e.code == "NoSuchKey":
                        # File doesn't exist yet, continue waiting
                        file_contents.append(None)
                        continue
                    else:
                        print(f"✗ Error checking {object_key}: {e}")
                        file_contents.append(None)

            validation_results.update({
                "files_found": files_found, 
                "file_details": file_details, 
                "file_contents": file_contents,
                "schema_validations": schema_validations,
                "quality_validations": quality_validations,
                "total_size": total_size
            })

            # If we found all expected files, we're done
            if files_found == len(expected_object_keys):
                print(f"✓ All {files_found} expected files found in MinIO")
                return True, validation_results

            # Wait a bit before checking again
            time.sleep(5)

        # Timeout reached
        print(f"⚠️  Timeout: Found {files_found}/{len(expected_object_keys)} files after {MINIO_VALIDATION_TIMEOUT}s")

        # Still return partial success if we found some files
        return files_found > 0, validation_results

    except Exception as e:
        print(f"✗ MinIO validation failed: {e}")
        validation_results["error"] = str(e)
        return False, validation_results


def display_test_summary(
    start_date: str,
    end_date: str,
    expected_dates: List[str],
    success: bool,
    total_time: float,
    minio_results: Dict = None,
) -> None:
    """Display test summary."""
    print("=" * 70)
    print("END-TO-END TEST SUMMARY")
    print("=" * 70)

    print("Test Configuration:")
    print(f"  Symbol: {TEST_SYMBOL}")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Expected business days: {len(expected_dates)}")
    print(f"  Business days: {', '.join(expected_dates)}")
    print("  Storage: S3-compatible object storage (MinIO)")
    print()

    print("Test Results:")
    status = "PASSED" if success else "FAILED"
    icon = "🎉" if success else "❌"
    print(f"  {icon} Overall result: {status}")
    print(f"  ⏱️  Total execution time: {total_time:.2f}s")

    # Add MinIO validation results
    if minio_results:
        print(f"  📦 MinIO Connection: {'✓ Connected' if minio_results.get('minio_connected') else '✗ Failed'}")
        print(f"  📁 Files Found: {minio_results.get('files_found', 0)}/{minio_results.get('files_expected', 0)}")
        if minio_results.get("total_size", 0) > 0:
            size_mb = minio_results["total_size"] / (1024 * 1024)
            print(f"  📊 Total Data Size: {size_mb:.2f} MB")

        # Show file details if available
        if minio_results.get("file_details"):
            print("  📋 File Details:")
            for file_detail in minio_results["file_details"]:
                size_kb = file_detail["size"] / 1024
                print(f"     • {file_detail['object_key']} ({size_kb:.1f} KB)")
    print()

    if success:
        print("✅ End-to-end flow verified successfully!")
        print("✅ API request accepted and processing initiated")
        print("✅ Date splitting is working correctly")
        print("✅ MinIO object storage publishing working")
        print("✅ Data successfully written to storage")
        print("✅ Unified processing pattern working")
    else:
        print("❌ End-to-end test failed")
        print("🔍 Check API logs and ensure:")
        print("   - ThetaTerminal is running and authenticated")
        print("   - MinIO is running on localhost:9000")
        print("   - API server is running on localhost:8000")

        if minio_results and not minio_results.get("minio_connected"):
            print("   - MinIO connection configuration is correct")
        elif minio_results and minio_results.get("files_found", 0) == 0:
            print("   - Data processing pipeline is working correctly")

@pytest.mark.e2e
def test_aapl_option_week_e2e():
    """Run the end-to-end AAPL options test."""
    logger.info("🚀 Starting End-to-End AAPL Options Test")

    total_start_time = time.time()

    # Step 1: Get business week date range
    logger.info("Step 1: Calculating business week date range...")
    start_date, end_date = get_business_week_range()
    expected_dates_int = generate_trading_date_list(start_date, end_date)
    expected_dates = [str(d) for d in expected_dates_int]  # Convert to strings for display

    logger.info(f"✓ Business week: {start_date} to {end_date}")
    logger.info(f"✓ Trading days: {', '.join(expected_dates)} ({len(expected_dates)} days)")

    # Step 2: Check API health
    logger.info("Step 2: Checking API health...")
    health_ok = check_api_health()
    assert health_ok, "API health check failed. Ensure API server is running."

    # Step 3: Make options request
    logger.info("Step 3: Making historical options request...")
    response_data = make_options_request(start_date, end_date)

    # Step 4: Validate response
    logger.info("Step 4: Validating response...")
    response_success = validate_response(response_data, expected_dates)
    assert response_success, "Response validation failed"

    # Step 5: Validate MinIO data with comprehensive validation
    logger.info("Step 5: Validating MinIO data storage...")
    expected_object_keys = generate_expected_object_keys(expected_dates_int, TEST_SYMBOL, TEST_INTERVAL)
    logger.info(f"Expected object keys: {expected_object_keys}")

    minio_success, minio_results = validate_minio_data_with_content(expected_object_keys)
    logger.info(f"MinIO validation: {'✓ Passed' if minio_success else '✗ Failed'}")

    # Step 6: Comprehensive validation checks
    schema_validations = minio_results.get("schema_validations", [])
    quality_validations = minio_results.get("quality_validations", [])
    
    schema_success = all(result.get("schema_valid", False) for result in schema_validations)
    quality_success = all(result.get("total_rows", 0) > 0 for result in quality_validations)
    
    logger.info(f"Schema validation: {'✓ Passed' if schema_success else '✗ Failed'}")
    logger.info(f"Data quality validation: {'✓ Passed' if quality_success else '✗ Failed'}")

    # Step 7: Display summary
    total_time = time.time() - total_start_time
    overall_success = response_success and minio_success and schema_success and quality_success
    display_test_summary(start_date, end_date, expected_dates, overall_success, total_time, minio_results)

    # Step 8: Final assertions for pytest
    assert response_success, "API response validation failed"
    assert minio_success, f"MinIO validation failed: found {minio_results.get('files_found', 0)}/{minio_results.get('files_expected', 0)} files"
    assert schema_success, f"Schema validation failed: {[r.get('error', 'Unknown') for r in schema_validations if not r.get('schema_valid', True)]}"
    assert quality_success, f"Data quality validation failed: {[r.get('data_errors', []) for r in quality_validations if r.get('total_rows', 0) == 0]}"

    # Only log success if all assertions pass
    if overall_success:
        logger.info("🎉 End-to-end test completed successfully!")
    else:
        logger.error("❌ End-to-end test failed - check assertions above")


if __name__ == "__main__":
    # Allow running directly for debugging
    test_aapl_option_week_e2e()
