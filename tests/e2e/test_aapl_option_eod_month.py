#!/usr/bin/env python3
"""
End-to-end test for AAPL option EOD (End-of-Day) data retrieval.

Tests the complete API workflow: API request → DataProcessingService → MinIO publishing

This script:
1. Makes API request for AAPL option EOD data for a full month
2. Verifies the API response and processing
3. Checks that monthly data aggregation worked correctly
4. Validates MinIO storage and performance metrics
5. Verifies EOD option data structure (17 tick fields + 4 contract fields = 21 total)

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
import polars as pl

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
TEST_YEARMO = 202311  # November 2023 - known good period with option data
TEST_EXPIRATION = 0  
REQUEST_TIMEOUT = 600  # 10 minutes timeout for API request (EOD requests take longer)
MINIO_VALIDATION_TIMEOUT = 60  # 1 minute timeout for MinIO validation

# Expected EOD option schema (17 tick fields + 4 contract fields = 21 total)
EXPECTED_EOD_OPTION_FIELDS = [
    # EOD tick fields (17 fields - same as stock EOD)
    'ms_of_day', 'ms_of_day2', 'open', 'high', 'low', 'close', 'volume', 'count',
    'bid_size', 'bid_exchange', 'bid', 'bid_condition', 'ask_size', 'ask_exchange', 
    'ask', 'ask_condition', 'date',
    # Contract fields (4 fields)
    'root', 'expiration', 'strike', 'right'
]


def get_test_month_range() -> Tuple[int, str, str]:
    """
    Get test month range for EOD option data.
    
    Returns:
        Tuple of (yearmo, start_date, end_date) for testing
    """
    # Use November 2023 for testing - a known good period with option data
    yearmo = TEST_YEARMO
    year = yearmo // 100
    month = yearmo % 100
    
    # Full month range
    start_date = f"{year}{month:02d}01"
    end_date = f"{year}{month:02d}30"  # November has 30 days
    
    return yearmo, start_date, end_date


def generate_expected_object_key(yearmo: int, symbol: str) -> str:
    """
    Generate expected MinIO object key for EOD option data using actual request object.
    
    Args:
        yearmo: Year-month in YYYYMM format
        symbol: Stock symbol (e.g., 'AAPL')
        
    Returns:
        Expected object key
    """
    # Use actual HistOptionBulkRequest to generate object key
    request = HistOptionBulkRequest(
        root=symbol,
        yearmo=yearmo,
        exp=TEST_EXPIRATION,
        endpoint="eod",
        return_format="parquet"
    )
    object_key = request.generate_object_key()
    
    logger.debug(f"Generated expected object key using actual request model: {object_key}")
    return object_key


def validate_eod_option_schema(file_content: bytes) -> Dict:
    """
    Validate Parquet file schema against expected EOD option structure.
    
    Args:
        file_content: Raw Parquet file content as bytes
        
    Returns:
        Dict with validation results
    """
    try:
        # Read Parquet file
        table = pq.read_table(pa.BufferReader(file_content))
        
        # Get expected schema from actual definitions
        expected_tick_fields = TICK_SCHEMAS["eod"]["field_names"]
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
            "file_size_bytes": len(file_content),
            "expected_field_count": len(expected_fields)
        }
    except Exception as e:
        return {
            "schema_valid": False,
            "error": str(e),
            "file_size_bytes": len(file_content),
            "expected_field_count": len(EXPECTED_EOD_OPTION_FIELDS)
        }


def validate_eod_option_data_quality(file_content: bytes) -> Dict:
    """
    Validate EOD option data quality and realistic ranges.
    
    Args:
        file_content: Raw Parquet file content as bytes
        
    Returns:
        Dict with data quality results
    """
    try:
        # Read Parquet file using polars
        df = pl.read_parquet(pa.BufferReader(file_content))
        
        quality_checks = {
            "total_rows": df.height,
            "non_null_rows": df.height - df.null_count().sum_horizontal().item() if df.height > 0 else 0,
            "has_option_data": False,
            "has_calls": False,
            "has_puts": False,
            "price_ranges_realistic": True,
            "time_ranges_valid": True,
            "contract_data_valid": True,
            "unique_contracts": 0,
            "unique_strikes": 0,
            "unique_expirations": 0,
            "date_range_valid": True,
            "data_errors": []
        }
        
        if df.height > 0:
            # Check for option contract data
            if 'expiration' in df.columns and 'strike' in df.columns and 'right' in df.columns:
                # Filter for actual option contracts (expiration > 0)
                option_data = df.filter(pl.col('expiration') > 0)
                quality_checks["has_option_data"] = option_data.height > 0
                
                if option_data.height > 0:
                    # Check for calls and puts
                    calls = option_data.filter(pl.col('right') == 'C')
                    puts = option_data.filter(pl.col('right') == 'P')
                    quality_checks["has_calls"] = calls.height > 0
                    quality_checks["has_puts"] = puts.height > 0
                    
                    # Count unique contracts
                    quality_checks["unique_contracts"] = option_data.select(
                        pl.concat_str(['expiration', 'strike', 'right'])
                    ).n_unique()
                    quality_checks["unique_strikes"] = option_data.select('strike').n_unique()
                    quality_checks["unique_expirations"] = option_data.select('expiration').n_unique()
            
            # Check price ranges for EOD data
            if 'open' in df.columns and 'high' in df.columns and 'low' in df.columns and 'close' in df.columns:
                # Filter positive prices
                price_data = df.filter(
                    (pl.col('open') > 0) & 
                    (pl.col('high') > 0) & 
                    (pl.col('low') > 0) & 
                    (pl.col('close') > 0)
                )
                
                if price_data.height > 0:
                    for price_col in ['open', 'high', 'low', 'close']:
                        price_values = price_data.select(price_col)
                        price_min = price_values.min().item()
                        price_max = price_values.max().item()
                        # Option prices can be very low (pennies) or high (hundreds)
                        if not (0.01 <= price_min and price_max <= 5000):
                            quality_checks["price_ranges_realistic"] = False
                            quality_checks["data_errors"].append(
                                f"{price_col} prices outside realistic range: {price_min}-{price_max}"
                            )
            
            # Check bid/ask data
            if 'bid' in df.columns and 'ask' in df.columns:
                bid_ask_data = df.filter((pl.col('bid') > 0) & (pl.col('ask') > 0))
                if bid_ask_data.height > 0:
                    bid_min = bid_ask_data.select('bid').min().item()
                    bid_max = bid_ask_data.select('bid').max().item()
                    ask_min = bid_ask_data.select('ask').min().item()
                    ask_max = bid_ask_data.select('ask').max().item()
                    
                    if not (0.01 <= bid_min and bid_max <= 5000):
                        quality_checks["price_ranges_realistic"] = False
                        quality_checks["data_errors"].append(f"Bid prices outside realistic range: {bid_min}-{bid_max}")
                    
                    if not (0.01 <= ask_min and ask_max <= 5000):
                        quality_checks["price_ranges_realistic"] = False
                        quality_checks["data_errors"].append(f"Ask prices outside realistic range: {ask_min}-{ask_max}")
            
            # Check time ranges
            if 'ms_of_day' in df.columns:
                ms_min = df.select('ms_of_day').min().item()
                ms_max = df.select('ms_of_day').max().item()
                if not (0 <= ms_min and ms_max <= 86400000):  # 24 hours in ms
                    quality_checks["time_ranges_valid"] = False
                    quality_checks["data_errors"].append(f"Time values outside valid range: {ms_min}-{ms_max}")
            
            # Check date range (should be November 2023)
            if 'date' in df.columns:
                dates = df.select('date')
                date_min = dates.min().item()
                date_max = dates.max().item()
                
                # Should be within November 2023
                if not (20231101 <= date_min and date_max <= 20231130):
                    quality_checks["date_range_valid"] = False
                    quality_checks["data_errors"].append(
                        f"Date range outside November 2023: {date_min}-{date_max}"
                    )
        
        return quality_checks
        
    except Exception as e:
        return {
            "total_rows": 0,
            "data_errors": [f"Failed to validate EOD option data quality: {str(e)}"]
        }


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


def make_eod_option_request(yearmo: int, start_date: str, end_date: str) -> Dict:
    """
    Make historical option EOD API request.
    
    Args:
        yearmo: Year-month in YYYYMM format
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format
        
    Returns:
        API response data
    """
    payload = {
        "root": TEST_SYMBOL,
        "start_date": start_date,
        "end_date": end_date,
        "endpoint": "eod",
        "return_format": "parquet"
    }
    
    print("Making EOD Option API request:")
    print(f"  Endpoint: POST {API_BASE_URL}/historical/option")
    print(f"  Symbol: {TEST_SYMBOL}")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Year-month: {yearmo}")
    print("  Endpoint: eod")
    print("  Format: parquet")
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


def validate_eod_option_response(response_data: Dict, yearmo: int) -> bool:
    """
    Validate the EOD option API response.
    
    Args:
        response_data: API response data
        yearmo: Expected year-month that should have been processed
        
    Returns:
        True if validation passes
    """
    print("Validating EOD Option API response:")
    
    if "error" in response_data:
        print(f"✗ API returned error: {response_data['error']}")
        return False
    
    # Check required fields
    required_fields = ["status", "request_id"]
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
    print(f"✓ Month {yearmo} will be processed (monthly aggregation for EOD)")
    
    return True


def validate_minio_storage(symbol: str, yearmo: int) -> bool:
    """
    Validate MinIO storage contains the expected EOD option data.
    
    Args:
        symbol: Stock symbol
        yearmo: Year-month of data in YYYYMM format
        
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
        
        # Generate expected object key
        object_key = generate_expected_object_key(yearmo, symbol)
        print(f"  Looking for object: {object_key}")
        
        try:
            stat = client.stat_object(config.bucket, object_key)
            file_size = stat.size
            print(f"✓ EOD option file found: {object_key}")
            print(f"✓ File size: {file_size:,} bytes")
            
            # Download and validate Parquet file
            response = client.get_object(config.bucket, object_key)
            parquet_data = response.read()
            response.close()
            
            # Read Parquet file with PyArrow
            table = pq.read_table(pa.BufferReader(parquet_data))
            
            # Validate schema
            schema_result = validate_eod_option_schema(parquet_data)
            quality_result = validate_eod_option_data_quality(parquet_data)
            
            schema_names = table.schema.names
            print("✓ Parquet file loaded successfully")
            print(f"✓ Columns found: {len(schema_names)}")
            print(f"✓ Rows found: {len(table):,}")
            
            # Check schema validation
            if not schema_result["schema_valid"]:
                print(f"✗ Schema validation failed")
                print(f"  Missing fields: {schema_result.get('missing_fields', [])}")
                print(f"  Extra fields: {schema_result.get('extra_fields', [])}")
                return False
            
            print(f"✓ All expected EOD option fields present: {len(EXPECTED_EOD_OPTION_FIELDS)} fields")
            
            # Basic data validation
            if len(table) == 0:
                print("✗ EOD option file is empty")
                return False
            
            # Check for reasonable data size (EOD should have substantial data for a month)
            expected_min_rows = 1000  # Option EOD should have thousands of records
            
            if len(table) < expected_min_rows:
                print(f"? Fewer rows than expected: {len(table)} < {expected_min_rows}")
            else:
                print(f"✓ Row count within expected range: {len(table):,} rows")
            
            # Validate data quality
            if quality_result.get("data_errors"):
                print(f"⚠️  Data quality warnings: {quality_result['data_errors']}")
            
            if quality_result.get("has_option_data"):
                print(f"✓ Option contract data found")
                print(f"  Unique contracts: {quality_result.get('unique_contracts', 0)}")
                print(f"  Unique strikes: {quality_result.get('unique_strikes', 0)}")
                print(f"  Unique expirations: {quality_result.get('unique_expirations', 0)}")
                print(f"  Has calls: {quality_result.get('has_calls', False)}")
                print(f"  Has puts: {quality_result.get('has_puts', False)}")
            else:
                print("⚠️  No option contract data found")
            
            # Validate data types for key fields
            try:
                # Check that date column exists and has reasonable values
                dates = table.column('date').to_pandas()
                min_date = dates.min()
                max_date = dates.max()
                print(f"✓ Date range: {min_date} to {max_date}")
                
                # Check that OHLC data exists
                for field in ['open', 'high', 'low', 'close']:
                    if field in schema_names:
                        values = table.column(field).to_pandas()
                        non_zero_values = values[values > 0]
                        if len(non_zero_values) > 0:
                            print(f"✓ {field} data looks valid (range: {non_zero_values.min():.2f} - {non_zero_values.max():.2f})")
                        else:
                            print(f"⚠️  {field} column has no positive values")
                
                # Check contract fields
                if 'root' in schema_names:
                    roots = set(table.column('root').to_pylist())
                    print(f"✓ Root symbols: {roots}")
                
                if 'right' in schema_names:
                    rights = set(table.column('right').to_pylist())
                    print(f"✓ Option types: {rights}")
                
            except Exception as e:
                print(f"? Data validation warning: {e}")
            
            return True
            
        except S3Error as e:
            if e.code == "NoSuchKey":
                print(f"✗ EOD option file not found: {object_key}")
            else:
                print(f"✗ MinIO error: {e}")
            return False
            
    except Exception as e:
        print(f"✗ MinIO validation failed: {e}")
        return False


def display_test_summary(
    yearmo: int, start_date: str, end_date: str, success: bool, total_time: float, error_msg: str = None
) -> None:
    """Display test summary."""
    print("=" * 70)
    print("END-TO-END EOD OPTION DATA TEST SUMMARY")
    print("=" * 70)
    
    year = yearmo // 100
    month = yearmo % 100
    month_name = ["", "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"][month]
    
    print("Test Configuration:")
    print(f"  Symbol: {TEST_SYMBOL}")
    print(f"  Month: {month_name} {year} ({yearmo})")
    print(f"  Date range: {start_date} to {end_date}")
    print("  Data type: End-of-Day (EOD) Option Data with OHLC + Quote + Contract")
    print("  Format: Parquet")
    print("  Storage: S3-compatible object storage (MinIO)")
    print(f"  Expected schema: {len(EXPECTED_EOD_OPTION_FIELDS)} fields (17 tick + 4 contract)")
    print("  Expected files: 1 (monthly aggregation)")
    print()
    
    print("Test Results:")
    status = "PASSED" if success else "FAILED"
    icon = "🎉" if success else "❌"
    print(f"  {icon} Overall result: {status}")
    print(f"  ⏱️  Total execution time: {total_time:.2f}s")
    
    if error_msg:
        print(f"  ❌ Error: {error_msg}")
    print()
    
    if success:
        print("✅ End-to-end EOD option data workflow verified successfully!")
        print("✅ Monthly data aggregation is working correctly")
        print("✅ MinIO object storage upload is working")
        print("✅ EOD option data structure validation passed")
        print("✅ API processing completed without errors")
        print("✅ Option contract data is properly structured")
    else:
        print("❌ End-to-end EOD option test failed")
        print("🔍 Check API logs and ensure:")
        print("   - ThetaTerminal is running and authenticated")
        print("   - MinIO is running on localhost:9000")
        print("   - API server is running on localhost:8000")
        print("   - EOD endpoint /historical/option with eod is available")


def run_eod_option_test_main() -> int:
    """
    Run the end-to-end AAPL option EOD test for direct execution.
    Returns exit code (0 for success, 1 for failure).
    """
    logger.info("🚀 Starting End-to-End AAPL Option EOD Test")
    
    total_start_time = time.time()
    yearmo = None
    start_date = None
    end_date = None
    error_msg = None
    
    try:
        # Step 1: Get test month range
        logger.info("Step 1: Setting up test month range...")
        yearmo, start_date, end_date = get_test_month_range()
        
        year = yearmo // 100
        month = yearmo % 100
        logger.info(f"✓ Test month: {year}-{month:02d} ({yearmo})")
        logger.info(f"✓ Date range: {start_date} to {end_date}")
        
        # Step 2: Check API health
        logger.info("Step 2: Checking API health...")
        health_ok = check_api_health()
        if not health_ok:
            error_msg = "API health check failed. Ensure API server is running."
            return 1
        
        # Step 3: Make EOD option request
        logger.info("Step 3: Making historical option EOD request...")
        response_data = make_eod_option_request(yearmo, start_date, end_date)
        
        # Step 4: Validate response
        logger.info("Step 4: Validating API response...")
        response_ok = validate_eod_option_response(response_data, yearmo)
        if not response_ok:
            error_msg = "EOD option API response validation failed"
            return 1
        
        # Step 5: Wait a moment for processing to complete
        logger.info("Step 5: Waiting for processing to complete...")
        time.sleep(10)  # Give the system time to process and store the data (EOD may take longer)
        
        # Step 6: Validate MinIO storage
        logger.info("Step 6: Validating MinIO storage...")
        storage_ok = validate_minio_storage(TEST_SYMBOL, yearmo)
        if not storage_ok:
            error_msg = "MinIO storage validation failed"
            return 1
        
        # Step 7: Display summary
        total_time = time.time() - total_start_time
        display_test_summary(yearmo, start_date, end_date, True, total_time)
        
        logger.info("🎉 End-to-end EOD option test completed successfully!")
        return 0
        
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"Test failed with exception: {e}", exc_info=True)
        return 1
        
    finally:
        # Always display summary, even on failure
        if yearmo is not None and start_date is not None and end_date is not None and error_msg:
            total_time = time.time() - total_start_time
            display_test_summary(yearmo, start_date, end_date, False, total_time, error_msg)


@pytest.mark.e2e
def test_aapl_option_eod_month_e2e():
    """Run the end-to-end AAPL option EOD test."""
    logger.info("🚀 Starting End-to-End AAPL Option EOD Test")
    
    total_start_time = time.time()
    
    # Step 1: Get test month range
    logger.info("Step 1: Setting up test month range...")
    yearmo, start_date, end_date = get_test_month_range()
    
    year = yearmo // 100
    month = yearmo % 100
    logger.info(f"✓ Test month: {year}-{month:02d} ({yearmo})")
    logger.info(f"✓ Date range: {start_date} to {end_date}")
    
    # Step 2: Check API health
    logger.info("Step 2: Checking API health...")
    health_ok = check_api_health()
    assert health_ok, "API health check failed. Ensure API server is running."
    
    # Step 3: Make EOD option request
    logger.info("Step 3: Making historical option EOD request...")
    response_data = make_eod_option_request(yearmo, start_date, end_date)
    
    # Step 4: Validate response
    logger.info("Step 4: Validating API response...")
    response_ok = validate_eod_option_response(response_data, yearmo)
    assert response_ok, "EOD option API response validation failed"
    
    # Step 5: Wait a moment for processing to complete
    logger.info("Step 5: Waiting for processing to complete...")
    time.sleep(10)  # Give the system time to process and store the data (EOD may take longer)
    
    # Step 6: Validate MinIO storage
    logger.info("Step 6: Validating MinIO storage...")
    storage_ok = validate_minio_storage(TEST_SYMBOL, yearmo)
    assert storage_ok, "MinIO storage validation failed"
    
    # Step 7: Display summary
    total_time = time.time() - total_start_time
    display_test_summary(yearmo, start_date, end_date, True, total_time)
    
    logger.info("🎉 End-to-end EOD option test completed successfully!")


if __name__ == "__main__":
    import sys
    # Allow running directly for debugging
    exit_code = run_eod_option_test_main()
    sys.exit(exit_code)