from typing import Dict, Tuple
import requests
import time
import pytest

from tests.e2e.utils import (
    validate_async_response,
    validate_job_completion_for_minio,
    display_job_timing_info,
)

# Test configuration
API_BASE_URL = "http://localhost:8000"
TEST_SYMBOL = "AAPL"
TEST_INTERVAL = 450_000  # 1 minute intervals


def make_ohlc_request() -> Dict:
    """
    Make historical stock EOD API request.

    Args:
        start_year: Start year for EOD data
        end_year: End year for EOD data

    Returns:
        API response data
    """
    # Convert years to date format expected by unified endpoint
    start_date = f"20230101"
    end_date = f"20230201"

    payload = {
        "root": "AAPL",
        "start_date": start_date,
        "end_date": end_date,
        "schema": "ohlc",
        "interval": 4_500_000,
        "return_format": "parquet",
    }

    print("Making EOD API request:")
    print(f"  Endpoint: POST {API_BASE_URL}/historical/stock")
    print(f"  Symbol: {TEST_SYMBOL}")
    print(f"  Date range: {start_date} to {end_date}")
    print("  Endpoint: ohlc")
    print("  Format: parquet")
    print()

    start_time = time.time()

    try:
        response = requests.post(
            f"{API_BASE_URL}/historical/stock",
            json=payload,
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

    except Exception as e:
        print(f"  Error: {e}")
        return {"error": str(e)}


@pytest.mark.e2e
def test_aapl_ohlc_e2e():
    """Run the end-to-end AAPL OHLC test."""
    print("🚀 Starting End-to-End AAPL OHLC Test")
    
    total_start_time = time.time()
    
    # Step 1: Check API health
    print("Step 1: Checking API health...")
    try:
        response = requests.get(f"{API_BASE_URL}/health", timeout=10)
        assert response.status_code == 200, "API health check failed"
        print("✓ API is healthy")
    except Exception as e:
        assert False, f"API health check failed: {e}"
    
    # Step 2: Make OHLC request
    print("Step 2: Making OHLC request...")
    response_data = make_ohlc_request()
    
    # Step 3: Validate async response
    print("Step 3: Validating async response...")
    success = validate_async_response(response_data, "OHLC request")
    assert success, "Async response validation failed"
    
    job_id = response_data.get("job_id", "")
    assert job_id, "No job ID received from async response"
    
    # Step 4: Wait for background job completion
    print("Step 4: Waiting for background job completion...")
    job_success, job_data = validate_job_completion_for_minio(
        API_BASE_URL, job_id, 1  # OHLC typically processes 1 month
    )
    assert job_success, "Background job did not complete successfully"
    
    # Step 5: Display summary
    total_time = time.time() - total_start_time
    print("=" * 50)
    print("END-TO-END OHLC TEST SUMMARY")
    print("=" * 50)
    print("Test Configuration:")
    print(f"  Symbol: {TEST_SYMBOL}")
    print("  Data type: OHLC")
    print("  Date range: 2023-01-01 to 2023-02-01")
    print()
    
    if job_data:
        display_job_timing_info(job_data, total_time)
    else:
        print(f"  🕐 Total Test Time: {total_time:.1f}s")
    
    print("\n✅ End-to-end OHLC workflow verified successfully!")
    print("✅ Async background job processing working")
    print("✅ Job completion tracking working")
    print("🎉 OHLC test completed successfully!")


if __name__ == "__main__":
    # Allow running directly for debugging
    test_aapl_ohlc_e2e()
