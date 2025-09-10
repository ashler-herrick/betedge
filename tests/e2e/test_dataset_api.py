"""
Simple E2E test for the dataset API workflow.
"""

import json
import time

import pytest
import requests


API_BASE_URL = "http://localhost:8000"
TEST_TIMEOUT = 30  # seconds


def test_dataset_request_workflow():
    """
    Test complete dataset request workflow:
    1. Request a dataset via historical stock endpoint
    2. Poll job status until completion
    3. Verify job completes successfully
    """
    # Test historical stock data request using existing API
    request_data = {
        "root": "AAPL",
        "start_date": "20240101",
        "end_date": "20240131",
        "schema": "ohlc",
        "interval": 86400000,  # daily
        "return_format": "parquet",
    }

    print(f"Making dataset request to {API_BASE_URL}/historical/stock")
    print(f"Request data: {json.dumps(request_data, indent=2)}")

    # Step 1: Request dataset
    response = requests.post(f"{API_BASE_URL}/historical/stock", json=request_data, timeout=10)

    print(f"Response status: {response.status_code}")
    print(f"Response headers: {dict(response.headers)}")

    # Should be 202 Accepted for new job
    assert response.status_code == 202

    response_data = response.json()
    print(f"Response data: {json.dumps(response_data, indent=2)}")

    job_id = response_data["job_id"]
    status_url = response_data["status_url"]

    assert response_data["status"] == "accepted"
    assert "job_id" in response_data
    assert "status_url" in response_data

    # Step 2: Poll job status until completion
    print(f"\nPolling job status at {API_BASE_URL}{status_url}")

    start_time = time.time()
    max_polls = TEST_TIMEOUT  # Poll once per second

    for poll_count in range(max_polls):
        if time.time() - start_time > TEST_TIMEOUT:
            pytest.fail(f"Job {job_id} did not complete within {TEST_TIMEOUT} seconds")

        time.sleep(1)  # Wait 1 second between polls

        status_response = requests.get(f"{API_BASE_URL}{status_url}", timeout=10)
        assert status_response.status_code == 200

        status_data = status_response.json()
        current_status = status_data["status"]
        progress_pct = status_data["progress"]["percentage"]
        print(f"Poll {poll_count + 1}: {current_status} ({progress_pct:.1f}%)")

        if current_status == "completed":
            print(f"Job completed successfully after {poll_count + 1} polls")
            print(f"Final progress: {status_data['progress']}")
            return

        elif current_status == "failed":
            error_msg = status_data.get("error_message", "Unknown error")
            pytest.fail(f"Job {job_id} failed: {error_msg}")

    pytest.fail(f"Job {job_id} did not complete within {TEST_TIMEOUT} seconds")


def test_different_requests():
    """
    Test that different requests create different job IDs.
    """
    request_data1 = {
        "root": "TSLA",
        "start_date": "20240101",
        "end_date": "20240131",
        "schema": "ohlc",
        "interval": 86400000,
        "return_format": "parquet",
    }

    request_data2 = {
        "root": "MSFT",  # Different symbol
        "start_date": "20240101",
        "end_date": "20240131",
        "schema": "ohlc",
        "interval": 86400000,
        "return_format": "parquet",
    }

    print("Testing different requests")

    # Make first request
    response1 = requests.post(f"{API_BASE_URL}/historical/stock", json=request_data1, timeout=10)
    print(f"First request status: {response1.status_code}")
    assert response1.status_code == 202
    job_id1 = response1.json()["job_id"]

    # Make second different request
    response2 = requests.post(f"{API_BASE_URL}/historical/stock", json=request_data2, timeout=10)
    print(f"Second request status: {response2.status_code}")
    assert response2.status_code == 202
    job_id2 = response2.json()["job_id"]

    # Should return different job IDs
    assert job_id1 != job_id2
    print(f"Different requests returned different job IDs: {job_id1} vs {job_id2}")


def test_health_endpoint():
    """Test that health check endpoint works."""
    response = requests.get(f"{API_BASE_URL}/health", timeout=10)

    assert response.status_code == 200
    response_data = response.json()
    assert "status" in response_data
    assert response_data["status"] == "healthy"
    print("Health check passed")


def test_job_status_not_found():
    """Test job status endpoint with non-existent job."""
    fake_job_id = "00000000-0000-0000-0000-000000000000"

    response = requests.get(f"{API_BASE_URL}/jobs/{fake_job_id}", timeout=10)

    assert response.status_code == 404
    assert "Job not found" in response.json()["detail"]
    print("Job not found test passed")


if __name__ == "__main__":
    # For running individual tests during development
    test_health_endpoint()
    test_dataset_request_workflow()
    test_different_requests()
    test_job_status_not_found()
    print("All tests passed!")
