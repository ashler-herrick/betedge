"""
Shared utilities for E2E tests with async background job support.

This module provides common functions for testing the async background job
architecture, including job polling, response validation, and status monitoring.
"""

import time
from typing import Dict, Optional, Tuple
import requests
import logging

logger = logging.getLogger(__name__)

# Default timeouts for E2E tests
DEFAULT_JOB_TIMEOUT = 300  # 5 minutes for job completion
DEFAULT_POLL_INTERVAL = 5  # 5 seconds between status checks


def validate_async_response(response_data: Dict, context: str = "request") -> bool:
    """
    Validate async API response format.

    Args:
        response_data: API response data
        context: Context description for logging

    Returns:
        True if validation passes
    """
    print(f"Validating async {context} response:")

    if "error" in response_data:
        print(f"✗ API returned error: {response_data['error']}")
        return False

    # Check required fields for async response
    required_fields = ["status", "job_id", "message", "status_url"]
    for field in required_fields:
        if field not in response_data:
            print(f"✗ Missing required field: {field}")
            return False

    # Check status is "accepted"
    if response_data["status"] != "accepted":
        print(f"✗ Expected status 'accepted', got: {response_data['status']}")
        return False

    print(f"✓ Status: {response_data['status']}")
    print(f"✓ Job ID: {response_data['job_id']}")
    print(f"✓ Status URL: {response_data['status_url']}")
    print(f"✓ Message: {response_data['message']}")

    return True


def poll_job_status(
    api_base_url: str, job_id: str, timeout: int = DEFAULT_JOB_TIMEOUT, poll_interval: int = DEFAULT_POLL_INTERVAL
) -> Optional[Dict]:
    """
    Poll job status until completion or timeout.

    Args:
        api_base_url: Base URL for the API
        job_id: Job ID to poll
        timeout: Maximum time to wait in seconds
        poll_interval: Seconds between status checks

    Returns:
        Final job status dict if completed, None if timeout
    """
    print(f"⏳ Polling job {job_id} for completion (timeout: {timeout}s)...")

    start_time = time.time()
    last_progress = -1

    while time.time() - start_time < timeout:
        try:
            response = requests.get(f"{api_base_url}/jobs/{job_id}", timeout=10)

            if response.status_code == 200:
                job_data = response.json()
                status = job_data.get("status")
                progress = job_data.get("progress", {})
                completed = progress.get("completed", 0)
                total = progress.get("total", 0)
                percentage = progress.get("percentage", 0)

                # Show progress updates
                if percentage != last_progress:
                    print(f"  📊 Progress: {completed}/{total} items ({percentage:.1f}%)")
                    last_progress = percentage

                # Check if job is finished
                if status in ("completed", "failed"):
                    elapsed = time.time() - start_time
                    if status == "completed":
                        print(f"✓ Job completed successfully in {elapsed:.1f}s")
                    else:
                        error_msg = job_data.get("error_message", "Unknown error")
                        print(f"✗ Job failed after {elapsed:.1f}s: {error_msg}")

                    return job_data

            elif response.status_code == 404:
                print(f"✗ Job {job_id} not found")
                return None
            else:
                print(f"⚠️ Status check failed: {response.status_code}")

        except Exception as e:
            print(f"⚠️ Error checking job status: {e}")

        time.sleep(poll_interval)

    elapsed = time.time() - start_time
    print(f"⚠️ Job polling timed out after {elapsed:.1f}s")
    return None


def wait_for_job_completion(api_base_url: str, job_id: str, timeout: int = DEFAULT_JOB_TIMEOUT) -> bool:
    """
    Wait for job completion and return success status.

    Args:
        api_base_url: Base URL for the API
        job_id: Job ID to wait for
        timeout: Maximum time to wait in seconds

    Returns:
        True if job completed successfully, False otherwise
    """
    job_data = poll_job_status(api_base_url, job_id, timeout)

    if job_data is None:
        return False

    return job_data.get("status") == "completed"


def validate_job_completion_for_minio(
    api_base_url: str, job_id: str, expected_items: int, timeout: int = DEFAULT_JOB_TIMEOUT
) -> Tuple[bool, Dict]:
    """
    Validate job completion specifically for MinIO data validation.

    Args:
        api_base_url: Base URL for the API
        job_id: Job ID to validate
        expected_items: Expected number of items to be processed
        timeout: Maximum time to wait in seconds

    Returns:
        Tuple of (success, job_data)
    """
    print("🔍 Validating job completion for MinIO data validation...")

    job_data = poll_job_status(api_base_url, job_id, timeout)

    if job_data is None:
        print("✗ Job status polling failed")
        return False, {}

    status = job_data.get("status")
    progress = job_data.get("progress", {})
    completed = progress.get("completed", 0)
    total = progress.get("total", 0)

    if status != "completed":
        error_msg = job_data.get("error_message", "Unknown error")
        print(f"✗ Job failed or incomplete: {status} - {error_msg}")
        return False, job_data

    if total != expected_items:
        print(f"⚠️ Expected {expected_items} items but job processed {total}")

    if completed != total:
        print(f"⚠️ Job marked complete but {completed}/{total} items processed")

    print(f"✓ Job completed: {completed}/{total} items processed")
    return True, job_data


def display_job_timing_info(job_data: Dict, total_test_time: float) -> None:
    """
    Display job timing information in test summaries.

    Args:
        job_data: Job status data from API
        total_test_time: Total test execution time
    """
    if not job_data:
        return

    created_at = job_data.get("created_at", "")
    updated_at = job_data.get("updated_at", "")
    progress = job_data.get("progress", {})

    print("Job Processing Info:")
    print(f"  🆔 Job ID: {job_data.get('job_id', 'N/A')}")
    print(f"  📊 Final Progress: {progress.get('completed', 0)}/{progress.get('total', 0)} items")

    if created_at and updated_at:
        try:
            from datetime import datetime

            created = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
            updated = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
            job_duration = (updated - created).total_seconds()
            print(f"  ⏱️ Job Duration: {job_duration:.1f}s")
        except:
            pass

    print(f"  🕐 Total Test Time: {total_test_time:.1f}s")
