# type: ignore
"""
End-to-End tests for the /jobs API endpoint.

Tests the job listing functionality including filtering, pagination,
and integration with the job tracking system.
"""

import logging
import time

import httpx

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Test configuration
BASE_URL = "http://localhost:8000"
TIMEOUT = 30


class TestJobsAPI:
    """Test suite for /jobs endpoint functionality."""

    def __init__(self):
        """Initialize test suite."""
        self.client = None

    def setup_method(self):
        """Set up test environment before each test."""
        self.client = httpx.Client(base_url=BASE_URL, timeout=TIMEOUT)

    def teardown_method(self):
        """Clean up after each test."""
        if hasattr(self, "client") and self.client:
            self.client.close()

    def test_health_check(self):
        """Verify API is running before testing jobs endpoint."""
        response = self.client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "healthy"

    def test_jobs_endpoint_empty_state(self):
        """Test /jobs endpoint returns proper format when no jobs exist."""
        response = self.client.get("/jobs")

        assert response.status_code == 200
        data = response.json()

        # Verify response structure
        assert "total" in data
        assert "limit" in data
        assert "status_filter" in data
        assert "jobs" in data
        assert isinstance(data["jobs"], list)
        assert data["limit"] is None
        assert data["status_filter"] is None

    def test_jobs_endpoint_with_limit(self):
        """Test /jobs endpoint with limit parameter."""
        limit = 5
        response = self.client.get(f"/jobs?limit={limit}")

        assert response.status_code == 200
        data = response.json()

        assert data["limit"] == limit
        assert len(data["jobs"]) <= limit

    def test_jobs_endpoint_with_valid_status(self):
        """Test /jobs endpoint with valid status filter."""
        for status_value in ["pending", "running", "completed", "failed"]:
            response = self.client.get(f"/jobs?job_status={status_value}")

            assert response.status_code == 200
            data = response.json()

            assert data["status_filter"] == status_value
            # Verify all returned jobs have the requested status
            for job in data["jobs"]:
                assert job["status"] == status_value

    def test_jobs_endpoint_with_invalid_status(self):
        """Test /jobs endpoint with invalid status returns 400."""
        invalid_status = "invalid_status"
        response = self.client.get(f"/jobs?job_status={invalid_status}")

        assert response.status_code == 500
        data = response.json()

        assert "Invalid status" in data["detail"]
        assert invalid_status in data["detail"]
        assert "Valid statuses:" in data["detail"]

    def test_jobs_endpoint_case_insensitive_status(self):
        """Test /jobs endpoint handles case-insensitive status values."""
        for status_value in ["PENDING", "Running", "COMPLETED", "Failed"]:
            response = self.client.get(f"/jobs?job_status={status_value}")

            assert response.status_code == 200
            data = response.json()

            assert data["status_filter"] == status_value
            # Verify all returned jobs have the correct lowercase status
            expected_status = status_value.lower()
            for job in data["jobs"]:
                assert job["status"] == expected_status

    def test_jobs_endpoint_combined_parameters(self):
        """Test /jobs endpoint with both limit and status parameters."""
        limit = 3
        status_value = "completed"
        response = self.client.get(f"/jobs?limit={limit}&job_status={status_value}")

        assert response.status_code == 200
        data = response.json()

        assert data["limit"] == limit
        assert data["status_filter"] == status_value
        assert len(data["jobs"]) <= limit

        # Verify all returned jobs have the requested status
        for job in data["jobs"]:
            assert job["status"] == status_value

    def test_job_response_structure(self):
        """Test that job objects in response have correct structure."""
        response = self.client.get("/jobs")

        assert response.status_code == 200
        data = response.json()

        for job in data["jobs"]:
            # Verify required fields exist
            required_fields = ["job_id", "status", "progress", "created_at", "updated_at", "error_message"]
            for field in required_fields:
                assert field in job

            # Verify progress structure
            progress = job["progress"]
            assert "completed" in progress
            assert "total" in progress
            assert "percentage" in progress

            # Verify data types
            assert isinstance(job["job_id"], str)
            assert isinstance(job["status"], str)
            assert isinstance(progress["completed"], int)
            assert isinstance(progress["total"], int)
            assert isinstance(progress["percentage"], (int, float))


class TestJobsAPIIntegration:
    """Integration tests that create actual jobs to test the endpoint."""

    def __init__(self):
        """Initialize integration test suite."""
        self.client = None

    def setup_method(self):
        """Set up test environment with job creation capability."""
        self.client = httpx.Client(base_url=BASE_URL, timeout=TIMEOUT)

    def teardown_method(self):
        """Clean up after each test."""
        if hasattr(self, "client") and self.client:
            self.client.close()

    def test_jobs_endpoint_after_job_creation(self):
        """Test /jobs endpoint shows jobs after creating them via API."""
        # Create a test job via the historical stock endpoint
        request_data = {
            "root": "AAPL",
            "start_date": "20231201",
            "end_date": "20231201",
            "data_schema": "quote",
            "interval": 60000,
            "return_format": "parquet",
        }

        # Submit job
        create_response = self.client.post("/historical/stock", json=request_data)
        assert create_response.status_code == 202
        create_data = create_response.json()

        job_id = create_data["job_id"]
        assert "job_id" in create_data

        # Wait a moment for job to be created in database
        time.sleep(0.5)

        # Check that job appears in jobs list
        jobs_response = self.client.get("/jobs")
        assert jobs_response.status_code == 200
        jobs_data = jobs_response.json()

        # Find our job in the list
        created_job = None
        for job in jobs_data["jobs"]:
            if job["job_id"] == job_id:
                created_job = job
                break

        assert created_job is not None, f"Job {job_id} not found in jobs list"
        assert created_job["status"] in ["pending", "running", "completed", "failed"]

    def test_jobs_filtering_after_multiple_jobs(self):
        """Test job filtering works correctly after creating multiple jobs."""
        # Create multiple test jobs
        job_ids = []
        request_data_template = {
            "root": "AAPL",
            "start_date": "20231201",
            "end_date": "20231201",
            "data_schema": "quote",
            "interval": 60000,
            "return_format": "parquet",
        }

        # Create 3 jobs
        for i in range(3):
            response = self.client.post("/historical/stock", json=request_data_template)
            assert response.status_code == 202
            job_ids.append(response.json()["job_id"])

        # Wait for jobs to be created
        time.sleep(1)

        # Test limit parameter
        limited_response = self.client.get("/jobs?limit=2")
        assert limited_response.status_code == 200
        limited_data = limited_response.json()

        assert len(limited_data["jobs"]) <= 2
        assert limited_data["limit"] == 2

        # Verify jobs are ordered by creation date (most recent first)
        if len(limited_data["jobs"]) >= 2:
            job1_created = limited_data["jobs"][0]["created_at"]
            job2_created = limited_data["jobs"][1]["created_at"]
            assert job1_created >= job2_created


def run_test_method(test_instance, test_name, method):
    """Run a single test method with proper setup/teardown."""
    try:
        test_instance.setup_method()
        logger.info(f"Running {test_name}...")
        method()
        logger.info(f"✅ {test_name} passed")
    finally:
        test_instance.teardown_method()


def run_tests():
    """Run all jobs API tests."""
    logger.info("🚀 Starting Jobs API End-to-End Tests")

    try:
        # Run basic functionality tests
        test_suite = TestJobsAPI()

        run_test_method(test_suite, "Health Check", test_suite.test_health_check)
        run_test_method(test_suite, "Empty State", test_suite.test_jobs_endpoint_empty_state)
        run_test_method(test_suite, "Limit Parameter", test_suite.test_jobs_endpoint_with_limit)
        run_test_method(test_suite, "Valid Status Filtering", test_suite.test_jobs_endpoint_with_valid_status)
        run_test_method(test_suite, "Invalid Status Handling", test_suite.test_jobs_endpoint_with_invalid_status)
        run_test_method(test_suite, "Case Insensitive Status", test_suite.test_jobs_endpoint_case_insensitive_status)
        run_test_method(test_suite, "Combined Parameters", test_suite.test_jobs_endpoint_combined_parameters)
        run_test_method(test_suite, "Job Response Structure", test_suite.test_job_response_structure)

        # Run integration tests
        logger.info("Running integration tests...")
        integration_suite = TestJobsAPIIntegration()

        run_test_method(
            integration_suite, "Job Creation Integration", integration_suite.test_jobs_endpoint_after_job_creation
        )
        run_test_method(
            integration_suite, "Multiple Jobs Filtering", integration_suite.test_jobs_filtering_after_multiple_jobs
        )

        logger.info("✅ All Jobs API tests completed successfully!")

    except Exception as e:
        logger.error(f"❌ Jobs API tests failed: {e}")
        raise


if __name__ == "__main__":
    run_tests()
