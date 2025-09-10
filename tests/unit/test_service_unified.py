"""
Unit tests for DataProcessingService async background job processing.
"""

import asyncio
import pytest
from unittest.mock import Mock, AsyncMock, patch
from uuid import uuid4
import io

from betedge_data.manager.service import DataProcessingService
from betedge_data.manager.external_models import (
    ExternalHistoricalOptionRequest,
    ExternalHistoricalStockRequest,
    ExternalEarningsRequest,
)
from betedge_data.manager.utils import generate_month_list
from betedge_data.manager.job_tracker import JobStatus


# Test utilities for async job lifecycle testing
async def wait_for_job_completion(service, job_id, timeout=5.0):
    """Wait for a job to complete or fail within timeout."""
    start_time = asyncio.get_event_loop().time()
    while True:
        job_info = service.get_job_status(job_id)
        if job_info and job_info.is_finished:
            return job_info

        if asyncio.get_event_loop().time() - start_time > timeout:
            raise TimeoutError(f"Job {job_id} did not complete within {timeout} seconds")

        await asyncio.sleep(0.1)


async def wait_for_background_tasks(service, timeout=5.0):
    """Wait for all background tasks to complete."""
    start_time = asyncio.get_event_loop().time()
    while service._background_tasks:
        if asyncio.get_event_loop().time() - start_time > timeout:
            raise TimeoutError(f"Background tasks did not complete within {timeout} seconds")
        await asyncio.sleep(0.1)


@pytest.mark.unit
class TestDataProcessingServiceUnified:
    """Test unified processing pattern for DataProcessingService."""

    @pytest.fixture
    def mock_publisher(self):
        """Create a mock MinIOPublisher."""
        publisher = Mock()
        publisher.file_exists.return_value = False  # Default: file doesn't exist
        publisher.publish = AsyncMock()  # Async method
        publisher.close = AsyncMock()
        return publisher

    @pytest.fixture
    def service(self, mock_publisher):
        """Create a DataProcessingService for testing."""
        with patch("betedge_data.manager.service.MinIOPublisher", return_value=mock_publisher):
            service = DataProcessingService(force_refresh=False)
            return service

    @pytest.fixture
    def mock_option_request(self):
        """Create a mock ExternalHistoricalOptionRequest."""
        request = Mock()

        # Create mock subrequests with generate_object_key method
        mock_subrequest1 = Mock()
        mock_subrequest1.generate_object_key.return_value = (
            "historical-options/quote/AAPL/2024/01/02/15m/all/data.parquet"
        )
        mock_subrequest1.root = "AAPL"
        mock_subrequest1.date = 20240102

        mock_subrequest2 = Mock()
        mock_subrequest2.generate_object_key.return_value = (
            "historical-options/quote/AAPL/2024/01/03/15m/all/data.parquet"
        )
        mock_subrequest2.root = "AAPL"
        mock_subrequest2.date = 20240103

        request.get_subrequests.return_value = [mock_subrequest1, mock_subrequest2]

        # Mock the client
        mock_client = Mock()
        mock_client.get_data.return_value = io.BytesIO(b"fake parquet data")

        request.get_client.return_value = mock_client
        request.root = "AAPL"
        return request

    @pytest.fixture
    def mock_stock_request(self):
        """Create a mock ExternalHistoricalStockRequest."""
        request = Mock()
        request.generate_requests.return_value = [Mock()]  # One request

        # Mock the client context manager
        mock_client = Mock()
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client.get_data = Mock(return_value=b"mock_data")

        request.get_client.return_value = mock_client
        request.root = "TSLA"
        return request

    @pytest.fixture
    def mock_earnings_request(self):
        """Create a mock ExternalEarningsRequest."""
        request = Mock()
        request.generate_requests.return_value = [Mock(), Mock(), Mock()]  # Three requests

        # Mock the client context manager
        mock_client = Mock()
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client.get_data = Mock(return_value=b"mock_data")

        request.get_client.return_value = mock_client
        return request

    async def test_process_request_unified_pattern(self, service, mock_option_request):
        """Test unified process_request method creates background job and processes asynchronously."""
        request_id = uuid4()

        # Call process_request - this should create job and return immediately
        await service.process_request(mock_option_request, request_id)

        # Verify job was created
        job_info = service.get_job_status(request_id)
        assert job_info is not None
        assert job_info.job_id == request_id
        assert job_info.total_items == 2  # Two subrequests
        assert job_info.status in [JobStatus.PENDING, JobStatus.RUNNING]

        # Wait for background processing to complete
        final_job_info = await wait_for_job_completion(service, request_id)

        # Verify job completed successfully
        assert final_job_info.status == JobStatus.COMPLETED
        assert final_job_info.completed_items == 2
        assert final_job_info.progress_percentage == 100.0

        # Verify the request's methods were called during background processing
        mock_option_request.get_subrequests.assert_called()
        mock_option_request.get_client.assert_called()

    async def test_client_get_data_called_correctly(self, service, mock_option_request):
        """Test that client.get_data() is called for each generated request in background processing."""
        request_id = uuid4()

        # Start background processing
        await service.process_request(mock_option_request, request_id)

        # Wait for background processing to complete
        final_job_info = await wait_for_job_completion(service, request_id)
        assert final_job_info.status == JobStatus.COMPLETED

        # Verify that get_data was called for each individual request
        mock_client = mock_option_request.get_client.return_value
        subrequests = mock_option_request.get_subrequests.return_value
        mock_client.get_data.assert_any_call(subrequests[0])
        mock_client.get_data.assert_any_call(subrequests[1])
        assert mock_client.get_data.call_count == 2

    def test_external_requests_have_required_methods(self):
        """Test that all External*Request classes have required methods."""
        option_req = ExternalHistoricalOptionRequest(root="AAPL", start_date="20231101", end_date="20231102")
        stock_req = ExternalHistoricalStockRequest(root="TSLA", start_date="20231101", end_date="20231102")
        earnings_req = ExternalEarningsRequest(start_date="202311", end_date="202312")

        for request in [option_req, stock_req, earnings_req]:
            assert hasattr(request, "get_subrequests") and callable(request.get_subrequests)
            assert hasattr(request, "get_client") and callable(request.get_client)

    def test_generate_requests_returns_list(self):
        """Test that get_subrequests returns a list for all request types."""
        option_req = ExternalHistoricalOptionRequest(root="AAPL", start_date="20231101", end_date="20231102")
        stock_req = ExternalHistoricalStockRequest(root="TSLA", start_date="20231101", end_date="20231102")
        earnings_req = ExternalEarningsRequest(start_date="202311", end_date="202312")

        assert isinstance(option_req.get_subrequests(), list)
        assert isinstance(stock_req.get_subrequests(), list)
        assert isinstance(earnings_req.get_subrequests(), list)


@pytest.mark.unit
class TestGenerateMonthList:
    """Test generate_month_list utility function."""

    def test_generate_month_list_single_month(self):
        """Test generate_month_list with single month."""
        result = generate_month_list("202311", "202311")
        assert result == [(2023, 11)]

    def test_generate_month_list_multiple_months(self):
        """Test generate_month_list with multiple months across year boundary."""
        result = generate_month_list("202311", "202402")
        expected = [(2023, 11), (2023, 12), (2024, 1), (2024, 2)]
        assert result == expected


@pytest.mark.unit
class TestDataProcessingServicePublishing:
    """Test MinIOPublisher integration in DataProcessingService."""

    @pytest.fixture
    def mock_publisher(self):
        """Create a mock MinIOPublisher."""
        publisher = Mock()
        publisher.file_exists.return_value = False
        publisher.publish = AsyncMock()
        publisher.close = AsyncMock()
        return publisher

    @pytest.fixture
    def service_with_publisher(self, mock_publisher):
        """Create a DataProcessingService with mocked publisher."""
        with patch("betedge_data.manager.service.MinIOPublisher", return_value=mock_publisher):
            service = DataProcessingService(force_refresh=False)
            return service

    @pytest.fixture
    def mock_request_with_object_key(self):
        """Create a mock request with object key generation."""
        request = Mock()

        # Mock individual requests with object key generation
        mock_req1 = Mock()
        mock_req1.generate_object_key.return_value = "test/path/req1.parquet"
        mock_req1.root = "TEST"
        mock_req1.date = 20240101

        mock_req2 = Mock()
        mock_req2.generate_object_key.return_value = "test/path/req2.parquet"
        mock_req2.root = "TEST"
        mock_req2.date = 20240102

        request.get_subrequests.return_value = [mock_req1, mock_req2]

        # Mock client with data
        mock_client = Mock()
        mock_client.get_data = Mock(return_value=io.BytesIO(b"mock_data"))

        request.get_client.return_value = mock_client
        return request

    async def test_process_request_publishes_data(self, service_with_publisher, mock_request_with_object_key):
        """Test that background processing publishes data for each individual request."""
        request_id = uuid4()

        # Start background processing
        await service_with_publisher.process_request(mock_request_with_object_key, request_id)

        # Wait for background job to complete
        final_job_info = await wait_for_job_completion(service_with_publisher, request_id)
        assert final_job_info.status == JobStatus.COMPLETED

        # Verify publisher.publish was called for each individual request
        assert service_with_publisher.publisher.publish.call_count == 2

        # Verify correct object keys were passed
        calls = service_with_publisher.publisher.publish.call_args_list
        assert calls[0][0][1] == "test/path/req1.parquet"  # Second arg is object_key
        assert calls[1][0][1] == "test/path/req2.parquet"  # Second arg is object_key

    async def test_publisher_closed_on_service_close(self, service_with_publisher):
        """Test that publisher is closed when service is closed."""
        await service_with_publisher.close()

        service_with_publisher.publisher.close.assert_called_once()

    async def test_file_exists_skips_processing(self, service_with_publisher, mock_request_with_object_key):
        """Test that existing files are skipped when force_refresh=False."""
        # Set file_exists to return True for the first file
        service_with_publisher.publisher.file_exists.side_effect = [True, False]

        request_id = uuid4()
        await service_with_publisher.process_request(mock_request_with_object_key, request_id)

        # Wait for background job to complete
        final_job_info = await wait_for_job_completion(service_with_publisher, request_id)
        assert final_job_info.status == JobStatus.COMPLETED
        assert final_job_info.completed_items == 2  # Both files counted (1 skipped, 1 processed)

        # Verify file_exists was called for both files
        assert service_with_publisher.publisher.file_exists.call_count == 2

        # Verify only one file was processed (the one that doesn't exist)
        assert service_with_publisher.publisher.publish.call_count == 1

        # Verify get_data was only called once (for the non-existing file)
        mock_client = mock_request_with_object_key.get_client.return_value
        assert mock_client.get_data.call_count == 1

    async def test_force_refresh_ignores_existing_files(self, mock_publisher, mock_request_with_object_key):
        """Test that force_refresh=True processes files even if they exist."""
        # Create service with force_refresh=True
        with patch("betedge_data.manager.service.MinIOPublisher", return_value=mock_publisher):
            service = DataProcessingService(force_refresh=True)

        # Set file_exists to return True for all files
        mock_publisher.file_exists.return_value = True

        request_id = uuid4()
        await service.process_request(mock_request_with_object_key, request_id)

        # Wait for background job to complete
        final_job_info = await wait_for_job_completion(service, request_id)
        assert final_job_info.status == JobStatus.COMPLETED
        assert final_job_info.completed_items == 2

        # Verify file_exists was NOT called when force_refresh=True
        assert mock_publisher.file_exists.call_count == 0

        # Verify all files were processed despite existing
        assert mock_publisher.publish.call_count == 2

        await service.close()
