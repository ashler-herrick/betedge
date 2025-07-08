"""
Unit tests for DataProcessingService unified processing pattern.
"""

import pytest
import sys
from unittest.mock import Mock, MagicMock, AsyncMock, patch
from uuid import uuid4

# Mock storage modules completely before any imports
sys.modules["betedge_data.storage"] = MagicMock()
sys.modules["betedge_data.storage.publisher"] = MagicMock()
sys.modules["betedge_data.storage.config"] = MagicMock()

# Create mock classes
mock_publisher = MagicMock()
mock_config = MagicMock()
sys.modules["betedge_data.storage"].MinIOPublisher = mock_publisher

# Now import the modules
from betedge_data.manager.service import DataProcessingService
from betedge_data.manager.models import (
    ExternalHistoricalOptionRequest,
    ExternalHistoricalStockRequest,
    ExternalEarningsRequest,
)
from betedge_data.manager.utils import generate_month_list

@pytest.mark.unit
class TestDataProcessingServiceUnified:
    """Test unified processing pattern for DataProcessingService."""

    @pytest.fixture
    def service(self):
        """Create a DataProcessingService for testing."""
        service = DataProcessingService(force_refresh=False)
        return service

    @pytest.fixture
    def mock_option_request(self):
        """Create a mock ExternalHistoricalOptionRequest."""
        request = Mock()
        request.generate_requests.return_value = [Mock(), Mock()]  # Two requests

        # Mock the client context manager
        mock_client = Mock()
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client.get_data = Mock(return_value=b"mock_data")

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
        """Test unified process_request method works for all request types."""
        request_id = uuid4()

        response = await service.process_request(mock_option_request, request_id)

        # Verify the request's methods were called
        mock_option_request.generate_requests.assert_called_once()
        mock_option_request.get_client.assert_called_once()
        assert response is None

    async def test_client_get_data_called_correctly(self, service, mock_option_request):
        """Test that client.get_data() is called for each generated request."""
        request_id = uuid4()

        # Mock the individual requests returned by generate_requests()
        mock_req1 = Mock()
        mock_req2 = Mock()
        mock_option_request.generate_requests.return_value = [mock_req1, mock_req2]

        await service.process_request(mock_option_request, request_id)

        # Verify that get_data was called for each individual request
        mock_client = mock_option_request.get_client.return_value
        mock_client.get_data.assert_any_call(mock_req1)
        mock_client.get_data.assert_any_call(mock_req2)
        assert mock_client.get_data.call_count == 2

    def test_external_requests_have_required_methods(self):
        """Test that all External*Request classes have required methods."""
        option_req = ExternalHistoricalOptionRequest(root="AAPL", start_date="20231101", end_date="20231102")
        stock_req = ExternalHistoricalStockRequest(root="TSLA", start_date="20231101", end_date="20231102")
        earnings_req = ExternalEarningsRequest(start_date="202311", end_date="202312")

        for request in [option_req, stock_req, earnings_req]:
            assert hasattr(request, "generate_requests") and callable(request.generate_requests)
            assert hasattr(request, "get_client") and callable(request.get_client)

    def test_generate_requests_returns_list(self):
        """Test that generate_requests returns a list for all request types."""
        option_req = ExternalHistoricalOptionRequest(root="AAPL", start_date="20231101", end_date="20231102")
        stock_req = ExternalHistoricalStockRequest(root="TSLA", start_date="20231101", end_date="20231102")
        earnings_req = ExternalEarningsRequest(start_date="202311", end_date="202312")

        assert isinstance(option_req.generate_requests(), list)
        assert isinstance(stock_req.generate_requests(), list)
        assert isinstance(earnings_req.generate_requests(), list)

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
        publisher = AsyncMock()
        publisher.publish = AsyncMock()
        publisher.close = AsyncMock()
        return publisher

    @pytest.fixture
    def service_with_publisher(self, mock_publisher):
        """Create a DataProcessingService with mocked publisher."""
        with patch('betedge_data.manager.service.MinIOPublisher', return_value=mock_publisher):
            service = DataProcessingService(force_refresh=False)
            return service

    @pytest.fixture
    def mock_request_with_object_key(self):
        """Create a mock request with object key generation."""
        request = Mock()
        
        # Mock individual requests with object key generation
        mock_req1 = Mock()
        mock_req1.generate_object_key.return_value = "test/path/req1.parquet"
        mock_req2 = Mock()
        mock_req2.generate_object_key.return_value = "test/path/req2.parquet"
        request.generate_requests.return_value = [mock_req1, mock_req2]

        # Mock client with data
        mock_client = Mock()
        mock_client.__enter__ = Mock(return_value=mock_client)
        mock_client.__exit__ = Mock(return_value=None)
        mock_client.get_data = Mock(return_value=b"mock_data")
        
        request.get_client.return_value = mock_client
        return request

    async def test_process_request_publishes_data(self, service_with_publisher, mock_request_with_object_key):
        """Test that process_request publishes data for each individual request."""
        request_id = uuid4()
        
        await service_with_publisher.process_request(mock_request_with_object_key, request_id)
        
        # Verify publisher.publish was called for each individual request
        assert service_with_publisher.publisher.publish.call_count == 2
        
        # Verify correct data and object keys were passed
        calls = service_with_publisher.publisher.publish.call_args_list
        assert calls[0][0] == (b"mock_data", "test/path/req1.parquet")
        assert calls[1][0] == (b"mock_data", "test/path/req2.parquet")

    async def test_publisher_closed_on_service_close(self, service_with_publisher):
        """Test that publisher is closed when service is closed."""
        await service_with_publisher.close()
        
        service_with_publisher.publisher.close.assert_called_once()
