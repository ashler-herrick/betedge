"""
Integration tests for ThetaTerminal + data processing workflow.

These tests require ThetaTerminal to be running and authenticated.
"""

import io
import logging
from datetime import datetime, timedelta

import pytest
import requests

from betedge_data.historical.config import HistoricalClientConfig
from betedge_data.historical.option.client import HistoricalOptionClient
from betedge_data.manager.service import DataProcessingService
from betedge_data.manager.models import HistoricalOptionRequest

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)


@pytest.fixture
def theta_config():
    """ThetaTerminal configuration for integration tests."""
    return HistoricalClientConfig()


@pytest.fixture
def data_service(theta_config):
    """Data processing service for integration tests."""
    try:
        return DataProcessingService()
    except Exception as e:
        if "MinIO" in str(e) or "AccessDenied" in str(e):
            pytest.skip(f"DataProcessingService requires MinIO: {e}")
        else:
            raise


def test_theta_terminal_connectivity():
    """Test basic connectivity to ThetaTerminal."""
    config = HistoricalClientConfig()
    
    # Try to make a basic request to verify connectivity
    try:
        response = requests.get(f"{config.base_url}/bulk_hist/option/quote", timeout=10)
        # We expect this to fail with 400 (missing params) rather than connection error
        assert response.status_code in [400, 422], f"Unexpected status: {response.status_code}"
        logger.info("✓ ThetaTerminal connectivity verified")
    except requests.exceptions.ConnectionError:
        pytest.skip("ThetaTerminal not available - skipping integration test")
    except requests.exceptions.Timeout:
        pytest.skip("ThetaTerminal timeout - skipping integration test")


# Historical stock client functionality removed - no longer available


def test_historical_option_client_basic(theta_config):
    """Test basic historical option client functionality."""
    try:
        with HistoricalOptionClient() as client:
            # Test with a recent date that should have options data
            recent_date = int((datetime.now() - timedelta(days=30)).strftime("%Y%m%d"))
            
            # Create a basic option request
            from betedge_data.historical.option.models import HistoricalOptionRequest
            request = HistoricalOptionRequest(
                root="AAPL",
                start_date=recent_date,
                end_date=recent_date,
                exp=0,  # All expirations
                max_dte=30,
                base_pct=0.1,
                interval=900000,  # 15 minutes (minimum for exp=0)
            )
            
            # Test getting filtered parquet data
            parquet_data = client.get_filtered_bulk_quote_as_parquet(request)
            assert isinstance(parquet_data, io.BytesIO)
            assert parquet_data.getvalue()  # Should have some data
            
            logger.info(f"✓ Retrieved {len(parquet_data.getvalue())} bytes of parquet data for AAPL")
                
    except Exception as e:
        if "ThetaTerminal" in str(e) or "connection" in str(e).lower() or "HTTP" in str(e):
            pytest.skip(f"ThetaTerminal not available: {e}")
        else:
            raise


def test_data_processing_service_option_request(data_service):
    """Test the data processing service with a small option request."""
    # Use a recent date with known options activity
    recent_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
    
    request = HistoricalOptionRequest(
        root="AAPL",
        start_date=recent_date,
        end_date=recent_date,  # Single day to keep test fast
        exp=0,  # All expirations
        max_dte=30,
        base_pct=0.2,  # Wider spread to get fewer contracts
        interval=900000,  # 15 minutes (minimum for exp=0)
    )
    
    try:
        response = data_service.process_historical_option_request(request)
        
        # Verify response structure
        assert response.status == "success"
        assert response.request_id is not None
        assert response.message is not None
        assert response.processing_time_ms is not None
        assert response.processing_time_ms > 0
        
        # Should have some storage location
        if response.storage_location:
            assert "historical-options" in response.storage_location
            
        logger.info(f"✓ Processed option request in {response.processing_time_ms}ms")
        logger.info(f"✓ Storage location: {response.storage_location}")
        
    except Exception as e:
        if "ThetaTerminal" in str(e) or "connection" in str(e).lower():
            pytest.skip(f"ThetaTerminal not available: {e}")
        else:
            raise


def test_data_processing_error_handling(data_service):
    """Test data processing service error handling."""
    # Test with invalid date range
    request = HistoricalOptionRequest(
        root="AAPL",
        start_date=20240102, 
        end_date=20240101,  # Invalid: start > end
        exp=0,
        max_dte=30,
        base_pct=0.1,
    )
    
    with pytest.raises(ValueError, match=".*date.*"):
        data_service.process_historical_option_request(request)
    
    logger.info("✓ Correctly handled invalid date range")


@pytest.mark.slow
def test_bulk_option_processing(data_service):
    """Test processing single day of option data (bulk processing removed)."""
    # Use a single recent date for testing
    recent_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d")
    
    request = HistoricalOptionRequest(
        root="SPY",  # Use SPY for high volume
        start_date=recent_date,
        end_date=recent_date,  # Single day due to exp=0 constraint
        exp=0,
        max_dte=14,  # Shorter DTE for fewer contracts
        base_pct=0.3,  # Wider spread
        interval=900000,  # 15 minutes (minimum for exp=0)
    )
    
    try:
        response = data_service.process_historical_option_request(request)
        
        assert response.status == "success"
        assert response.processing_time_ms > 0
        
        # Should be processing single day
        assert "1/1 days" in response.message or "day processed" in response.message or "Successfully" in response.message
        
        logger.info(f"✓ Bulk processing completed in {response.processing_time_ms}ms")
        logger.info(f"✓ Message: {response.message}")
        
    except Exception as e:
        if "ThetaTerminal" in str(e) or "connection" in str(e).lower():
            pytest.skip(f"ThetaTerminal not available: {e}")
        else:
            raise


# Memory usage monitoring test removed - requires psutil dependency