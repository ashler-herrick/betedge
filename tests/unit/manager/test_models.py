"""
Unit tests for manager models module.
"""

from uuid import UUID

import pytest
from pydantic import ValidationError

from betedge_data.manager.models import (
    HistoricalOptionRequest,
    DataProcessingResponse,
    ErrorResponse,
    generate_date_list,
)


@pytest.mark.unit
class TestHistoricalOptionRequest:
    """Test HistoricalOptionRequest model."""

    def test_valid_request(self):
        """Test creating a valid historical option request."""
        request = HistoricalOptionRequest(
            root="AAPL", start_date="20231113", end_date="20231117", exp=0, max_dte=30, base_pct=0.1
        )

        assert request.root == "AAPL"
        assert request.start_date == "20231113"
        assert request.end_date == "20231117"
        assert request.exp == 0
        assert request.max_dte == 30
        assert request.base_pct == 0.1
        assert request.interval == 900000  # Default
        assert request.start_time is None
        assert request.end_time is None

    def test_custom_interval(self):
        """Test request with custom interval."""
        request = HistoricalOptionRequest(
            root="MSFT",
            start_date="20231113",
            end_date="20231117",
            interval=60000,  # 1 minute
        )

        assert request.interval == 60000

    def test_date_validation(self):
        """Test date format validation."""
        # Invalid date format
        with pytest.raises(ValidationError) as exc_info:
            HistoricalOptionRequest(
                root="AAPL",
                start_date="2023-11-13",  # Wrong format
                end_date="20231117",
            )

        errors = exc_info.value.errors()
        assert any(e["loc"] == ("start_date",) for e in errors)

    def test_date_order_validation(self):
        """Test that end_date must be >= start_date."""
        # Valid: same date
        request = HistoricalOptionRequest(root="AAPL", start_date="20231117", end_date="20231117")
        assert request.start_date == request.end_date

        # Invalid: end before start
        with pytest.raises(ValidationError) as exc_info:
            HistoricalOptionRequest(root="AAPL", start_date="20231117", end_date="20231113")

        assert "end_date must be >= start_date" in str(exc_info.value)

    def test_exp_validation(self):
        """Test expiration validation."""
        # Valid: 0 (all expirations)
        request = HistoricalOptionRequest(root="AAPL", start_date="20231113", end_date="20231117", exp=0)
        assert request.exp == 0

        # Valid: specific expiration (string format)
        request = HistoricalOptionRequest(root="AAPL", start_date="20231113", end_date="20231117", exp="20231124")
        assert request.exp == "20231124"

        # Invalid: negative
        with pytest.raises(ValidationError):
            HistoricalOptionRequest(root="AAPL", start_date="20231113", end_date="20231117", exp=-1)

    def test_percentage_validation(self):
        """Test base_pct validation."""
        # Valid percentages (must be > 0)
        for pct in [0.1, 0.5, 1.0]:
            request = HistoricalOptionRequest(root="AAPL", start_date="20231113", end_date="20231117", base_pct=pct)
            assert request.base_pct == pct

        # Invalid: negative
        with pytest.raises(ValidationError):
            HistoricalOptionRequest(root="AAPL", start_date="20231113", end_date="20231117", base_pct=-0.1)

        # Invalid: greater than 1
        with pytest.raises(ValidationError):
            HistoricalOptionRequest(root="AAPL", start_date="20231113", end_date="20231117", base_pct=1.1)


# Note: HistoricalStockRequest, LiveStockRequest, LiveOptionRequest don't exist in manager models
# These would be defined elsewhere or are part of different modules


@pytest.mark.unit
class TestResponses:
    """Test response models."""

    def test_data_processing_response(self):
        """Test DataProcessingResponse model."""
        request_id = "550e8400-e29b-41d4-a716-446655440000"

        response = DataProcessingResponse(
            status="success",
            request_id=request_id,
            message="Successfully processed data",
            data_type="parquet",
            storage_location="s3://bucket/path/",
            records_count=1000,
            processing_time_ms=1500,
        )

        assert response.status == "success"
        assert str(response.request_id) == request_id
        assert response.message == "Successfully processed data"
        assert response.data_type == "parquet"
        assert response.storage_location == "s3://bucket/path/"
        assert response.records_count == 1000
        assert response.processing_time_ms == 1500

    def test_error_response(self):
        """Test ErrorResponse model."""
        request_id = "550e8400-e29b-41d4-a716-446655440000"

        response = ErrorResponse(
            request_id=request_id,
            error_code="PROCESSING_ERROR",
            message="Failed to process data",
            details="Connection timeout",
        )

        assert str(response.request_id) == request_id
        assert response.error_code == "PROCESSING_ERROR"
        assert response.message == "Failed to process data"
        assert response.details == "Connection timeout"

    def test_response_with_uuid_object(self):
        """Test response with UUID object instead of string."""
        request_id = UUID("550e8400-e29b-41d4-a716-446655440000")

        response = DataProcessingResponse(status="success", request_id=request_id, message="Test")

        assert response.request_id == request_id
        assert isinstance(response.request_id, UUID)


@pytest.mark.unit
class TestUtilityFunctions:
    """Test utility functions in models module."""

    def test_generate_date_list_single_day(self):
        """Test generating date list for a single day."""
        dates = generate_date_list("20231117", "20231117")
        assert dates == ["20231117"]

    def test_generate_date_list_multiple_days(self):
        """Test generating date list for multiple days."""
        dates = generate_date_list("20231113", "20231117")
        expected = ["20231113", "20231114", "20231115", "20231116", "20231117"]
        assert dates == expected

    def test_generate_date_list_across_months(self):
        """Test generating date list across month boundary."""
        dates = generate_date_list("20231130", "20231202")
        expected = ["20231130", "20231201", "20231202"]
        assert dates == expected

    def test_generate_date_list_across_years(self):
        """Test generating date list across year boundary."""
        dates = generate_date_list("20231230", "20240102")
        expected = ["20231230", "20231231", "20240101", "20240102"]
        assert dates == expected

    def test_generate_date_list_leap_year(self):
        """Test generating date list including leap day."""
        dates = generate_date_list("20240228", "20240301")
        expected = ["20240228", "20240229", "20240301"]  # 2024 is a leap year
        assert dates == expected

    def test_generate_date_list_invalid_dates(self):
        """Test error handling for invalid dates."""
        # Invalid format
        with pytest.raises(ValueError):
            generate_date_list("2023-11-13", "20231117")

        # Invalid date (month 13)
        with pytest.raises(ValueError):
            generate_date_list("20231301", "20231302")

        # End before start (should still work, return empty)
        dates = generate_date_list("20231117", "20231113")
        assert dates == []
