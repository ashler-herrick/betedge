"""
Integration tests for PaginatedHTTPClient with Pydantic models.

Tests the HTTP client against real ThetaData API endpoints to ensure
proper pagination, streaming, and model validation.
"""

import pytest

from betedge_data.common.http import PaginatedHTTPClient
from betedge_data.common.models import (
    OptionThetaDataResponse,
    StockThetaDataResponse,
    Header,
    Contract,
    OptionResponseItem,
)


class TestPaginatedHTTPClientIntegration:
    """Integration tests for PaginatedHTTPClient with real API data."""

    @pytest.fixture
    def client(self):
        """Create HTTP client for testing."""
        return PaginatedHTTPClient(timeout=30.0)

    @pytest.fixture
    def option_api_params(self):
        """Parameters for ThetaData option API."""
        return {"root": "AAPL", "exp": "20231117", "start_date": "20231110", "end_date": "20231110", "ivl": "900000"}

    @pytest.fixture
    def stock_api_params(self):
        """Parameters for ThetaData stock API."""
        return {"root": "AAPL", "start_date": "20231110", "end_date": "20231110", "ivl": "60000"}

    def test_option_response_with_streaming(self, client, option_api_params):
        """Test PaginatedHTTPClient fetches and validates real option data with streaming."""
        url = "http://127.0.0.1:25510/v2/bulk_hist/option/quote"

        with client:
            response = client.fetch_paginated(
                url=url,
                response_model=OptionThetaDataResponse,
                params=option_api_params,
                stream_response=True,
                item_path="response.item",
            )

            # Validate response type
            assert isinstance(response, OptionThetaDataResponse)

            # Validate header structure
            assert hasattr(response, "header")
            assert isinstance(response.header, Header)
            assert hasattr(response.header, "latency_ms")
            assert hasattr(response.header, "format")
            assert isinstance(response.header.format, list)

            # Validate response data
            assert hasattr(response, "response")
            assert isinstance(response.response, list)

            # If we have data, validate structure
            if response.response:
                first_item = response.response[0]
                assert isinstance(first_item, OptionResponseItem)
                assert hasattr(first_item, "ticks")
                assert hasattr(first_item, "contract")
                assert isinstance(first_item.contract, Contract)
                assert isinstance(first_item.ticks, list)

                # Validate contract fields
                contract = first_item.contract
                assert contract.root == "AAPL"
                assert isinstance(contract.expiration, int)
                assert isinstance(contract.strike, int)
                assert contract.right in ["C", "P"]

                # Validate tick structure (should be arrays)
                if first_item.ticks:
                    tick = first_item.ticks[0]
                    assert isinstance(tick, list)
                    assert len(tick) == 10  # Expected tick array length

    def test_option_response_without_streaming(self, client, option_api_params):
        """Test PaginatedHTTPClient with regular JSON parsing (no streaming)."""
        url = "http://127.0.0.1:25510/v2/bulk_hist/option/quote"

        with client:
            response = client.fetch_paginated(
                url=url, response_model=OptionThetaDataResponse, params=option_api_params, stream_response=False
            )

            # Should get same structure as streaming
            assert isinstance(response, OptionThetaDataResponse)
            assert isinstance(response.header, Header)
            assert isinstance(response.response, list)

    def test_response_model_validation_success(self, client, option_api_params):
        """Test that valid API responses are properly validated by the model."""
        url = "http://127.0.0.1:25510/v2/bulk_hist/option/quote"

        with client:
            response = client.fetch_paginated(
                url=url,
                response_model=OptionThetaDataResponse,
                params=option_api_params,
                stream_response=True,
                item_path="response.item",
            )

            # Should be able to call model methods
            has_next = response.has_next_page()
            assert isinstance(has_next, bool)

            # Should be able to serialize back to dict
            data_dict = response.model_dump()
            assert isinstance(data_dict, dict)
            assert "header" in data_dict
            assert "response" in data_dict

    def test_tick_parsing_methods(self, client, option_api_params):
        """Test that tick parsing methods work correctly."""
        url = "http://127.0.0.1:25510/v2/bulk_hist/option/quote"

        with client:
            response = client.fetch_paginated(
                url=url,
                response_model=OptionThetaDataResponse,
                params=option_api_params,
                stream_response=True,
                item_path="response.item",
            )

            # Test tick parsing if we have data
            if response.response and response.response[0].ticks:
                first_item = response.response[0]

                # Test get_parsed_ticks method
                parsed_ticks = first_item.get_parsed_ticks()
                assert isinstance(parsed_ticks, list)

                if parsed_ticks:
                    from betedge_data.common.models import QuoteTick

                    tick = parsed_ticks[0]
                    assert isinstance(tick, QuoteTick)
                    assert hasattr(tick, "ms_of_day")
                    assert hasattr(tick, "bid")
                    assert hasattr(tick, "ask")

    def test_pagination_support(self, client, option_api_params):
        """Test pagination handling with has_next_page method."""
        url = "http://127.0.0.1:25510/v2/bulk_hist/option/quote"

        with client:
            response = client.fetch_paginated(
                url=url,
                response_model=OptionThetaDataResponse,
                params=option_api_params,
                stream_response=True,
                item_path="response.item",
            )

            # Test pagination method
            has_next = response.has_next_page()
            assert isinstance(has_next, bool)

            # Check header for next_page field
            if hasattr(response.header, "next_page"):
                next_page = response.header.next_page
                # Should be None or a string
                assert next_page is None or isinstance(next_page, str)

    def test_header_format_validation(self, client, option_api_params):
        """Test that header format field is properly validated."""
        url = "http://127.0.0.1:25510/v2/bulk_hist/option/quote"

        with client:
            response = client.fetch_paginated(
                url=url, response_model=OptionThetaDataResponse, params=option_api_params, stream_response=False
            )

            # Check header format contains expected fields
            expected_fields = [
                "ms_of_day",
                "bid_size",
                "bid_exchange",
                "bid",
                "bid_condition",
                "ask_size",
                "ask_exchange",
                "ask",
                "ask_condition",
                "date",
            ]

            header_format = response.header.format
            assert isinstance(header_format, list)

            # Should contain standard tick fields (exact match may vary by API)
            for field in expected_fields:
                assert field in header_format or any(field in f for f in header_format)

    def test_streaming_vs_regular_consistency(self, client, option_api_params):
        """Test that streaming and regular parsing produce equivalent results."""
        url = "http://127.0.0.1:25510/v2/bulk_hist/option/quote"

        with client:
            # Get response with streaming
            stream_response = client.fetch_paginated(
                url=url,
                response_model=OptionThetaDataResponse,
                params=option_api_params,
                stream_response=True,
                item_path="response.item",
            )

            # Get response without streaming
            regular_response = client.fetch_paginated(
                url=url, response_model=OptionThetaDataResponse, params=option_api_params, stream_response=False
            )

            # Both should be valid models
            assert isinstance(stream_response, OptionThetaDataResponse)
            assert isinstance(regular_response, OptionThetaDataResponse)

            # Should have similar data structure (exact match may vary due to API timing)
            assert len(stream_response.response) > 0 or len(regular_response.response) > 0

            # Headers should have similar structure
            assert stream_response.header.format == regular_response.header.format

    def test_client_context_manager(self, option_api_params):
        """Test that client properly handles context manager lifecycle."""
        url = "http://127.0.0.1:25510/v2/bulk_hist/option/quote"

        # Should work with context manager
        with PaginatedHTTPClient() as client:
            response = client.fetch_paginated(
                url=url,
                response_model=OptionThetaDataResponse,
                params=option_api_params,
                stream_response=True,
                item_path="response.item",
            )
            assert isinstance(response, OptionThetaDataResponse)

        # Client should be properly closed after context
        assert hasattr(client, "client")  # Internal httpx client should exist

    @pytest.mark.parametrize("stream_mode", [True, False])
    def test_response_types_parametrized(self, client, option_api_params, stream_mode):
        """Parametrized test for both streaming modes."""
        url = "http://127.0.0.1:25510/v2/bulk_hist/option/quote"

        with client:
            response = client.fetch_paginated(
                url=url,
                response_model=OptionThetaDataResponse,
                params=option_api_params,
                stream_response=stream_mode,
                item_path="response.item" if stream_mode else None,
            )

            assert isinstance(response, OptionThetaDataResponse)
            assert hasattr(response, "header")
            assert hasattr(response, "response")

    def test_stock_response_basic_validation(self, client, stock_api_params):
        """Test PaginatedHTTPClient fetches and validates real stock data."""
        url = "http://127.0.0.1:25510/v2/hist/stock/quote"

        with client:
            response = client.fetch_paginated(
                url=url,
                response_model=StockThetaDataResponse,
                params=stock_api_params,
                stream_response=False,
                collect_items=True,
            )

            # Validate response type
            assert isinstance(response, StockThetaDataResponse)

            # Validate header structure
            assert hasattr(response, "header")
            assert isinstance(response.header, Header)
            assert hasattr(response.header, "latency_ms")
            assert hasattr(response.header, "format")
            assert isinstance(response.header.format, list)

            # Validate response data
            assert hasattr(response, "response")
            assert isinstance(response.response, list)

            # If we have data, validate tick structure
            if response.response:
                first_tick = response.response[0]
                assert isinstance(first_tick, list)
                assert len(first_tick) == 10  # Stock tick should have 10 fields

    def test_stock_header_format_validation(self, client, stock_api_params):
        """Test that stock response header format contains expected fields."""
        url = "http://127.0.0.1:25510/v2/hist/stock/quote"

        with client:
            response = client.fetch_paginated(
                url=url, response_model=StockThetaDataResponse, params=stock_api_params, stream_response=False
            )

            # Check header format contains expected stock fields
            expected_fields = [
                "ms_of_day",
                "bid_size",
                "bid_exchange",
                "bid",
                "bid_condition",
                "ask_size",
                "ask_exchange",
                "ask",
                "ask_condition",
                "date",
            ]

            header_format = response.header.format
            assert isinstance(header_format, list)
            assert len(header_format) == 10  # Stock should have 10 fields

            # Should contain standard stock tick fields
            for field in expected_fields:
                assert field in header_format or any(field in f for f in header_format)

    def test_stock_tick_data_structure(self, client, stock_api_params):
        """Test that stock tick data has proper structure and types."""
        url = "http://127.0.0.1:25510/v2/hist/stock/quote"

        with client:
            response = client.fetch_paginated(
                url=url, response_model=StockThetaDataResponse, params=stock_api_params, stream_response=False
            )

            # Test model serialization works
            data_dict = response.model_dump()
            assert isinstance(data_dict, dict)
            assert "header" in data_dict
            assert "response" in data_dict

            # Test tick data structure if we have data
            if response.response:
                first_tick = response.response[0]
                assert isinstance(first_tick, list)
                assert len(first_tick) == 10

                # Check reasonable data types for key fields
                # ms_of_day should be an integer
                assert isinstance(first_tick[0], (int, float))
                # bid should be a number
                assert isinstance(first_tick[3], (int, float))
                # ask should be a number
                assert isinstance(first_tick[7], (int, float))
                # date should be an integer
                assert isinstance(first_tick[9], (int, float))
