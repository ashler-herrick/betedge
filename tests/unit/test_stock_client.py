"""
Unit tests for HistoricalStockClient methods.
"""

import pytest
from decimal import Decimal

from betedge_data.historical.stock.client import HistoricalStockClient
from betedge_data.common.models import (
    StockThetaDataResponse,
    Header,
)

@pytest.mark.unit
class TestHistoricalStockClient:
    """Unit tests for HistoricalStockClient."""

    @pytest.fixture
    def stock_client(self):
        """Create stock client for testing."""
        return HistoricalStockClient()

    def test_flatten_stock_data_ohlc_endpoint(self, stock_client):
        """Test _flatten_stock_data with OHLC endpoint data."""
        # Create mock OHLC stock data with 2 ticks
        ohlc_stock_data = StockThetaDataResponse(
            header=Header(
                latency_ms=50, format=["ms_of_day", "open", "high", "low", "close", "volume", "count", "date"]
            ),
            response=[
                [
                    34200000,
                    Decimal("150.20"),
                    Decimal("150.60"),
                    Decimal("150.10"),
                    Decimal("150.40"),
                    5000,
                    200,
                    20231110,
                ],
                [
                    35100000,
                    Decimal("150.40"),
                    Decimal("150.80"),
                    Decimal("150.30"),
                    Decimal("150.70"),
                    6000,
                    250,
                    20231110,
                ],
            ],
        )

        # Call _flatten_stock_data with OHLC endpoint parameter (new schema-aware API)
        result = stock_client._flatten_stock_data(ohlc_stock_data, endpoint="ohlc")

        # Basic assertions for OHLC schema
        assert len(result["ms_of_day"]) == 2  # 2 stock ticks

        # OHLC-specific fields (different from quote)
        assert "open" in result
        assert "high" in result
        assert "low" in result
        assert "close" in result
        assert "volume" in result
        assert "count" in result

        # Verify OHLC data values
        assert result["open"][0] == 150.20  # First stock tick
        assert result["close"][1] == 150.70  # Second stock tick
        assert result["volume"][0] == 5000  # First stock tick
        assert result["count"][1] == 250  # Second stock tick

    def test_create_arrow_table_ohlc_schema(self, stock_client):
        """Test _create_arrow_table with OHLC schema data."""
        # Create mock OHLC flattened data
        ohlc_flattened_data = {
            "ms_of_day": [34200000, 35100000],
            "open": [150.20, 150.40],
            "high": [150.60, 150.80],
            "low": [150.10, 150.30],
            "close": [150.40, 150.70],
            "volume": [5000, 6000],
            "count": [200, 250],
            "date": [20231110, 20231110],
        }

        # Call _create_arrow_table with OHLC schema parameter (new schema-aware API)
        table = stock_client._create_arrow_table(ohlc_flattened_data, schema_type="ohlc")

        # Verify table structure for OHLC
        expected_ohlc_columns = ["ms_of_day", "open", "high", "low", "close", "volume", "count", "date"]
        assert table.column_names == expected_ohlc_columns
        assert table.num_rows == 2

    def test_flatten_stock_data_quote_endpoint_backwards_compatibility(self, stock_client):
        """Test that quote endpoint still works with new schema-aware API."""
        # Use existing quote mock data
        stock_data = StockThetaDataResponse(
            header=Header(
                latency_ms=50,
                format=[
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
                ],
            ),
            response=[
                [34200000, 100, 69, Decimal("150.25"), 50, 200, 60, Decimal("150.50"), 50, 20231110],
                [35100000, 150, 65, Decimal("150.30"), 50, 180, 65, Decimal("150.55"), 50, 20231110],
            ],
        )

        # Test new API with explicit quote endpoint
        result = stock_client._flatten_stock_data(stock_data, endpoint="quote")

        # Should have quote fields (using actual field names from QuoteTick model)
        assert "bid" in result
        assert "ask" in result
        assert "bid_size" in result
        assert "ask_size" in result

        # Should NOT have OHLC fields
        assert "open" not in result
        assert "close" not in result
        assert "volume" not in result

        # Test table creation with quote schema
        table = stock_client._create_arrow_table(result, schema_type="quote")
        expected_quote_columns = [
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
        assert table.column_names == expected_quote_columns
