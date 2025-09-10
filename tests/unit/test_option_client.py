"""
Unit tests for HistoricalOptionClient methods.
"""

import pytest
from decimal import Decimal

from betedge_data.historical.option.option_client import HistoricalOptionClient
from betedge_data.common.models import (
    OptionThetaDataResponse,
    StockThetaDataResponse,
    OptionResponseItem,
    Contract,
    Header,
)


@pytest.mark.unit
class TestHistoricalOptionClient:
    """Unit tests for HistoricalOptionClient."""

    @pytest.fixture
    def option_client(self):
        """Create option client for testing."""
        return HistoricalOptionClient()

    def test_flatten_data_ohlc_endpoint(self, option_client):
        """Test _flatten_option_data with OHLC endpoint data."""
        # Create mock OHLC option data with 1 contract, 2 ticks
        ohlc_option_data = OptionThetaDataResponse(
            header=Header(
                latency_ms=100, format=["ms_of_day", "open", "high", "low", "close", "volume", "count", "date"]
            ),
            response=[
                OptionResponseItem(
                    ticks=[
                        [
                            34200000,
                            Decimal("32.10"),
                            Decimal("32.50"),
                            Decimal("32.00"),
                            Decimal("32.30"),
                            1000,
                            50,
                            20231110,
                        ],
                        [
                            35100000,
                            Decimal("32.30"),
                            Decimal("32.80"),
                            Decimal("32.20"),
                            Decimal("32.60"),
                            1500,
                            75,
                            20231110,
                        ],
                    ],
                    contract=Contract(root="AAPL", expiration=20260116, strike=210000, right="P"),
                )
            ],
        )

        # Create mock OHLC stock data with 2 ticks matching option timestamps
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

        # Call _flatten_option_data with OHLC endpoint parameter (new schema-aware API)
        result = option_client._flatten_option_data(ohlc_option_data, ohlc_stock_data, endpoint="ohlc")

        # Basic assertions for OHLC schema
        assert len(result["ms_of_day"]) == 4  # 2 option + 2 stock

        # OHLC-specific fields (different from quote)
        assert "open" in result
        assert "high" in result
        assert "low" in result
        assert "close" in result
        assert "volume" in result
        assert "count" in result

        # Contract info should still be present
        assert "root" in result
        assert "expiration" in result
        assert "strike" in result
        assert "right" in result

        # Verify OHLC data values
        assert result["open"][0] == 32.10  # First option tick
        assert result["close"][1] == 32.60  # Second option tick
        assert result["volume"][2] == 5000  # First stock tick
        assert result["count"][3] == 250  # Second stock tick

    def test_create_arrow_table_ohlc_schema(self, option_client):
        """Test _create_arrow_table with OHLC schema data."""
        # Create mock OHLC flattened data
        ohlc_flattened_data = {
            "ms_of_day": [34200000, 35100000, 34200000, 35100000],
            "open": [32.10, 32.30, 150.20, 150.40],
            "high": [32.50, 32.80, 150.60, 150.80],
            "low": [32.00, 32.20, 150.10, 150.30],
            "close": [32.30, 32.60, 150.40, 150.70],
            "volume": [1000, 1500, 5000, 6000],
            "count": [50, 75, 200, 250],
            "date": [20231110, 20231110, 20231110, 20231110],
            "root": ["AAPL", "AAPL", "AAPL", "AAPL"],
            "expiration": [20260116, 20260116, 0, 0],
            "strike": [210000, 210000, 0, 0],
            "right": ["P", "P", "U", "U"],
        }

        # Call _create_arrow_table with OHLC schema parameter (new schema-aware API)
        table = option_client._create_arrow_table(ohlc_flattened_data, schema_type="ohlc")

        # Verify table structure for OHLC
        expected_ohlc_columns = [
            "ms_of_day",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "count",
            "date",
            "root",
            "expiration",
            "strike",
            "right",
        ]
        assert table.column_names == expected_ohlc_columns
        assert table.num_rows == 4

    def test_flatten_data_quote_endpoint_backwards_compatibility(self, option_client):
        """Test that quote endpoint still works with new schema-aware API."""
        # Use existing quote mock data from basic test
        option_data = OptionThetaDataResponse(
            header=Header(
                latency_ms=100,
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
                OptionResponseItem(
                    ticks=[
                        [34200000, 0, 0, Decimal("0.0000"), 0, 0, 0, Decimal("0.0000"), 0, 20231110],
                        [35100000, 34, 31, Decimal("32.1500"), 50, 85, 65, Decimal("33.2000"), 50, 20231110],
                    ],
                    contract=Contract(root="AAPL", expiration=20260116, strike=210000, right="P"),
                )
            ],
        )

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
        result = option_client._flatten_option_data(option_data, stock_data, endpoint="quote")

        # Should have quote fields (using actual field names from QuoteTick model)
        assert "bid" in result
        assert "ask" in result
        assert "bid_size" in result
        assert "ask_size" in result

        # Test table creation with quote schema
        table = option_client._create_arrow_table(result, schema_type="quote")
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
            "root",
            "expiration",
            "strike",
            "right",
        ]
        assert table.column_names == expected_quote_columns
