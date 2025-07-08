"""
Integration tests for HistoricalOptionClient.

Tests the complete get_bulk_quote method including real API calls,
Rust filtering, and output format validation with schema inspection.
"""

import io
import pytest
import pyarrow.parquet as pq
import pyarrow.ipc as ipc
import polars as pl

from betedge_data.historical.option.client import HistoricalOptionClient
from betedge_data.historical.option.models import HistOptionBulkRequest


class TestHistoricalOptionClientIntegration:
    """Integration tests for HistoricalOptionClient with real API data."""

    @pytest.fixture
    def option_client(self):
        """Create option client for testing."""
        return HistoricalOptionClient()

    @pytest.fixture
    def option_bulk_request_params(self):
        """Parameters for HistOptionBulkRequest testing."""
        return {
            "root": "AAPL",
            "date": 20250702,  # Known good historical date
            "interval": 900000,  # 15 minute intervals
            "endpoint": "quote",
        }

    def test_get_bulk_quote_parquet(self, option_client, option_bulk_request_params):
        """Test get_bulk_quote with parquet output format."""
        # Create request with parquet format
        request = HistOptionBulkRequest(**option_bulk_request_params, return_format="parquet")

        with option_client:
            # Call the method
            result = option_client.get_bulk_quote(request)

            # Validate return type
            assert isinstance(result, io.BytesIO)

            # Verify we have data
            buffer_size = len(result.getvalue())
            assert buffer_size > 0, f"Expected parquet data, got {buffer_size} bytes"
            print(f"Parquet file size: {buffer_size:,} bytes")

            # Reset buffer position for reading
            result.seek(0)

            # Read and validate parquet data
            table = pl.read_parquet(result)

            # Print schema for debugging
            print(f"Parquet schema: {table.schema}")
            print(f"Parquet row count: {table.height:,}")
            print(f"Parquet column names: {table.columns}")

            # Validate we have rows
            assert table.height > 0, "Expected option data rows"

            # Validate we have both option and stock data
            unique_rights = table.select(pl.col("right")).unique().to_series().to_list()
            assert "C" in unique_rights or "P" in unique_rights, "Expected call or put options"
            assert "U" in unique_rights, "Expected underlying stock data"

            # Validate schema has all expected fields
            expected_columns = [
                "ms_of_day",
                "bid_size",
                "bid_exchange",
                "bid_price",
                "bid_condition",
                "ask_size",
                "ask_exchange",
                "ask_price",
                "ask_condition",
                "date",
                "root",
                "expiration",
                "strike",
                "right",
            ]
            assert table.columns == expected_columns, (
                f"Schema mismatch. Expected: {expected_columns}, Got: {table.columns}"
            )

            # Validate data types match Rust schema
            expected_schema = {
                "ms_of_day": pl.UInt32,
                "bid_size": pl.UInt16,
                "bid_exchange": pl.UInt8,
                "bid_price": pl.Float32,
                "bid_condition": pl.UInt8,
                "ask_size": pl.UInt16,
                "ask_exchange": pl.UInt8,
                "ask_price": pl.Float32,
                "ask_condition": pl.UInt8,
                "date": pl.UInt32,
                "root": pl.String,
                "expiration": pl.UInt32,
                "strike": pl.UInt32,
                "right": pl.String,
            }

            for col, expected_type in expected_schema.items():
                actual_type = table.schema[col]
                assert actual_type == expected_type, (
                    f"Type mismatch for {col}: expected {expected_type}, got {actual_type}"
                )

    def test_get_bulk_quote_ipc(self, option_client, option_bulk_request_params):
        """Test get_bulk_quote with IPC output format."""
        # Create request with IPC format
        request = HistOptionBulkRequest(**option_bulk_request_params, return_format="ipc")

        with option_client:
            # Call the method
            result = option_client.get_bulk_quote(request)

            # Validate return type
            assert isinstance(result, io.BytesIO)

            # Verify we have data
            buffer_size = len(result.getvalue())
            assert buffer_size > 0, f"Expected IPC data, got {buffer_size} bytes"
            print(f"IPC file size: {buffer_size:,} bytes")

            # Reset buffer position for reading
            result.seek(0)

            # Read and validate IPC data
            reader = ipc.RecordBatchFileReader(result)
            table = reader.read_all()

            # Print schema for debugging
            print(f"IPC schema: {table.schema}")
            print(f"IPC row count: {len(table):,}")
            print(f"IPC column names: {table.column_names}")

            # Convert to polars for easier validation
            table_pl = pl.from_arrow(table)

            # Validate we have rows
            assert table_pl.height > 0, "Expected option data rows"

            # Validate we have both option and stock data
            unique_rights = table_pl.select(pl.col("right")).unique().to_series().to_list()
            assert "C" in unique_rights or "P" in unique_rights, "Expected call or put options"
            assert "U" in unique_rights, "Expected underlying stock data"

            # Validate schema has all expected fields
            expected_columns = [
                "ms_of_day",
                "bid_size",
                "bid_exchange",
                "bid_price",
                "bid_condition",
                "ask_size",
                "ask_exchange",
                "ask_price",
                "ask_condition",
                "date",
                "root",
                "expiration",
                "strike",
                "right",
            ]
            assert table_pl.columns == expected_columns, (
                f"Schema mismatch. Expected: {expected_columns}, Got: {table_pl.columns}"
            )

    def test_get_bulk_quote_both_formats(self, option_client, option_bulk_request_params):
        """Test that both parquet and IPC formats produce equivalent results."""
        with option_client:
            # Get parquet result
            parquet_request = HistOptionBulkRequest(**option_bulk_request_params, return_format="parquet")
            parquet_result = option_client.get_bulk_quote(parquet_request)

            # Get IPC result
            ipc_request = HistOptionBulkRequest(**option_bulk_request_params, return_format="ipc")
            ipc_result = option_client.get_bulk_quote(ipc_request)

            # Both should return BytesIO
            assert isinstance(parquet_result, io.BytesIO)
            assert isinstance(ipc_result, io.BytesIO)

            # Both should have data
            parquet_size = len(parquet_result.getvalue())
            ipc_size = len(ipc_result.getvalue())

            assert parquet_size > 0, "Parquet result should have data"
            assert ipc_size > 0, "IPC result should have data"

            print(f"Format comparison - Parquet: {parquet_size:,} bytes, IPC: {ipc_size:,} bytes")

            # Read both formats
            parquet_result.seek(0)
            ipc_result.seek(0)

            parquet_table = pq.read_table(parquet_result)

            reader = ipc.RecordBatchFileReader(ipc_result)
            ipc_table = reader.read_all()

            # Compare schemas
            assert parquet_table.schema.equals(ipc_table.schema), "Schemas should be identical between formats"

            # Compare row counts
            assert len(parquet_table) == len(ipc_table), "Row counts should be identical between formats"

            print(f"Both formats verified - {len(parquet_table):,} rows with identical schemas")

    def test_request_validation(self, option_client):
        """Test that request validation works properly."""
        with option_client:
            # Test invalid return format
            with pytest.raises(ValueError):
                HistOptionBulkRequest(root="AAPL", date=20231110, return_format="invalid")

            # Test invalid date format
            with pytest.raises(ValueError):
                HistOptionBulkRequest(
                    root="AAPL",
                    date=2023111,  # 7 digits
                )
