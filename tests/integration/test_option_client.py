"""
Integration tests for HistoricalOptionClient get_data method.

Tests the complete get_data method including real API calls,
data processing, and output format validation with schema inspection.
"""

import io
import pytest
import pyarrow.parquet as pq
import pyarrow.ipc as ipc
import pyarrow as pa

from betedge_data.historical.option.option_client import HistoricalOptionClient
from betedge_data.historical.option.hist_option_bulk_request import HistOptionBulkRequest


@pytest.mark.integration
class TestHistoricalOptionClientIntegration:
    """Integration tests for HistoricalOptionClient with real API data."""

    @pytest.fixture
    def option_client(self):
        """Create option client for testing."""
        return HistoricalOptionClient()

    def test_get_data_quote_parquet(self, option_client):
        """Test get_data with quote endpoint and parquet format."""
        # Test parameters from: http://127.0.0.1:25510/v2/bulk_hist/option/quote?root=AAPL&exp=20231117&start_date=20231110&end_date=20231110&ivl=900000
        request = HistOptionBulkRequest(
            root="AAPL",
            date=20231110,
            exp=20231117,
            interval=900000,  # 15 minutes
            schema="quote",
            return_format="parquet",
        )

        # Call the method
        result = option_client.get_data(request)

        # Validate return type
        assert isinstance(result, io.BytesIO), "Expected BytesIO object"

        # Verify we have data
        buffer_size = len(result.getvalue())
        assert buffer_size > 0, f"Expected parquet data, got {buffer_size} bytes"
        print(f"Quote Parquet file size: {buffer_size:,} bytes")

        # Reset buffer position for reading
        result.seek(0)

        # Read and validate parquet data
        table = pq.read_table(result)

        # Print schema for debugging
        print(f"Quote Parquet schema: {table.schema}")
        print(f"Quote Parquet row count: {len(table):,}")
        print(f"Quote Parquet column names: {table.column_names}")

        # Validate we have rows
        assert len(table) > 0, "Expected option data rows"

        # Validate schema has all expected fields for quote endpoint
        expected_columns = [
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
        assert table.column_names == expected_columns, (
            f"Schema mismatch. Expected: {expected_columns}, Got: {table.column_names}"
        )

        # Validate we have both option and stock data
        right_column = table.column("right").to_pylist()
        unique_rights = set(right_column)
        assert "C" in unique_rights or "P" in unique_rights, "Expected call or put options"
        assert "U" in unique_rights, "Expected underlying stock data"

        # Validate data types match expected Arrow schema
        schema = table.schema
        assert schema.field("ms_of_day").type == pa.uint32(), "ms_of_day should be uint32"
        assert schema.field("bid_size").type == pa.uint32(), "bid_size should be uint32"
        assert schema.field("bid").type == pa.float32(), "bid should be float32"
        assert schema.field("root").type == pa.string(), "root should be string"
        assert schema.field("right").type == pa.string(), "right should be string"

        # Validate data content
        root_values = set(table.column("root").to_pylist())
        assert root_values == {"AAPL"}, f"Expected only AAPL, got {root_values}"

        date_values = set(table.column("date").to_pylist())
        assert date_values == {20231110}, f"Expected date 20231110, got {date_values}"

    def test_get_data_quote_ipc(self, option_client):
        """Test get_data with quote endpoint and IPC format."""
        request = HistOptionBulkRequest(
            root="AAPL", date=20231110, exp=20231117, interval=900000, schema="quote", return_format="ipc"
        )

        # Call the method
        result = option_client.get_data(request)

        # Validate return type
        assert isinstance(result, io.BytesIO), "Expected BytesIO object"

        # Verify we have data
        buffer_size = len(result.getvalue())
        assert buffer_size > 0, f"Expected IPC data, got {buffer_size} bytes"
        print(f"Quote IPC file size: {buffer_size:,} bytes")

        # Reset buffer position for reading
        result.seek(0)

        # Read and validate IPC data
        reader = ipc.RecordBatchFileReader(result)
        table = reader.read_all()

        # Print schema for debugging
        print(f"Quote IPC schema: {table.schema}")
        print(f"Quote IPC row count: {len(table):,}")
        print(f"Quote IPC column names: {table.column_names}")

        # Validate we have rows
        assert len(table) > 0, "Expected option data rows"

        # Same validations as parquet
        expected_columns = [
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
        assert table.column_names == expected_columns

    def test_format_consistency_quote(self, option_client):
        """Test that parquet and IPC formats produce equivalent results for quote endpoint."""

        # Get parquet result
        parquet_request = HistOptionBulkRequest(
            root="AAPL", date=20231110, exp=20231117, interval=900000, schema="quote", return_format="parquet"
        )
        parquet_result = option_client.get_data(parquet_request)

        # Get IPC result
        ipc_request = HistOptionBulkRequest(
            root="AAPL", date=20231110, exp=20231117, interval=900000, schema="quote", return_format="ipc"
        )
        ipc_result = option_client.get_data(ipc_request)

        # Both should return BytesIO
        assert isinstance(parquet_result, io.BytesIO)
        assert isinstance(ipc_result, io.BytesIO)

        # Both should have data
        parquet_size = len(parquet_result.getvalue())
        ipc_size = len(ipc_result.getvalue())

        assert parquet_size > 0, "Parquet result should have data"
        assert ipc_size > 0, "IPC result should have data"

        print(f"Quote format comparison - Parquet: {parquet_size:,} bytes, IPC: {ipc_size:,} bytes")

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

        print(f"Quote formats verified - {len(parquet_table):,} rows with identical schemas")

    def test_data_content_validation(self, option_client):
        """Test detailed data content validation."""
        request = HistOptionBulkRequest(
            root="AAPL", date=20231110, exp=20231117, interval=900000, schema="quote", return_format="parquet"
        )

        result = option_client.get_data(request)
        result.seek(0)
        table = pq.read_table(result)

        # Validate we have multiple contracts (strikes and rights)
        strikes = set(table.column("strike").to_pylist())
        rights = set(table.column("right").to_pylist())

        print(f"Unique strikes: {sorted(strikes)}")
        print(f"Unique rights: {sorted(rights)}")

        # Should have multiple strike prices
        assert len(strikes) > 1, f"Expected multiple strikes, got {strikes}"

        # Should have calls, puts, and underlying
        assert "C" in rights, "Expected call options"
        assert "P" in rights, "Expected put options"
        assert "U" in rights, "Expected underlying stock data"

        # Validate expirations
        expirations = set(table.column("expiration").to_pylist())
        print(f"Unique expirations: {sorted(expirations)}")

        # Should have expiration date and 0 (for underlying)
        assert 20231117 in expirations, "Expected expiration 20231117"
        assert 0 in expirations, "Expected 0 expiration for underlying"

        # Validate timestamp ranges (should be trading hours)
        timestamps = table.column("ms_of_day").to_pylist()
        min_time = min(timestamps)
        max_time = max(timestamps)

        print(f"Time range: {min_time} to {max_time}")

        # Should be within reasonable trading hours (9:30 AM to 4:00 PM EST)
        # 9:30 AM = 34200000 ms, 4:00 PM = 57600000 ms
        assert min_time >= 34200000, f"Minimum time {min_time} seems too early"
        assert max_time <= 57600000, f"Maximum time {max_time} seems too late"

    def test_request_validation(self, option_client):
        """Test that request validation works properly."""

        # Test invalid return format
        with pytest.raises(ValueError, match="return_format must be"):
            HistOptionBulkRequest(root="AAPL", date=20231110, return_format="invalid")

        # Test invalid schema
        with pytest.raises(ValueError, match="schema must be"):
            HistOptionBulkRequest(root="AAPL", date=20231110, schema="invalid")

        # Test invalid date format
        with pytest.raises(ValueError):
            HistOptionBulkRequest(
                root="AAPL",
                date=2023111,  # 7 digits instead of 8
            )
