"""
Integration tests for HistoricalStockClient get_data method.

Tests the complete get_data method including real API calls,
data processing, and output format validation with schema inspection.
"""

import io
import pytest
import pyarrow.parquet as pq
import pyarrow.ipc as ipc
import pyarrow as pa
import polars as pl

from betedge_data.historical.stock.stock_client import HistoricalStockClient
from betedge_data.historical.stock.hist_stock_request import HistStockRequest


@pytest.mark.integration
class TestHistoricalStockClientIntegration:
    """Integration tests for HistoricalStockClient with real API data."""

    @pytest.fixture
    def stock_client(self):
        """Create stock client for testing."""
        return HistoricalStockClient()

    def test_get_data_quote_parquet(self, stock_client):
        """Test get_data with quote endpoint and parquet format."""
        # Test parameters - using recent trading date with known data
        request = HistStockRequest(
            root="AAPL",
            date=20231110,
            interval=60000,  # 1 minute
            schema="quote",
            return_format="parquet",
        )

        # Call the method
        result = stock_client.get_data(request)

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
        assert len(table) > 0, "Expected stock data rows"

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
        ]
        assert table.column_names == expected_columns, (
            f"Schema mismatch. Expected: {expected_columns}, Got: {table.column_names}"
        )

        # Validate data types match expected Arrow schema
        schema = table.schema
        assert schema.field("ms_of_day").type == pa.uint32(), "ms_of_day should be uint32"
        assert schema.field("bid_size").type == pa.uint32(), "bid_size should be uint32"
        assert schema.field("bid").type == pa.float32(), "bid should be float32"
        assert schema.field("ask").type == pa.float32(), "ask should be float32"
        assert schema.field("date").type == pa.uint32(), "date should be uint32"

        # Validate data content
        date_values = set(table.column("date").to_pylist())
        assert date_values == {20231110}, f"Expected date 20231110, got {date_values}"

        # Validate bid/ask data is reasonable
        bid_values = table.column("bid").to_pylist()
        ask_values = table.column("ask").to_pylist()

        # Filter out zero values (market closed periods)
        valid_bids = [b for b in bid_values if b > 0]
        valid_asks = [a for a in ask_values if a > 0]

        if valid_bids and valid_asks:
            assert min(valid_bids) > 0, "Bid prices should be positive"
            assert min(valid_asks) > 0, "Ask prices should be positive"
            # Ask should generally be >= bid (spread should be non-negative)
            print(f"Bid range: ${min(valid_bids):.2f} - ${max(valid_bids):.2f}")
            print(f"Ask range: ${min(valid_asks):.2f} - ${max(valid_asks):.2f}")

    def test_get_data_quote_ipc(self, stock_client):
        """Test get_data with quote endpoint and IPC format."""
        request = HistStockRequest(root="AAPL", date=20231110, interval=60000, schema="quote", return_format="ipc")

        # Call the method
        result = stock_client.get_data(request)

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
        assert len(table) > 0, "Expected stock data rows"

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
        ]
        assert table.column_names == expected_columns

    def test_get_data_ohlc_parquet(self, stock_client):
        """Test get_data with OHLC endpoint and parquet format."""
        request = HistStockRequest(root="AAPL", date=20231103, interval=60000, schema="ohlc", return_format="parquet")

        # Call the method
        result = stock_client.get_data(request)

        # Validate return type
        assert isinstance(result, io.BytesIO), "Expected BytesIO object"

        # Verify we have data
        buffer_size = len(result.getvalue())
        assert buffer_size > 0, f"Expected parquet data, got {buffer_size} bytes"
        print(f"OHLC Parquet file size: {buffer_size:,} bytes")

        # Reset buffer position for reading
        result.seek(0)

        # Read and validate parquet data
        table = pq.read_table(result)

        # Print schema for debugging
        print(f"OHLC Parquet schema: {table.schema}")
        print(f"OHLC Parquet row count: {len(table):,}")
        print(f"OHLC Parquet column names: {table.column_names}")

        # Validate we have rows
        assert len(table) > 0, "Expected stock data rows"

        # Validate schema has all expected fields for OHLC endpoint
        expected_columns = [
            "ms_of_day",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "count",
            "date",
        ]
        assert table.column_names == expected_columns, (
            f"Schema mismatch. Expected: {expected_columns}, Got: {table.column_names}"
        )

        # Validate data types for OHLC schema
        schema = table.schema
        assert schema.field("ms_of_day").type == pa.uint32(), "ms_of_day should be uint32"
        assert schema.field("open").type == pa.float32(), "open should be float32"
        assert schema.field("high").type == pa.float32(), "high should be float32"
        assert schema.field("low").type == pa.float32(), "low should be float32"
        assert schema.field("close").type == pa.float32(), "close should be float32"
        assert schema.field("volume").type == pa.uint32(), "volume should be uint32"
        assert schema.field("count").type == pa.uint32(), "count should be uint32"
        assert schema.field("date").type == pa.uint32(), "date should be uint32"

        # Validate data content
        date_values = set(table.column("date").to_pylist())
        assert date_values == {20231103}, f"Expected date 20231103, got {date_values}"

        # Validate OHLC relationships (High >= Open,Close,Low; Low <= Open,Close,High)
        ohlc_data = pl.from_arrow(table.select(["open", "high", "low", "close"]))

        # Filter out zero/invalid values
        valid_data = ohlc_data.filter(
            (pl.col("open") > 0) & (pl.col("high") > 0) & (pl.col("low") > 0) & (pl.col("close") > 0)
        )

        if len(valid_data) > 0:
            # High should be >= all other prices
            assert (valid_data.select(pl.col("high") >= pl.col("open"))).to_series().all(), "High should be >= Open"
            assert (valid_data.select(pl.col("high") >= pl.col("close"))).to_series().all(), "High should be >= Close"
            assert (valid_data.select(pl.col("high") >= pl.col("low"))).to_series().all(), "High should be >= Low"

            # Low should be <= all other prices
            assert (valid_data.select(pl.col("low") <= pl.col("open"))).to_series().all(), "Low should be <= Open"
            assert (valid_data.select(pl.col("low") <= pl.col("close"))).to_series().all(), "Low should be <= Close"
            assert (valid_data.select(pl.col("low") <= pl.col("high"))).to_series().all(), "Low should be <= High"

            print(f"OHLC validation passed for {len(valid_data)} valid records")

    def test_get_data_ohlc_ipc(self, stock_client):
        """Test get_data with OHLC endpoint and IPC format."""
        request = HistStockRequest(root="AAPL", date=20231103, interval=60000, schema="ohlc", return_format="ipc")

        # Call the method
        result = stock_client.get_data(request)

        # Validate return type
        assert isinstance(result, io.BytesIO), "Expected BytesIO object"

        # Verify we have data
        buffer_size = len(result.getvalue())
        assert buffer_size > 0, f"Expected IPC data, got {buffer_size} bytes"
        print(f"OHLC IPC file size: {buffer_size:,} bytes")

        # Reset buffer position for reading
        result.seek(0)

        # Read and validate IPC data
        reader = ipc.RecordBatchFileReader(result)
        table = reader.read_all()

        # Print schema for debugging
        print(f"OHLC IPC schema: {table.schema}")
        print(f"OHLC IPC row count: {len(table):,}")

        # Validate we have rows
        assert len(table) > 0, "Expected stock data rows"

        # Same validations as parquet
        expected_columns = [
            "ms_of_day",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "count",
            "date",
        ]
        assert table.column_names == expected_columns

    def test_get_data_eod_parquet(self, stock_client):
        """Test get_data with EOD endpoint and parquet format using year-based request."""
        # Test EOD endpoint with year-based request (unified model)
        request = HistStockRequest(
            root="AAPL",
            year=2024,
            schema="eod",
            return_format="parquet",
        )

        # Call the method
        result = stock_client.get_data(request)

        # Validate return type
        assert isinstance(result, io.BytesIO), "Expected BytesIO object"

        # Verify we have data
        buffer_size = len(result.getvalue())
        assert buffer_size > 0, f"Expected parquet data, got {buffer_size} bytes"
        print(f"EOD Parquet file size: {buffer_size:,} bytes")

        # Reset buffer position for reading
        result.seek(0)

        # Read and validate parquet data
        table = pq.read_table(result)

        # Print schema for debugging
        print(f"EOD Parquet schema: {table.schema}")
        print(f"EOD Parquet row count: {len(table):,}")
        print(f"EOD Parquet column names: {table.column_names}")

        # Validate we have rows
        assert len(table) > 0, "Expected EOD data rows"

        # Validate schema has all expected fields for EOD endpoint (17 fields)
        expected_columns = [
            "ms_of_day",
            "ms_of_day2",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "count",
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
        assert table.column_names == expected_columns, (
            f"Schema mismatch. Expected: {expected_columns}, Got: {table.column_names}"
        )

        # Validate data types for EOD schema (combines OHLC + quote data)
        schema = table.schema
        assert schema.field("ms_of_day").type == pa.uint32(), "ms_of_day should be uint32"
        assert schema.field("ms_of_day2").type == pa.uint32(), "ms_of_day2 should be uint32"
        assert schema.field("open").type == pa.float32(), "open should be float32"
        assert schema.field("high").type == pa.float32(), "high should be float32"
        assert schema.field("low").type == pa.float32(), "low should be float32"
        assert schema.field("close").type == pa.float32(), "close should be float32"
        assert schema.field("volume").type == pa.uint32(), "volume should be uint32"
        assert schema.field("count").type == pa.uint32(), "count should be uint32"
        assert schema.field("bid").type == pa.float32(), "bid should be float32"
        assert schema.field("ask").type == pa.float32(), "ask should be float32"
        assert schema.field("date").type == pa.uint32(), "date should be uint32"

        # Validate EOD data content (should be ~252 trading days for a year)
        expected_min_rows = 200  # Account for holidays/weekends
        expected_max_rows = 300  # Allow some buffer

        row_count = len(table)
        assert expected_min_rows <= row_count <= expected_max_rows, (
            f"Expected {expected_min_rows}-{expected_max_rows} EOD records, got {row_count}"
        )

        print(f"EOD records: {row_count} (within expected range)")

        # Validate date range (should span 2024)
        dates = table.column("date").to_pylist()
        min_date = min(dates)
        max_date = max(dates)

        assert min_date >= 20240101, f"Min date {min_date} should be >= 20240101"
        assert max_date <= 20241231, f"Max date {max_date} should be <= 20241231"

        print(f"EOD date range: {min_date} to {max_date}")

        # Validate OHLC relationships in EOD data
        ohlc_data = pl.from_arrow(table.select(["open", "high", "low", "close"]))
        valid_data = ohlc_data.filter(
            (pl.col("open") > 0) & (pl.col("high") > 0) & (pl.col("low") > 0) & (pl.col("close") > 0)
        )

        if len(valid_data) > 0:
            assert (valid_data.select(pl.col("high") >= pl.col("open"))).to_series().all(), "High should be >= Open"
            assert (valid_data.select(pl.col("high") >= pl.col("close"))).to_series().all(), "High should be >= Close"
            assert (valid_data.select(pl.col("high") >= pl.col("low"))).to_series().all(), "High should be >= Low"
            assert (valid_data.select(pl.col("low") <= pl.col("open"))).to_series().all(), "Low should be <= Open"
            assert (valid_data.select(pl.col("low") <= pl.col("close"))).to_series().all(), "Low should be <= Close"
            print(f"EOD OHLC validation passed for {len(valid_data)} valid records")

    def test_get_data_eod_ipc(self, stock_client):
        """Test get_data with EOD endpoint and IPC format using date-based request."""
        # Test EOD endpoint with date-based request (should extract year)
        request = HistStockRequest(
            root="AAPL",
            date=20240615,  # Should extract year 2024 for EOD processing
            schema="eod",
            return_format="ipc",
        )

        # Call the method
        result = stock_client.get_data(request)

        # Validate return type
        assert isinstance(result, io.BytesIO), "Expected BytesIO object"

        # Verify we have data
        buffer_size = len(result.getvalue())
        assert buffer_size > 0, f"Expected IPC data, got {buffer_size} bytes"
        print(f"EOD IPC file size: {buffer_size:,} bytes")

        # Reset buffer position for reading
        result.seek(0)

        # Read and validate IPC data
        reader = ipc.RecordBatchFileReader(result)
        table = reader.read_all()

        # Print schema for debugging
        print(f"EOD IPC schema: {table.schema}")
        print(f"EOD IPC row count: {len(table):,}")

        # Validate we have rows
        assert len(table) > 0, "Expected EOD data rows"

        # Same validations as parquet (17 field EOD schema)
        expected_columns = [
            "ms_of_day",
            "ms_of_day2",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "count",
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
        assert table.column_names == expected_columns

        # Verify the date extraction worked correctly (should be full year of 2024 data)
        dates = table.column("date").to_pylist()
        min_date = min(dates)
        max_date = max(dates)

        # Should get full year data despite providing single date
        assert min_date >= 20240101, "Should get full year data starting from Jan 1"
        assert max_date <= 20241231, "Should get full year data ending by Dec 31"

        print(f"EOD date extraction test: {min_date} to {max_date} (from input date 20240615)")

    def test_format_consistency_quote(self, stock_client):
        """Test that parquet and IPC formats produce equivalent results for quote endpoint."""

        # Get parquet result
        parquet_request = HistStockRequest(
            root="AAPL", date=20231110, interval=60000, schema="quote", return_format="parquet"
        )
        parquet_result = stock_client.get_data(parquet_request)

        # Get IPC result
        ipc_request = HistStockRequest(root="AAPL", date=20231110, interval=60000, schema="quote", return_format="ipc")
        ipc_result = stock_client.get_data(ipc_request)

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

    def test_format_consistency_ohlc(self, stock_client):
        """Test that parquet and IPC formats produce equivalent results for OHLC endpoint."""

        # Get parquet result
        parquet_request = HistStockRequest(
            root="AAPL", date=20231103, interval=60000, schema="ohlc", return_format="parquet"
        )
        parquet_result = stock_client.get_data(parquet_request)

        # Get IPC result
        ipc_request = HistStockRequest(root="AAPL", date=20231103, interval=60000, schema="ohlc", return_format="ipc")
        ipc_result = stock_client.get_data(ipc_request)

        # Read both formats
        parquet_result.seek(0)
        ipc_result.seek(0)

        parquet_table = pq.read_table(parquet_result)

        reader = ipc.RecordBatchFileReader(ipc_result)
        ipc_table = reader.read_all()

        # Compare schemas and row counts
        assert parquet_table.schema.equals(ipc_table.schema), "Schemas should be identical between formats"
        assert len(parquet_table) == len(ipc_table), "Row counts should be identical between formats"

        print(f"OHLC formats verified - {len(parquet_table):,} rows with identical schemas")

    def test_format_consistency_eod(self, stock_client):
        """Test that parquet and IPC formats produce equivalent results for EOD endpoint."""

        # Get parquet result (year-based)
        parquet_request = HistStockRequest(root="AAPL", year=2024, schema="eod", return_format="parquet")
        parquet_result = stock_client.get_data(parquet_request)

        # Get IPC result (date-based, should extract same year)
        ipc_request = HistStockRequest(root="AAPL", date=20240615, schema="eod", return_format="ipc")
        ipc_result = stock_client.get_data(ipc_request)

        # Read both formats
        parquet_result.seek(0)
        ipc_result.seek(0)

        parquet_table = pq.read_table(parquet_result)

        reader = ipc.RecordBatchFileReader(ipc_result)
        ipc_table = reader.read_all()

        # Compare schemas and row counts (should be identical for same year)
        assert parquet_table.schema.equals(ipc_table.schema), "Schemas should be identical between formats"
        assert len(parquet_table) == len(ipc_table), "Row counts should be identical between formats"

        print(f"EOD formats verified - {len(parquet_table):,} rows with identical schemas")

    def test_data_content_validation(self, stock_client):
        """Test detailed data content validation."""
        request = HistStockRequest(root="AAPL", date=20231110, interval=60000, schema="quote", return_format="parquet")

        result = stock_client.get_data(request)
        result.seek(0)
        table = pq.read_table(result)

        # Validate timestamp ranges (should be within trading hours)
        timestamps = table.column("ms_of_day").to_pylist()
        min_time = min(timestamps)
        max_time = max(timestamps)

        print(f"Time range: {min_time} to {max_time}")

        # Should be within reasonable trading hours (9:30 AM to 4:00 PM EST)
        # 9:30 AM = 34200000 ms, 4:00 PM = 57600000 ms
        assert min_time >= 34200000, f"Minimum time {min_time} seems too early"
        assert max_time <= 57600000, f"Maximum time {max_time} seems too late"

        # Validate bid/ask spread relationships
        bid_values = table.column("bid").to_pylist()
        ask_values = table.column("ask").to_pylist()

        # Pair up bid/ask for same timestamps
        valid_spreads = []
        for bid, ask in zip(bid_values, ask_values):
            if bid > 0 and ask > 0:  # Only check non-zero values
                spread = ask - bid
                valid_spreads.append(spread)
                assert spread >= 0, f"Ask ({ask}) should be >= Bid ({bid})"

        if valid_spreads:
            avg_spread = sum(valid_spreads) / len(valid_spreads)
            print(f"Average bid-ask spread: ${avg_spread:.4f}")
            print(f"Valid spread count: {len(valid_spreads)}")

        # Validate sizes are reasonable
        bid_sizes = [s for s in table.column("bid_size").to_pylist() if s > 0]
        ask_sizes = [s for s in table.column("ask_size").to_pylist() if s > 0]

        if bid_sizes and ask_sizes:
            print(f"Bid size range: {min(bid_sizes)} - {max(bid_sizes)}")
            print(f"Ask size range: {min(ask_sizes)} - {max(ask_sizes)}")

    def test_request_validation(self):
        """Test that unified model request validation works properly."""

        # Test invalid return format
        with pytest.raises(ValueError, match="return_format must be"):
            HistStockRequest(root="AAPL", date=20231110, schema="quote", return_format="invalid")

        # Test invalid endpoint
        with pytest.raises(ValueError, match="schema must be"):
            HistStockRequest(root="AAPL", date=20231110, schema="invalid")

        # Test invalid date format
        with pytest.raises(ValueError, match="Date must be 8 digits"):
            HistStockRequest(root="AAPL", date=2023111, schema="quote")  # 7 digits instead of 8

        # Test unified model validations
        with pytest.raises(ValueError, match="Either 'date' or 'year' must be provided"):
            HistStockRequest(root="AAPL", schema="quote")  # Neither date nor year

        with pytest.raises(ValueError, match="Provide either 'date' or 'year', not both"):
            HistStockRequest(root="AAPL", date=20231110, year=2023, schema="quote")  # Both provided

        with pytest.raises(ValueError, match="Schema 'quote' requires 'date' field"):
            HistStockRequest(root="AAPL", year=2023, schema="quote")  # Quote needs date

        # Test valid unified model combinations
        # EOD with year (should work)
        eod_year_req = HistStockRequest(root="AAPL", year=2024, schema="eod")
        assert eod_year_req.year == 2024
        assert eod_year_req.schema == "eod"

        # EOD with date (should extract year)
        eod_date_req = HistStockRequest(root="AAPL", date=20240615, schema="eod")
        assert eod_date_req.date == 20240615
        assert eod_date_req.year == 2024  # Should be auto-extracted
        assert eod_date_req.schema == "eod"

        # Quote with date (should work)
        quote_req = HistStockRequest(root="AAPL", date=20231110, schema="quote")
        assert quote_req.date == 20231110
        assert quote_req.schema == "quote"

        print("✅ All unified model validation tests passed")

        # Verify that stock_client fixture isn't needed for validation tests
        # (this test only validates the model, not the client)
