"""
Your goal is to add unit tests for the historical option client in this file.

Your tests should be as minimal as required to ensure the code is working, and should be robust to changes.
Do not do things like check that all fields are present in the request, or mock key functionality.
You should mark tests as unit if they do not require the theta client to be running, i.e. they do not send requests.
Tests that require the theta client to be running should be marked integration. 

Remember that your tests should be minimal and "information dense", i.e. they test the key 
components of functionality without making many assumptions or being brittle to future changes.
"""

import pytest
import json
import io
from betedge_data.historical.option.client import HistoricalOptionClient
from betedge_data.historical.option.models import HistoricalOptionRequest


class TestHistoricalOptionRequest:
    """Test the request model validation."""
    
    @pytest.mark.unit
    def test_exp_zero_requires_same_dates(self):
        """Test that exp=0 requires start_date to equal end_date."""
        # Valid: exp=0 with same dates
        request = HistoricalOptionRequest(
            root="AAPL", 
            start_date="20231201", 
            end_date="20231201",
            max_dte=30, 
            base_pct=0.1, 
            exp=0
        )
        # Should not raise
        assert request.exp == 0
        
        # Invalid: exp=0 with different dates
        with pytest.raises(ValueError, match="Start date must equal end date for exp=0"):
            HistoricalOptionRequest(
                root="AAPL", 
                start_date="20231201", 
                end_date="20231202",
                max_dte=30, 
                base_pct=0.1, 
                exp=0
            )
    
    @pytest.mark.unit
    def test_specific_exp_allows_date_ranges(self):
        """Test that specific expirations allow broader date ranges."""
        # Valid: specific exp with date range
        request = HistoricalOptionRequest(
            root="AAPL", 
            start_date="20231201", 
            end_date="20231205",
            max_dte=30, 
            base_pct=0.1, 
            exp="20231215"
        )
        # Should not raise
        assert request.exp == "20231215"
        assert request.start_date != request.end_date
    
    @pytest.mark.unit
    def test_return_format_validation(self):
        """Test return_format validation in model."""
        # Valid formats
        for fmt in ["parquet", "ipc"]:
            request = HistoricalOptionRequest(
                root="AAPL", 
                start_date="20231201", 
                end_date="20231201",
                max_dte=30, 
                base_pct=0.1, 
                return_format=fmt
            )
            assert request.return_format == fmt
        
        # Invalid format
        with pytest.raises(ValueError, match="expected one of: 'parquet', 'ipc'"):
            HistoricalOptionRequest(
                root="AAPL", 
                start_date="20231201", 
                end_date="20231201",
                max_dte=30, 
                base_pct=0.1, 
                return_format="json"
            )


class TestHistoricalOptionClient:
    """Test client logic without external dependencies."""
    
    @pytest.mark.unit
    def test_parquet_method_validates_format(self):
        """Test get_filtered_bulk_quote_as_parquet validates return_format."""
        client = HistoricalOptionClient()
        
        # Valid parquet request should not raise during format validation
        # (may raise later due to network, but that's tested in integration tests)
        
        # Invalid: ipc format for parquet method
        invalid_request = HistoricalOptionRequest(
            root="AAPL", 
            start_date="20231201", 
            end_date="20231201",
            max_dte=30, 
            base_pct=0.1, 
            return_format="ipc"
        )
        with pytest.raises(ValueError, match="Expected return_format='parquet'"):
            client.get_filtered_bulk_quote_as_parquet(invalid_request)
    
    @pytest.mark.unit
    def test_ipc_method_validates_format(self):
        """Test get_filtered_bulk_quote_as_ipc validates return_format."""
        client = HistoricalOptionClient()
        
        # Invalid: parquet format for ipc method
        invalid_request = HistoricalOptionRequest(
            root="AAPL", 
            start_date="20231201", 
            end_date="20231201",
            max_dte=30, 
            base_pct=0.1, 
            return_format="parquet"
        )
        with pytest.raises(ValueError, match="Expected return_format='ipc'"):
            client.get_filtered_bulk_quote_as_ipc(invalid_request)
    
    @pytest.mark.unit
    def test_exp_zero_not_supported_error(self):
        """Test that exp=0 raises not supported error in data fetching."""
        client = HistoricalOptionClient()
        
        request = HistoricalOptionRequest(
            root="AAPL", 
            start_date="20231201", 
            end_date="20231201",
            max_dte=30, 
            base_pct=0.1, 
            exp=0, 
            return_format="parquet"
        )
        
        with pytest.raises(ValueError, match="exp=0.*not yet supported"):
            client._fetch_option_and_stock_data(request)
    
    @pytest.mark.unit
    def test_json_combination_logic(self):
        """Test _combine_json_responses handles multiple JSON strings correctly."""
        client = HistoricalOptionClient()
        
        # Create sample JSON responses matching ThetaData format
        json1 = json.dumps({
            "header": {
                "latency_ms": 0,
                "format": ["ms_of_day", "bid_size", "bid_exchange", "bid", "bid_condition", 
                          "ask_size", "ask_exchange", "ask", "ask_condition", "date"]
            },
            "response": [
                {"contract": "AAPL_231215C150", "ticks": [[34200000, 100, 1, 150.5]]},
                {"contract": "AAPL_231215C155", "ticks": [[34200000, 50, 1, 155.0]]}
            ]
        })
        
        json2 = json.dumps({
            "header": {
                "latency_ms": 0,
                "format": ["ms_of_day", "bid_size", "bid_exchange", "bid", "bid_condition", 
                          "ask_size", "ask_exchange", "ask", "ask_condition", "date"]
            },
            "response": [
                {"contract": "AAPL_231215C160", "ticks": [[34200000, 25, 1, 160.0]]}
            ]
        })
        
        combined = client._combine_json_responses([json1, json2])
        combined_data = json.loads(combined)
        
        # Should have combined all contracts from both responses
        assert len(combined_data["response"]) == 3
        assert "header" in combined_data
        assert combined_data["header"]["format"] is not None
        
        # Verify specific contracts are present
        contracts = [item.get("contract", "") for item in combined_data["response"]]
        assert "AAPL_231215C150" in contracts
        assert "AAPL_231215C160" in contracts
    
    @pytest.mark.unit
    def test_json_combination_empty_input(self):
        """Test _combine_json_responses handles empty input."""
        client = HistoricalOptionClient()
        
        combined = client._combine_json_responses([])
        combined_data = json.loads(combined)
        
        assert combined_data["response"] == []
        assert "header" in combined_data


class TestHistoricalOptionClientIntegration:
    """Test client with real ThetaData API calls."""
    
    @pytest.mark.integration
    def test_parquet_generation_end_to_end(self):
        """Test complete flow from request to Parquet output."""
        client = HistoricalOptionClient()
        
        # Test with specific expiration (single day for speed)
        request = HistoricalOptionRequest(
            root="AAPL", 
            start_date="20231201", 
            end_date="20231201",
            max_dte=30, 
            base_pct=0.1, 
            exp="20231215", 
            return_format="parquet"
        )
        
        result = client.get_filtered_bulk_quote_as_parquet(request)
        
        # Verify output format and content
        assert isinstance(result, io.BytesIO)
        assert len(result.getvalue()) > 0
        
        # Verify it's actually Parquet data (starts with Parquet magic bytes)
        result.seek(0)
        magic_bytes = result.read(4)
        assert magic_bytes == b'PAR1'  # Parquet magic number
    
    @pytest.mark.integration
    def test_ipc_generation_end_to_end(self):
        """Test complete flow from request to Arrow IPC output."""
        client = HistoricalOptionClient()
        
        # Test with specific expiration
        request = HistoricalOptionRequest(
            root="AAPL", 
            start_date="20231201", 
            end_date="20231201",
            max_dte=30, 
            base_pct=0.1, 
            exp="20231215", 
            return_format="ipc"
        )
        
        result = client.get_filtered_bulk_quote_as_ipc(request)
        
        # Verify output format and content
        assert isinstance(result, io.BytesIO)
        assert len(result.getvalue()) > 0
        
        # Verify it contains binary data (Arrow IPC is binary format)
        result.seek(0)
        data = result.read(10)  # Read first 10 bytes
        assert len(data) > 0  # Has data
        assert isinstance(data, bytes)  # Is binary data
    
    @pytest.mark.integration
    def test_network_error_handling(self):
        """Test handling of network/API errors with invalid symbols."""
        client = HistoricalOptionClient()
        
        # Test with invalid symbol that should cause API error
        request = HistoricalOptionRequest(
            root="INVALID_SYMBOL_XYZ", 
            start_date="20231201", 
            end_date="20231201",
            max_dte=30, 
            base_pct=0.1, 
            exp="20231215", 
            return_format="parquet"
        )
        
        # Should raise RuntimeError with network/API failure message
        with pytest.raises(RuntimeError, match="failed"):
            client.get_filtered_bulk_quote_as_parquet(request)