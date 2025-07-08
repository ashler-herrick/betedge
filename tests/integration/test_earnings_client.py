"""
Integration test for earnings client.

Tests that the EarningsClient can fetch real data from NASDAQ API.
"""

import io
import logging

import pytest
import pyarrow.parquet as pq

from betedge_data.alternative.earnings.client import EarningsClient
from betedge_data.alternative.earnings.models import EarningsRequest

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)


@pytest.mark.integration
def test_earnings_client_integration():
    """Test that earnings client can fetch monthly data from NASDAQ API."""
    try:
        with EarningsClient() as client:
            # Test with a recent complete month
            request = EarningsRequest(year=2023, month=12)

            # Test the main function
            parquet_data = client.get_monthly_earnings_as_parquet(request)

            # Verify we got valid data
            assert isinstance(parquet_data, io.BytesIO)
            assert parquet_data.getvalue()  # Should have some data

            # Verify Parquet structure
            parquet_data.seek(0)
            table = pq.read_table(parquet_data)

            # Should have expected columns
            expected_columns = [
                "date",
                "symbol",
                "name",
                "time",
                "eps",
                "eps_forecast",
                "surprise_pct",
                "market_cap",
                "fiscal_quarter_ending",
                "num_estimates",
            ]
            assert table.column_names == expected_columns
            logger.info(f"✓ Retrieved {len(parquet_data.getvalue())} bytes of earnings data for 2023-12")
            logger.info(f"✓ Parquet contains {table.num_rows} earnings records")
            import polars as pl

            df = pl.from_arrow(table)
            print(df.filter(pl.col("time").is_not_null()))
            print(df)

    except Exception as e:
        if "connection" in str(e).lower() or "timeout" in str(e).lower() or "HTTP" in str(e):
            pytest.skip(f"NASDAQ API not available: {e}")
        else:
            raise


if __name__ == "__main__":
    test_earnings_client_integration()
