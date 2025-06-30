#!/usr/bin/env python3
"""
Basic functionality tests for LiveStockClient.

Tests core functionality without performance metrics.
"""

import logging

from betedge_data.live.stock.client import LiveStockClient
from betedge_data.live.config import ThetaClientConfig

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class LiveStockClientTest:
    """Test suite for LiveStockClient."""

    def __init__(self):
        self.config = ThetaClientConfig(stock_tier="value", timeout=30, max_concurrent_requests=4)

    def run_all_tests(self):
        """Run all live client tests."""
        logger.info("Starting Live Stock Client Test Suite")

        # Use recent trading day and market hours time
        # 9:30 AM ET = 34200000 ms since midnight ET
        test_date = "20250625"  # Recent trading day
        test_time_ms = 34200000  # 9:30 AM ET

        test_methods = [
            ("Single Ticker Quote", lambda: self.test_single_ticker_quote(test_date, test_time_ms)),
            ("Multi-Ticker Quote", lambda: self.test_multi_ticker_quote(test_date, test_time_ms)),
            ("Error Handling", lambda: self.test_error_handling(test_date)),
        ]

        results = []

        for test_name, test_func in test_methods:
            try:
                logger.info(f"Running test: {test_name}")
                result = test_func()
                results.append((test_name, "PASS", result))
                logger.info(f"✅ {test_name}: PASSED")
            except Exception as e:
                results.append((test_name, "FAIL", str(e)))
                logger.error(f"❌ {test_name}: FAILED - {e}")

        self.print_results(results)

    def test_single_ticker_quote(self, date: str, time_ms: int):
        """Test single ticker quote retrieval."""
        with LiveStockClient(self.config) as client:
            quote = client.get_quote_at_time("AAPL", date, time_ms)

            # Verify response structure
            assert isinstance(quote, dict), f"Expected dict, got {type(quote)}"

            if quote:  # If we got data
                logger.info(f"Single ticker quote keys: {list(quote.keys())}")
                logger.info(f"Sample quote data: {quote}")

                # Check for expected fields
                expected_fields = ["ms_of_day", "bid", "ask", "bid_size", "ask_size"]
                for field in expected_fields:
                    if field in quote:
                        logger.debug(f"Found expected field: {field} = {quote[field]}")
            else:
                logger.warning("No quote data returned (may be outside market hours)")

            return f"Quote retrieval successful, {len(quote)} fields"

    def test_multi_ticker_quote(self, date: str, time_ms: int):
        """Test multi-ticker quote retrieval."""
        tickers = ["AAPL", "MSFT"]

        with LiveStockClient(self.config) as client:
            quotes = client.get_quote_at_time(tickers, date, time_ms)

            # Verify response structure
            assert isinstance(quotes, list), f"Expected list, got {type(quotes)}"
            assert len(quotes) == len(tickers), f"Expected {len(tickers)} quotes, got {len(quotes)}"

            quote_count = 0
            for ticker, quote in quotes:
                assert isinstance(ticker, str), f"Expected ticker as string, got {type(ticker)}"
                assert isinstance(quote, dict), f"Expected quote as dict, got {type(quote)}"
                assert ticker in tickers, f"Unexpected ticker: {ticker}"

                if quote:  # If we got data for this ticker
                    quote_count += 1
                    logger.info(f"Multi-ticker quote for {ticker}: {len(quote)} fields")
                else:
                    logger.warning(f"No quote data for {ticker} (may be outside market hours)")

            return f"Multi-ticker retrieval successful, {quote_count}/{len(tickers)} tickers had data"

    def test_error_handling(self, date: str):
        """Test error handling with invalid inputs."""
        with LiveStockClient(self.config) as client:
            # Test invalid ticker
            try:
                quote = client.get_quote_at_time("INVALID_TICKER", date, 34200000)
                # ThetaData might return empty result rather than error
                logger.info(f"Invalid ticker result: {quote}")
                return "Error handling test completed (empty result for invalid ticker)"
            except Exception as e:
                logger.info(f"Caught expected error for invalid ticker: {e}")
                return f"Error handling test completed (caught: {type(e).__name__})"

    def print_results(self, results):
        """Print test results summary."""
        print("\n" + "=" * 60)
        print("LIVE STOCK CLIENT TEST RESULTS")
        print("=" * 60)

        print(f"{'Test Name':<30} {'Status':<10} {'Details'}")
        print("-" * 60)

        passed = 0
        failed = 0

        for test_name, status, details in results:
            print(f"{test_name:<30} {status:<10} {details}")
            if status == "PASS":
                passed += 1
            else:
                failed += 1

        print("\n" + "=" * 60)
        print(f"SUMMARY: {passed} passed, {failed} failed")

        if failed == 0:
            print("✅ All tests passed!")
        else:
            print(f"❌ {failed} test(s) failed")

        print("=" * 60)


def main():
    """Run live client test suite."""
    print("Live Stock Client Test Suite")
    print("Testing basic functionality:")
    print("- Single ticker quote retrieval")
    print("- Multi-ticker quote retrieval")
    print("- Error handling")
    print("\nAssuming ThetaTerminal is running and authenticated...")

    test_suite = LiveStockClientTest()
    test_suite.run_all_tests()


if __name__ == "__main__":
    main()
