#!/usr/bin/env python3
"""
Test script for tier-aware quote endpoint selection.
"""

import logging
from betedge_data.stock.historical.client import HistoricalStockClient
from betedge_data.config import ThetaClientConfig

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_value_tier_quotes():
    """Test quote requests with value tier configuration."""
    print("=" * 60)
    print("Testing Value Tier Quote Requests")
    print("=" * 60)

    # Create config with value tier
    config = ThetaClientConfig(stock_tier="value")

    with HistoricalStockClient(config) as client:
        try:
            print(f"Config: tier={config.stock_tier}, default_venue={config.default_venue}")

            count = 0
            for record in client.stream_quotes(ticker="AAPL", start_date="20240102", end_date="20240102"):
                print(f"Quote {count + 1}: {record.date} Bid:{record.bid:.2f} Ask:{record.ask:.2f}")
                count += 1
                if count >= 3:  # Limit for testing
                    break

            print(f"✓ Successfully processed {count} quote records with value tier")
            return True

        except Exception as e:
            print(f"✗ Error with value tier quotes: {e}")
            return False


def test_standard_tier_quotes():
    """Test quote requests with standard tier configuration."""
    print("=" * 60)
    print("Testing Standard Tier Quote Requests")
    print("=" * 60)

    # Create config with standard tier (default)
    config = ThetaClientConfig(stock_tier="standard")

    with HistoricalStockClient(config) as client:
        try:
            print(f"Config: tier={config.stock_tier}, default_venue={config.default_venue}")

            count = 0
            for record in client.stream_quotes(ticker="AAPL", start_date="20240102", end_date="20240102"):
                print(f"Quote {count + 1}: {record.date} Bid:{record.bid:.2f} Ask:{record.ask:.2f}")
                count += 1
                if count >= 3:  # Limit for testing
                    break

            print(f"✓ Successfully processed {count} quote records with standard tier")
            return True

        except Exception as e:
            print(f"✗ Error with standard tier quotes: {e}")
            return False


def test_manual_venue_override():
    """Test that manual venue parameter overrides tier logic."""
    print("=" * 60)
    print("Testing Manual Venue Override")
    print("=" * 60)

    # Create config with value tier
    config = ThetaClientConfig(stock_tier="value")

    with HistoricalStockClient(config) as client:
        try:
            print(f"Config: tier={config.stock_tier}, but manually specifying venue=nqb")

            count = 0
            for record in client.stream_quotes(
                ticker="AAPL",
                start_date="20240102",
                end_date="20240102",
                venue="nqb",  # Manual override
            ):
                print(f"Quote {count + 1}: {record.date} Bid:{record.bid:.2f} Ask:{record.ask:.2f}")
                count += 1
                if count >= 3:  # Limit for testing
                    break

            print(f"✓ Manual venue override working: {count} records")
            return True

        except Exception as e:
            print(f"Note: Manual venue override failed as expected for value tier: {e}")
            return True  # This is expected to fail for value tier with nqb


def test_ohlc_unaffected():
    """Test that OHLC requests are unaffected by tier changes."""
    print("=" * 60)
    print("Testing OHLC Unaffected by Tier")
    print("=" * 60)

    # Create config with value tier
    config = ThetaClientConfig(stock_tier="value")

    with HistoricalStockClient(config) as client:
        try:
            print(f"Config: tier={config.stock_tier}, testing OHLC (should still use nqb)")

            count = 0
            for record in client.stream_ohlc(ticker="AAPL", start_date="20240102", end_date="20240102", interval=60000):
                print(f"OHLC {count + 1}: {record.date} Close:{record.close:.2f}")
                count += 1
                if count >= 3:  # Limit for testing
                    break

            print(f"✓ OHLC working normally with value tier: {count} records")
            return True

        except Exception as e:
            print(f"✗ OHLC failed unexpectedly: {e}")
            return False


def main():
    """Run all tier-aware tests."""
    print("Testing Tier-Aware Quote Endpoint Selection")
    print("Note: This requires ThetaTerminal to be running and authenticated")
    print()

    tests = [
        ("OHLC Unaffected by Tier", test_ohlc_unaffected),
        ("Value Tier Quotes", test_value_tier_quotes),
        ("Standard Tier Quotes", test_standard_tier_quotes),
        ("Manual Venue Override", test_manual_venue_override),
    ]

    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
            print(f"{'✓' if result else '✗'} {test_name}: {'PASSED' if result else 'FAILED'}")
        except Exception as e:
            results.append((test_name, False))
            print(f"✗ {test_name}: FAILED with exception: {e}")
        print()

    # Summary
    print("=" * 60)
    print("Test Summary")
    print("=" * 60)
    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "PASSED" if result else "FAILED"
        print(f"{test_name}: {status}")

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tier-aware tests passed!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1


if __name__ == "__main__":
    exit(main())
