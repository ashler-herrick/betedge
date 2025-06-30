#!/usr/bin/env python3
"""
Test script for HistoricalStockClient.
"""

import logging
from betedge_data.historical.stock.client import HistoricalStockClient

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_ohlc_streaming():
    """Test OHLC data streaming."""
    print("=" * 50)
    print("Testing OHLC Data Streaming")
    print("=" * 50)

    with HistoricalStockClient() as client:
        try:
            count = 0
            for record in client.stream_ohlc_dicts(
                ticker="AAPL",
                start_date="20240101",
                end_date="20240102",  # Short range for testing
                interval=60000,  # 1 minute bars
            ):
                print(f"OHLC Record {count + 1}:")
                print(f"  Date: {record.date}")
                print(f"  Time: {record.ms_of_day}")
                print(f"  OHLC: {record.open:.2f} / {record.high:.2f} / {record.low:.2f} / {record.close:.2f}")
                print(f"  Volume: {record.volume}")
                print(f"  Count: {record.count}")
                print()

                count += 1
                if count >= 5:  # Limit output for testing
                    break

            print(f"Successfully processed {count} OHLC records")

        except Exception as e:
            print(f"Error during OHLC streaming: {e}")
            return False

    return True


def test_quote_streaming():
    """Test Quote data streaming."""
    print("=" * 50)
    print("Testing Quote Data Streaming")
    print("=" * 50)

    with HistoricalStockClient() as client:
        try:
            count = 0
            for record in client.stream_quotes_dicts(
                ticker="AAPL",
                start_date="20240101",
                end_date="20240102",  # Short range for testing
            ):
                print(f"Quote Record {count + 1}:")
                print(f"  Date: {record.date}")
                print(f"  Time: {record.ms_of_day}")
                print(f"  Bid: {record.bid:.2f} x {record.bid_size} ({record.bid_exchange})")
                print(f"  Ask: {record.ask:.2f} x {record.ask_size} ({record.ask_exchange})")
                print(f"  Conditions: Bid={record.bid_condition}, Ask={record.ask_condition}")
                print()

                count += 1
                if count >= 5:  # Limit output for testing
                    break

            print(f"Successfully processed {count} Quote records")

        except Exception as e:
            print(f"Error during Quote streaming: {e}")
            return False

    return True


def test_error_handling():
    """Test error handling with invalid parameters."""
    print("=" * 50)
    print("Testing Error Handling")
    print("=" * 50)

    with HistoricalStockClient() as client:
        # Test invalid date range
        try:
            list(
                client.stream_ohlc(
                    ticker="AAPL",
                    start_date="20240102",
                    end_date="20240101",  # Invalid: start > end
                    interval=60000,
                )
            )
            print("ERROR: Should have failed with invalid date range")
            return False
        except ValueError as e:
            print(f"✓ Correctly caught invalid date range: {e}")

        # Test invalid date format
        try:
            list(
                client.stream_ohlc_dicts(
                    ticker="AAPL",
                    start_date="2024-01-01",  # Invalid format
                    end_date="20240102",
                    interval=60000,
                )
            )
            print("ERROR: Should have failed with invalid date format")
            return False
        except ValueError as e:
            print(f"✓ Correctly caught invalid date format: {e}")

    return True


def main():
    """Run all tests."""
    print("Starting HistoricalStockClient Tests")
    print("Note: This requires ThetaTerminal to be running and authenticated")
    print()

    tests = [
        ("Error Handling", test_error_handling),
        ("OHLC Streaming", test_ohlc_streaming),
        ("Quote Streaming", test_quote_streaming),
    ]

    results = []
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
            print(f"✓ {test_name}: {'PASSED' if result else 'FAILED'}")
        except Exception as e:
            results.append((test_name, False))
            print(f"✗ {test_name}: FAILED with exception: {e}")
        print()

    # Summary
    print("=" * 50)
    print("Test Summary")
    print("=" * 50)
    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "PASSED" if result else "FAILED"
        print(f"{test_name}: {status}")

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("🎉 All tests passed!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1


if __name__ == "__main__":
    exit(main())
