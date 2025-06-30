#!/usr/bin/env python3
"""
Test script for BulkHistoricalOptionClient.
"""

import logging
from betedge_data.historical.option.client import BulkHistoricalOptionClient

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_bulk_option_quotes():
    """Test bulk option quote data retrieval."""
    print("=" * 50)
    print("Testing Bulk Historical Option Quote Data")
    print("=" * 50)

    with BulkHistoricalOptionClient() as client:
        try:
            # Test with a short date range and all expirations
            quotes = client.get_bulk_option_quotes(
                root="AAPL",
                start_date="20240315",
                end_date="20240315",  # Single day for testing
                exp=0,  # All expirations
                ivl=60000,  # 1 minute bars
            )
            
            print(f"Retrieved {len(quotes)} quote records")
            
            # Display first few records for inspection
            for i, quote in enumerate(quotes[:3]):
                print(f"Quote Record {i + 1}:")
                print(f"  Data: {quote}")
                print()

            if len(quotes) > 3:
                print(f"... and {len(quotes) - 3} more records")

            return True

        except Exception as e:
            print(f"Error during bulk option quote retrieval: {e}")
            return False


def test_url_building():
    """Test URL building functionality."""
    print("=" * 50)
    print("Testing URL Building")
    print("=" * 50)
    
    with BulkHistoricalOptionClient() as client:
        from betedge_data.historical.option.models import BulkHistoricalOptionRequest
        
        # Test basic request
        request = BulkHistoricalOptionRequest(
            root="AAPL",
            start_date="20240315",
            end_date="20240315",
            exp=0,
            ivl=60000
        )
        
        url = client._build_bulk_url(request)
        print(f"Generated URL: {url}")
        
        # Verify URL components
        expected_params = [
            "root=AAPL",
            "exp=0", 
            "start_date=20240315",
            "end_date=20240315",
            "ivl=60000",
            "use_csv=false",
            "pretty_time=false"
        ]
        
        for param in expected_params:
            if param in url:
                print(f"✓ Found parameter: {param}")
            else:
                print(f"✗ Missing parameter: {param}")
                return False
                
        return True


def main():
    """Run all tests."""
    print("Starting BulkHistoricalOptionClient Tests")
    print("Note: This requires ThetaTerminal to be running and authenticated")
    print()

    tests = [
        ("URL Building", test_url_building),
        ("Bulk Option Quotes", test_bulk_option_quotes),
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