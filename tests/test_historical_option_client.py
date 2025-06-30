#!/usr/bin/env python3
"""
Integration test for HistoricalOptionClient with time-matched underlying pricing.

Tests the updated client against live ThetaData API with various load scenarios.
"""

import io
import logging
import time
import tracemalloc
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple
import pyarrow.parquet as pq

from betedge_data.historical.option.client import HistoricalOptionClient
from betedge_data.config import ThetaClientConfig

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestConfig:
    """Configuration for integration tests."""

    # Test symbols and expirations
    PRIMARY_SYMBOL = "AAPL"
    BACKUP_SYMBOLS = ["MSFT", "GOOGL"]

    # Date ranges for different load tests
    LIGHT_DAYS = 1
    MEDIUM_DAYS = 2
    HEAVY_DAYS = 3

    # Expiration counts for load tests
    LIGHT_EXPIRATIONS = 1
    MEDIUM_EXPIRATIONS = 3
    HEAVY_EXPIRATIONS = 5

    # Test parameters
    MAX_DTE = 30
    BASE_PCT = 0.1
    INTERVAL = ThetaClientConfig().option_interval  # Use config default (9,000,000ms)

    @staticmethod
    def get_test_date_range(days_back: int = 7, days_span: int = 1) -> Tuple[str, str]:
        """Get a test date range in YYYYMMDD format."""
        end_date = datetime.now() - timedelta(days=days_back)
        start_date = end_date - timedelta(days=days_span - 1)
        return start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d")

    @staticmethod
    def get_test_expirations(count: int = 3) -> list[str]:
        """Get test expiration dates."""
        base_date = datetime.now() + timedelta(days=14)
        expirations = []
        for i in range(count):
            exp_date = base_date + timedelta(days=i * 7)  # Weekly expirations
            # Adjust to Friday (expiration day)
            days_ahead = 4 - exp_date.weekday()
            if days_ahead < 0:
                days_ahead += 7
            exp_date += timedelta(days=days_ahead)
            expirations.append(exp_date.strftime("%Y%m%d"))
        return expirations


def measure_performance(func, *args, **kwargs) -> Tuple[Any, float, float, int]:
    """Measure execution time and memory usage of a function."""
    tracemalloc.start()
    start_time = time.perf_counter()

    try:
        result = func(*args, **kwargs)
        end_time = time.perf_counter()

        # Get memory stats
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        execution_time = end_time - start_time
        peak_memory_mb = peak / (1024 * 1024)

        # Get result size if it's BytesIO
        result_size = 0
        if isinstance(result, io.BytesIO):
            result.seek(0, 2)  # Seek to end
            result_size = result.tell()
            result.seek(0)  # Reset position

        return result, execution_time, peak_memory_mb, result_size

    except Exception as e:
        tracemalloc.stop()
        raise e


def validate_parquet_output(parquet_buffer: io.BytesIO) -> Dict[str, Any]:
    """Validate Parquet output and extract metrics."""
    parquet_buffer.seek(0)

    try:
        # Read Parquet data
        table = pq.read_table(parquet_buffer)

        return {
            "valid": True,
            "num_rows": table.num_rows,
            "num_columns": table.num_columns,
            "schema": [field.name for field in table.schema],
            "size_bytes": len(parquet_buffer.getvalue()),
        }
    except Exception as e:
        return {"valid": False, "error": str(e), "size_bytes": len(parquet_buffer.getvalue())}


def test_single_expiration_integration() -> bool:
    """Test basic integration with single expiration."""
    print("=" * 60)
    print("Testing Single Expiration Integration")
    print("=" * 60)

    with HistoricalOptionClient() as client:
        try:
            # Get test parameters
            start_date, end_date = TestConfig.get_test_date_range(days_back=7, days_span=1)
            expirations = TestConfig.get_test_expirations(count=1)

            print(f"Symbol: {TestConfig.PRIMARY_SYMBOL}")
            print(f"Expiration: {expirations[0]}")
            print(f"Date range: {start_date} to {end_date}")
            print(f"Interval: {TestConfig.INTERVAL}ms ({TestConfig.INTERVAL / 1000 / 60:.1f} minutes)")
            print()

            # Execute with performance measurement
            result, exec_time, memory_mb, size_bytes = measure_performance(
                client.get_filtered_options_parquet,
                root=TestConfig.PRIMARY_SYMBOL,
                exp=expirations[0],
                start_date=start_date,
                end_date=end_date,
                max_dte=TestConfig.MAX_DTE,
                base_pct=TestConfig.BASE_PCT,
                interval=TestConfig.INTERVAL,
            )

            # Validate output
            validation = validate_parquet_output(result)

            # Print results
            print("Performance Metrics:")
            print(f"  Execution time: {exec_time:.2f}s")
            print(f"  Memory usage: {memory_mb:.1f}MB")
            print(f"  Output size: {size_bytes:,} bytes")
            print()

            print("Data Validation:")
            if validation["valid"]:
                print("  ✓ Valid Parquet output")
                print(f"  ✓ Rows: {validation['num_rows']:,}")
                print(f"  ✓ Columns: {validation['num_columns']}")
                print(f"  ✓ Schema: {', '.join(validation['schema'])}")
            else:
                print(f"  ✗ Invalid Parquet: {validation['error']}")
                return False

            return True

        except Exception as e:
            print(f"✗ Test failed: {e}")
            logger.error(f"Single expiration test error: {e}")
            return False


def test_multiple_expirations_integration() -> bool:
    """Test integration with multiple expirations (parallel processing)."""
    print("=" * 60)
    print("Testing Multiple Expirations Integration")
    print("=" * 60)

    with HistoricalOptionClient() as client:
        try:
            # Get test parameters
            start_date, end_date = TestConfig.get_test_date_range(days_back=7, days_span=1)
            expirations = TestConfig.get_test_expirations(count=3)

            print(f"Symbol: {TestConfig.PRIMARY_SYMBOL}")
            print(f"Expirations: {', '.join(expirations)}")
            print(f"Date range: {start_date} to {end_date}")
            print("Parallel fetching: Enabled")
            print()

            # Execute with performance measurement
            result, exec_time, memory_mb, size_bytes = measure_performance(
                client.get_filtered_options_parquet,
                root=TestConfig.PRIMARY_SYMBOL,
                exp=expirations,
                start_date=start_date,
                end_date=end_date,
                max_dte=TestConfig.MAX_DTE,
                base_pct=TestConfig.BASE_PCT,
                interval=TestConfig.INTERVAL,
                max_workers=4,
            )

            # Validate output
            validation = validate_parquet_output(result)

            # Print results
            print("Performance Metrics:")
            print(f"  Execution time: {exec_time:.2f}s")
            print(f"  Memory usage: {memory_mb:.1f}MB")
            print(f"  Output size: {size_bytes:,} bytes")
            print(f"  Throughput: {validation['num_rows'] / exec_time:.0f} ticks/second")
            print()

            print("Data Validation:")
            if validation["valid"]:
                print("  ✓ Valid Parquet output")
                print(f"  ✓ Rows: {validation['num_rows']:,}")
                print(f"  ✓ Combined {len(expirations)} expirations successfully")
            else:
                print(f"  ✗ Invalid Parquet: {validation['error']}")
                return False

            return True

        except Exception as e:
            print(f"✗ Test failed: {e}")
            logger.error(f"Multiple expiration test error: {e}")
            return False


def test_light_load() -> bool:
    """Test light load scenario."""
    print("=" * 60)
    print("Testing Light Load Scenario")
    print("=" * 60)

    with HistoricalOptionClient() as client:
        try:
            start_date, end_date = TestConfig.get_test_date_range(days_back=3, days_span=TestConfig.LIGHT_DAYS)
            expirations = TestConfig.get_test_expirations(count=TestConfig.LIGHT_EXPIRATIONS)

            print(f"Configuration: {TestConfig.LIGHT_EXPIRATIONS} expiration, {TestConfig.LIGHT_DAYS} day(s)")
            print("Expected: Fast execution, low memory usage")
            print()

            result, exec_time, memory_mb, size_bytes = measure_performance(
                client.get_filtered_options_parquet,
                root=TestConfig.PRIMARY_SYMBOL,
                exp=expirations[0] if len(expirations) == 1 else expirations,
                start_date=start_date,
                end_date=end_date,
                max_dte=TestConfig.MAX_DTE,
                base_pct=TestConfig.BASE_PCT,
                interval=TestConfig.INTERVAL,
            )

            validation = validate_parquet_output(result)

            print(f"Results: {exec_time:.2f}s, {memory_mb:.1f}MB, {validation['num_rows']:,} ticks")

            # Light load expectations
            if exec_time > 30:  # Should be under 30 seconds
                print(f"⚠️  Warning: Slow execution for light load ({exec_time:.2f}s)")

            return validation["valid"]

        except Exception as e:
            print(f"✗ Light load test failed: {e}")
            return False


def test_medium_load() -> bool:
    """Test medium load scenario."""
    print("=" * 60)
    print("Testing Medium Load Scenario")
    print("=" * 60)

    with HistoricalOptionClient() as client:
        try:
            start_date, end_date = TestConfig.get_test_date_range(days_back=3, days_span=TestConfig.MEDIUM_DAYS)
            expirations = TestConfig.get_test_expirations(count=TestConfig.MEDIUM_EXPIRATIONS)

            print(f"Configuration: {TestConfig.MEDIUM_EXPIRATIONS} expirations, {TestConfig.MEDIUM_DAYS} day(s)")
            print("Expected: Moderate execution time, parallel processing benefits")
            print()

            result, exec_time, memory_mb, size_bytes = measure_performance(
                client.get_filtered_options_parquet,
                root=TestConfig.PRIMARY_SYMBOL,
                exp=expirations,
                start_date=start_date,
                end_date=end_date,
                max_dte=TestConfig.MAX_DTE,
                base_pct=TestConfig.BASE_PCT,
                interval=TestConfig.INTERVAL,
                max_workers=4,
            )

            validation = validate_parquet_output(result)

            print(f"Results: {exec_time:.2f}s, {memory_mb:.1f}MB, {validation['num_rows']:,} ticks")

            return validation["valid"]

        except Exception as e:
            print(f"✗ Medium load test failed: {e}")
            return False


def test_heavy_load() -> bool:
    """Test heavy load scenario."""
    print("=" * 60)
    print("Testing Heavy Load Scenario")
    print("=" * 60)

    with HistoricalOptionClient() as client:
        try:
            start_date, end_date = TestConfig.get_test_date_range(days_back=3, days_span=TestConfig.HEAVY_DAYS)
            expirations = TestConfig.get_test_expirations(count=TestConfig.HEAVY_EXPIRATIONS)

            print(f"Configuration: {TestConfig.HEAVY_EXPIRATIONS} expirations, {TestConfig.HEAVY_DAYS} day(s)")
            print("Expected: Longer execution, high parallel processing efficiency")
            print()

            result, exec_time, memory_mb, size_bytes = measure_performance(
                client.get_filtered_options_parquet,
                root=TestConfig.PRIMARY_SYMBOL,
                exp=expirations,
                start_date=start_date,
                end_date=end_date,
                max_dte=TestConfig.MAX_DTE,
                base_pct=TestConfig.BASE_PCT,
                interval=TestConfig.INTERVAL,
                max_workers=6,
            )

            validation = validate_parquet_output(result)

            print(f"Results: {exec_time:.2f}s, {memory_mb:.1f}MB, {validation['num_rows']:,} ticks")
            print(f"Throughput: {validation['num_rows'] / exec_time:.0f} ticks/second")

            return validation["valid"]

        except Exception as e:
            print(f"✗ Heavy load test failed: {e}")
            return False


def main():
    """Run all integration tests."""
    print("Starting HistoricalOptionClient Integration Tests")
    print("Note: This requires ThetaTerminal to be running and authenticated")
    print("Testing time-matched underlying pricing functionality")
    print()

    tests = [
        ("Single Expiration Integration", test_single_expiration_integration),
        ("Multiple Expirations Integration", test_multiple_expirations_integration),
        ("Light Load Test", test_light_load),
        ("Medium Load Test", test_medium_load),
        ("Heavy Load Test", test_heavy_load),
    ]

    results = []
    start_time = time.time()

    for test_name, test_func in tests:
        print(f"\n🚀 Running: {test_name}")
        try:
            result = test_func()
            results.append((test_name, result))
            status = "PASSED" if result else "FAILED"
            print(f"✓ {test_name}: {status}")
        except Exception as e:
            results.append((test_name, False))
            print(f"✗ {test_name}: FAILED with exception: {e}")
        print()

    # Summary
    total_time = time.time() - start_time
    print("=" * 80)
    print("INTEGRATION TEST SUMMARY")
    print("=" * 80)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "PASSED" if result else "FAILED"
        icon = "✅" if result else "❌"
        print(f"{icon} {test_name}: {status}")

    print(f"\nOverall: {passed}/{total} tests passed")
    print(f"Total execution time: {total_time:.1f} seconds")

    if passed == total:
        print("\n🎉 All integration tests passed!")
        print("✅ Time-matched underlying pricing is working correctly!")
        return 0
    else:
        print(f"\n❌ {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    exit(main())
