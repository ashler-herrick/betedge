#!/usr/bin/env python3
"""
Performance testing for the optimized HistoricalStockClient.

Tests memory efficiency, throughput, and scaling characteristics
with increasing data volumes using consistent 1-minute intervals.
"""

import time
import psutil
import logging
import gc
import threading
from typing import List, Tuple
from dataclasses import dataclass
from contextlib import contextmanager

from betedge_data.historical.stock.client import HistoricalStockClient
from betedge_data.config import ThetaClientConfig

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetrics:
    """Performance metrics for a test run."""

    test_name: str
    record_count: int
    duration_seconds: float
    records_per_second: float
    peak_memory_mb: float
    avg_memory_mb: float
    network_requests: int
    cpu_percent: float
    errors: List[str]


class MemoryMonitor:
    """Monitor memory usage during streaming."""

    def __init__(self, interval: float = 0.1):
        self.interval = interval
        self.memory_samples = []
        self.running = False
        self.thread = None
        self.process = psutil.Process()

    def start(self):
        """Start memory monitoring."""
        self.running = True
        self.memory_samples = []
        self.thread = threading.Thread(target=self._monitor)
        self.thread.start()

    def stop(self) -> Tuple[float, float]:
        """Stop monitoring and return (peak_mb, avg_mb)."""
        self.running = False
        if self.thread:
            self.thread.join()

        if not self.memory_samples:
            return 0.0, 0.0

        peak_mb = max(self.memory_samples) / 1024 / 1024
        avg_mb = sum(self.memory_samples) / len(self.memory_samples) / 1024 / 1024
        return peak_mb, avg_mb

    def _monitor(self):
        """Background memory monitoring."""
        while self.running:
            try:
                memory_info = self.process.memory_info()
                self.memory_samples.append(memory_info.rss)
                time.sleep(self.interval)
            except Exception as e:
                logger.warning(f"Memory monitoring error: {e}")
                break


@contextmanager
def performance_test(test_name: str):
    """Context manager for performance testing."""
    logger.info(f"Starting performance test: {test_name}")

    # Setup
    gc.collect()  # Clean up before test
    monitor = MemoryMonitor()
    start_time = time.time()
    start_cpu = psutil.cpu_percent()

    # Track network requests by counting log entries
    network_requests = 0
    errors = []

    monitor.start()

    try:
        yield monitor, errors
    except Exception as e:
        errors.append(str(e))
        logger.error(f"Test {test_name} failed: {e}")
    finally:
        # Cleanup
        duration = time.time() - start_time
        peak_memory, avg_memory = monitor.stop()
        end_cpu = psutil.cpu_percent()

        # Basic metrics (will be updated by test)
        metrics = PerformanceMetrics(
            test_name=test_name,
            record_count=0,  # To be filled by test
            duration_seconds=duration,
            records_per_second=0.0,  # To be calculated
            peak_memory_mb=peak_memory,
            avg_memory_mb=avg_memory,
            network_requests=network_requests,
            cpu_percent=(start_cpu + end_cpu) / 2,
            errors=errors,
        )

        logger.info(f"Test {test_name} completed in {duration:.2f}s")
        logger.info(f"Memory: Peak={peak_memory:.1f}MB, Avg={avg_memory:.1f}MB")


class HistoricalClientPerformanceTest:
    """Performance test suite for HistoricalStockClient."""

    def __init__(self):
        self.config = ThetaClientConfig(
            stock_tier="value",
            timeout=120,  # Longer timeout for large datasets
            max_concurrent_requests=4,  # Sync with ThetaData HTTP_CONCURRENCY
        )
        self.results: List[PerformanceMetrics] = []

    def run_all_tests(self):
        """Run complete performance test suite."""
        logger.info("Starting Historical Client Performance Test Suite")

        # Test scenarios: volume scaling with consistent 1-minute intervals
        test_scenarios = [
            ("Small Volume (1 day, 1min)", self.test_small_volume),
            ("Medium Volume (1 week, 1min)", self.test_medium_volume),
            ("Large Volume (1 month, 1min)", self.test_large_volume),
            ("Multi-Ticker 2 (AAPL+MSFT)", self.test_multi_ticker_pairs),
            ("Multi-Ticker 4 (Basket)", self.test_multi_ticker_basket),
        ]

        for test_name, test_func in test_scenarios:
            try:
                test_func()
            except Exception as e:
                logger.error(f"Test '{test_name}' failed: {e}")

        self.print_results()

    def test_small_volume(self):
        """Test with small volume - 1 day of 1-minute OHLC data (~390 records)."""
        with performance_test("Small Volume") as (monitor, errors):
            with HistoricalStockClient(self.config) as client:
                record_count = 0
                start_time = time.time()

                for record in client.stream_ohlc_dicts(
                    ticker="AAPL",
                    start_date="20241220",
                    end_date="20241220",
                    # Uses config default interval (60000 = 1 minute)
                ):
                    record_count += 1

                duration = time.time() - start_time
                rps = record_count / duration if duration > 0 else 0

                metrics = PerformanceMetrics(
                    test_name="Small Volume",
                    record_count=record_count,
                    duration_seconds=duration,
                    records_per_second=rps,
                    peak_memory_mb=monitor.stop()[0],
                    avg_memory_mb=monitor.stop()[1],
                    network_requests=1,
                    cpu_percent=psutil.cpu_percent(),
                    errors=errors,
                )

                self.results.append(metrics)
                logger.info(f"Small volume: {record_count} records at {rps:.1f} rec/sec")

    def test_medium_volume(self):
        """Test with medium volume - 1 week of 1-minute data (~1,950 records)."""
        with performance_test("Medium Volume") as (monitor, errors):
            with HistoricalStockClient(self.config) as client:
                record_count = 0
                start_time = time.time()

                for record in client.stream_ohlc_dicts(
                    ticker="AAPL",
                    start_date="20241216",  # Week of Dec 16-20
                    end_date="20241220",
                    # Uses config default interval (60000 = 1 minute)
                ):
                    record_count += 1

                    # Memory check every 500 records
                    if record_count % 500 == 0:
                        current_memory = psutil.Process().memory_info().rss / 1024 / 1024
                        logger.debug(f"At {record_count} records: {current_memory:.1f}MB")

                duration = time.time() - start_time
                rps = record_count / duration if duration > 0 else 0

                metrics = PerformanceMetrics(
                    test_name="Medium Volume",
                    record_count=record_count,
                    duration_seconds=duration,
                    records_per_second=rps,
                    peak_memory_mb=monitor.stop()[0],
                    avg_memory_mb=monitor.stop()[1],
                    network_requests=3,  # Estimated pages for week
                    cpu_percent=psutil.cpu_percent(),
                    errors=errors,
                )

                self.results.append(metrics)
                logger.info(f"Medium volume: {record_count} records at {rps:.1f} rec/sec")

    def test_large_volume(self):
        """Test with large volume - 1 month of 1-minute data (~8,190 records)."""
        with performance_test("Large Volume") as (monitor, errors):
            with HistoricalStockClient(self.config) as client:
                record_count = 0
                start_time = time.time()

                for record in client.stream_ohlc_dicts(
                    ticker="AAPL",
                    start_date="20241120",  # Year of data
                    end_date="20241220",
                    # Uses config default interval (60000 = 1 minute)
                ):
                    record_count += 1

                    # Progress logging every 1000 records
                    if record_count % 1000 == 0:
                        elapsed = time.time() - start_time
                        rate = record_count / elapsed if elapsed > 0 else 0
                        current_memory = psutil.Process().memory_info().rss / 1024 / 1024
                        logger.info(
                            f"Large volume progress: {record_count} records ({rate:.1f} rec/sec, {current_memory:.1f}MB)"
                        )

                duration = time.time() - start_time
                rps = record_count / duration if duration > 0 else 0

                metrics = PerformanceMetrics(
                    test_name="Large Volume",
                    record_count=record_count,
                    duration_seconds=duration,
                    records_per_second=rps,
                    peak_memory_mb=monitor.stop()[0],
                    avg_memory_mb=monitor.stop()[1],
                    network_requests=15,  # Estimated pages for month
                    cpu_percent=psutil.cpu_percent(),
                    errors=errors,
                )

                self.results.append(metrics)
                logger.info(f"Large volume: {record_count} records at {rps:.1f} rec/sec")

    def test_multi_ticker_pairs(self):
        """Test multi-ticker performance with 2 tickers (pairs trading scenario)."""
        with performance_test("Multi-Ticker Pairs") as (monitor, errors):
            with HistoricalStockClient(self.config) as client:
                record_count = 0
                ticker_counts = {"AAPL": 0, "MSFT": 0}
                start_time = time.time()

                # Stream 2 tickers simultaneously
                for ticker, record in client.stream_ohlc_dicts(
                    ticker=["AAPL", "MSFT"],
                    start_date="20241120",
                    end_date="20241220",
                    # Uses config max_concurrent_requests
                ):
                    record_count += 1
                    ticker_counts[ticker] += 1

                    # Log progress every 200 records
                    if record_count % 200 == 0:
                        elapsed = time.time() - start_time
                        rate = record_count / elapsed if elapsed > 0 else 0
                        logger.debug(f"Multi-ticker progress: {record_count} records ({rate:.1f} rec/sec)")

                duration = time.time() - start_time
                rps = record_count / duration if duration > 0 else 0

                metrics = PerformanceMetrics(
                    test_name="Multi-Ticker Pairs",
                    record_count=record_count,
                    duration_seconds=duration,
                    records_per_second=rps,
                    peak_memory_mb=monitor.stop()[0],
                    avg_memory_mb=monitor.stop()[1],
                    network_requests=2,  # 2 tickers
                    cpu_percent=psutil.cpu_percent(),
                    errors=errors,
                )

                self.results.append(metrics)
                logger.info(f"Multi-ticker pairs: {record_count} total records at {rps:.1f} rec/sec")
                logger.info(f"  AAPL: {ticker_counts['AAPL']} records, MSFT: {ticker_counts['MSFT']} records")

    def test_multi_ticker_basket(self):
        """Test multi-ticker performance with 4 tickers (basket trading scenario)."""
        with performance_test("Multi-Ticker Basket") as (monitor, errors):
            with HistoricalStockClient(self.config) as client:
                record_count = 0
                ticker_counts = {"AAPL": 0, "MSFT": 0, "GOOGL": 0, "TSLA": 0}
                start_time = time.time()

                # Stream 4 tickers simultaneously
                for ticker, record in client.stream_ohlc_dicts(
                    ticker=["AAPL", "MSFT", "GOOGL", "TSLA"],
                    start_date="20241120",
                    end_date="20241220",
                    # Uses config max_concurrent_requests
                ):
                    record_count += 1
                    ticker_counts[ticker] += 1

                    # Log progress every 400 records
                    if record_count % 400 == 0:
                        elapsed = time.time() - start_time
                        rate = record_count / elapsed if elapsed > 0 else 0
                        current_memory = psutil.Process().memory_info().rss / 1024 / 1024
                        logger.info(
                            f"Basket progress: {record_count} records ({rate:.1f} rec/sec, {current_memory:.1f}MB)"
                        )

                duration = time.time() - start_time
                rps = record_count / duration if duration > 0 else 0

                metrics = PerformanceMetrics(
                    test_name="Multi-Ticker Basket",
                    record_count=record_count,
                    duration_seconds=duration,
                    records_per_second=rps,
                    peak_memory_mb=monitor.stop()[0],
                    avg_memory_mb=monitor.stop()[1],
                    network_requests=4,  # 4 tickers
                    cpu_percent=psutil.cpu_percent(),
                    errors=errors,
                )

                self.results.append(metrics)
                logger.info(f"Multi-ticker basket: {record_count} total records at {rps:.1f} rec/sec")
                for ticker, count in ticker_counts.items():
                    logger.info(f"  {ticker}: {count} records")

    def print_results(self):
        """Print comprehensive performance results."""
        print("\n" + "=" * 80)
        print("HISTORICAL CLIENT PERFORMANCE TEST RESULTS")
        print("=" * 80)

        print(
            f"{'Test Name':<25} {'Records':<10} {'Duration':<10} {'Rec/Sec':<10} {'Peak MB':<10} {'Avg MB':<10} {'Errors':<8}"
        )
        print("-" * 80)

        for result in self.results:
            errors_count = len(result.errors)
            print(
                f"{result.test_name:<25} {result.record_count:<10} {result.duration_seconds:<10.2f} "
                f"{result.records_per_second:<10.1f} {result.peak_memory_mb:<10.1f} {result.avg_memory_mb:<10.1f} {errors_count:<8}"
            )

        print("\n" + "=" * 80)
        print("BOTTLENECK ANALYSIS")
        print("=" * 80)

        # Find bottlenecks
        fastest_test = max(self.results, key=lambda x: x.records_per_second)
        slowest_test = min(self.results, key=lambda x: x.records_per_second)
        highest_memory = max(self.results, key=lambda x: x.peak_memory_mb)
        most_errors = max(self.results, key=lambda x: len(x.errors))

        print(f"Fastest throughput: {fastest_test.test_name} ({fastest_test.records_per_second:.1f} rec/sec)")
        print(f"Slowest throughput: {slowest_test.test_name} ({slowest_test.records_per_second:.1f} rec/sec)")
        print(f"Highest memory usage: {highest_memory.test_name} ({highest_memory.peak_memory_mb:.1f} MB)")
        print(f"Most errors: {most_errors.test_name} ({len(most_errors.errors)} errors)")

        # Performance recommendations
        print("\n" + "=" * 80)
        print("PERFORMANCE RECOMMENDATIONS")
        print("=" * 80)

        avg_memory = sum(r.peak_memory_mb for r in self.results) / len(self.results)
        avg_throughput = sum(r.records_per_second for r in self.results) / len(self.results)

        print(f"Average memory usage: {avg_memory:.1f} MB")
        print(f"Average throughput: {avg_throughput:.1f} records/sec")

        if avg_memory > 500:
            print("⚠️  High memory usage detected - consider smaller batch sizes")
        else:
            print("✅ Memory usage within acceptable limits")

        if avg_throughput < 50:
            print("⚠️  Low throughput detected - check network latency and ThetaData server performance")
        else:
            print("✅ Throughput performance acceptable")

        total_errors = sum(len(r.errors) for r in self.results)
        if total_errors > 0:
            print(f"⚠️  {total_errors} total errors detected across all tests")
            for result in self.results:
                if result.errors:
                    print(f"   {result.test_name}: {', '.join(result.errors)}")
        else:
            print("✅ No errors detected")


def main():
    """Run performance test suite."""
    print("Historical Client Performance Test Suite")
    print("Testing scaling characteristics with 1-minute OHLC data:")
    print("- Small Volume: 1 day (~390 records)")
    print("- Medium Volume: 1 week (~1,950 records)")
    print("- Large Volume: 1 month (~8,190 records)")
    print("- Multi-Ticker Pairs: 2 tickers simultaneous (AAPL+MSFT)")
    print("- Multi-Ticker Basket: 4 tickers simultaneous (AAPL+MSFT+GOOGL+TSLA)")
    print("\nMake sure ThetaTerminal is running and authenticated...")

    input("Press Enter to start performance tests...")

    test_suite = HistoricalClientPerformanceTest()
    test_suite.run_all_tests()


if __name__ == "__main__":
    main()
