"""
Historical stock data client for ThetaData API.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Generator, Optional, Dict, Any, List, Callable, Iterator, Union, Tuple
from urllib.parse import urlencode

import httpx
import ijson

from betedge_data.historical.config import HistoricalClientConfig
from .models import HistoricalStockRequest

logger = logging.getLogger(__name__)


class HistoricalStockClient:
    """Client for streaming historical stock data from ThetaData API."""

    def __init__(self):
        """
        Initialize the historical stock client.

        Args:
            config: Configuration instance, uses default if None
        """
        self.config = HistoricalClientConfig()
        self.client = httpx.Client(
            timeout=self.config.timeout,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=20),
            transport=httpx.HTTPTransport(retries=0),
            http2=True,
        )

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        if hasattr(self, "client"):
            self.client.close()

    def stream_ohlc_dicts(
        self,
        ticker: Union[str, List[str]],
        start_date: str,
        end_date: str,
        interval: Optional[int] = None,
        rth: bool = True,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        venue: Optional[str] = None,
        max_workers: Optional[int] = None,
    ) -> Union[Generator[Dict[str, Any], None, None], Generator[Tuple[str, Dict[str, Any]], None, None]]:
        """
        Stream OHLC data as dicts for efficient processing.

        Args:
            ticker: Stock symbol (e.g., 'AAPL') or list of symbols (e.g., ['AAPL', 'MSFT'])
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            interval: Interval size in milliseconds
            rth: Regular trading hours only (default True)
            start_time: Start time in milliseconds since midnight ET
            end_time: End time in milliseconds since midnight ET
            venue: Data venue (default uses config)
            max_workers: Maximum number of threads for multi-ticker streaming

        Yields:
            Single ticker: Dict records with OHLC data
            Multiple tickers: Tuple[ticker, Dict] records with OHLC data
        """
        # Handle single ticker vs multiple tickers
        if isinstance(ticker, str):
            # Single ticker - existing logic
            request = HistoricalStockRequest(
                ticker=ticker,
                start_date=start_date,
                end_date=end_date,
                interval=interval or self.config.stock_interval,
                rth=rth,
                start_time=start_time,
                end_time=end_time,
                venue=venue,
            )

            self._validate_date_range(request.start_date, request.end_date)

            url = self._build_ohlc_url(request)
            logger.info(f"Streaming OHLC dicts for {ticker} ({start_date} to {end_date})")

            yield from self._stream_records(url, self._convert_ohlc_to_dict)
        else:
            # Multiple tickers - use threading
            logger.info(f"Streaming OHLC dicts for {len(ticker)} tickers ({start_date} to {end_date})")

            kwargs = {
                "start_date": start_date,
                "end_date": end_date,
                "interval": interval,
                "rth": rth,
                "start_time": start_time,
                "end_time": end_time,
                "venue": venue,
            }

            yield from self._stream_multiple_tickers(ticker, self._stream_single_ticker_ohlc, max_workers, **kwargs)

    def stream_quotes_dicts(
        self,
        ticker: Union[str, List[str]],
        start_date: str,
        end_date: str,
        rth: bool = True,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        venue: Optional[str] = None,
        interval: Optional[int] = None,
        max_workers: Optional[int] = None,
    ) -> Union[Generator[Dict[str, Any], None, None], Generator[Tuple[str, Dict[str, Any]], None, None]]:
        """
        Stream Quote data as dicts for efficient processing.

        Args:
            ticker: Stock symbol (e.g., 'AAPL') or list of symbols (e.g., ['AAPL', 'MSFT'])
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            rth: Regular trading hours only (default True)
            start_time: Start time in milliseconds since midnight ET
            end_time: End time in milliseconds since midnight ET
            venue: Data venue (default uses config)
            interval: Interval size in milliseconds (optional)
            max_workers: Maximum number of threads for multi-ticker streaming

        Yields:
            Single ticker: Dict records with Quote data
            Multiple tickers: Tuple[ticker, Dict] records with Quote data
        """
        # Handle single ticker vs multiple tickers
        if isinstance(ticker, str):
            # Single ticker - existing logic
            request = HistoricalStockRequest(
                ticker=ticker,
                start_date=start_date,
                end_date=end_date,
                interval=interval or self.config.stock_interval,
                rth=rth,
                start_time=start_time,
                end_time=end_time,
                venue=venue,
            )

            self._validate_date_range(request.start_date, request.end_date)

            url = self._build_quote_url(request)

            # Log tier-aware venue selection
            venue_used = request.venue or (
                "utp_ca" if self.config.stock_tier.lower() == "value" else self.config.default_venue
            )
            logger.info(
                f"Streaming Quote dicts for {ticker} ({start_date} to {end_date}) (tier={self.config.stock_tier}, venue={venue_used})"
            )

            yield from self._stream_records(url, self._convert_quote_to_dict)
        else:
            # Multiple tickers - use threading
            logger.info(
                f"Streaming Quote dicts for {len(ticker)} tickers ({start_date} to {end_date}) (tier={self.config.stock_tier})"
            )

            kwargs = {
                "start_date": start_date,
                "end_date": end_date,
                "rth": rth,
                "start_time": start_time,
                "end_time": end_time,
                "venue": venue,
                "interval": interval,
            }

            yield from self._stream_multiple_tickers(ticker, self._stream_single_ticker_quotes, max_workers, **kwargs)

    def _build_ohlc_url(self, request: HistoricalStockRequest) -> str:
        """Build URL for OHLC endpoint."""
        params = self._build_params(request)
        params["ivl"] = request.interval  # Required for OHLC

        base_url = f"{self.config.base_url}/hist/stock/ohlc"
        return f"{base_url}?{urlencode(params)}"

    def _build_quote_url(self, request: HistoricalStockRequest) -> str:
        """Build URL for Quote endpoint with tier-aware venue selection."""
        params = self._build_params(request)
        if request.interval is not None:
            params["ivl"] = request.interval

        # Override venue for value tier if not explicitly set by user
        if request.venue is None and self.config.stock_tier.lower() == "value":
            params["venue"] = "utp_ca"
            logger.debug("Using venue=utp_ca for value tier quote request")

        base_url = f"{self.config.base_url}/hist/stock/quote"
        return f"{base_url}?{urlencode(params)}"

    def _build_params(self, request: HistoricalStockRequest) -> Dict[str, Any]:
        """Build common query parameters."""
        params = {
            "root": request.ticker,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "rth": str(request.rth).lower(),
            "use_csv": str(self.config.use_csv).lower(),
            "pretty_time": str(self.config.pretty_time).lower(),
            "venue": request.venue or self.config.default_venue,
        }

        if request.start_time is not None:
            params["start_time"] = request.start_time
        if request.end_time is not None:
            params["end_time"] = request.end_time

        return params

    def _stream_records(
        self, url: str, converter_func: Callable[[List], Dict[str, Any]]
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Stream individual records with minimal memory usage.

        Args:
            url: ThetaData API endpoint
            converter_func: Function to convert array to dict

        Yields:
            Dict records ready for JSON serialization
        """
        current_url = url
        page_count = 0
        total_records = 0
        start_time = time.time()

        while current_url:
            page_count += 1
            logger.debug(f"Streaming page {page_count}: {current_url.split('?')[0]}")

            try:
                request_start = time.time()

                # Standard HTTP request
                response = self.client.get(current_url)
                response.raise_for_status()

                request_duration = time.time() - request_start
                logger.debug(f"Request completed in {request_duration:.2f}s")

                # Stream individual items (memory efficient)
                page_records = 0
                for item in self._stream_json_items(response):
                    try:
                        record = converter_func(item)
                        yield record
                        page_records += 1
                        total_records += 1
                    except Exception as e:
                        logger.warning(f"Failed to convert item: {e}")
                        continue

                # Calculate performance metrics
                if page_records > 0 and request_duration > 0:
                    records_per_sec = page_records / request_duration
                    logger.debug(f"Page {page_count}: {page_records} records ({records_per_sec:.1f} records/sec)")
                else:
                    logger.debug(f"Page {page_count}: {page_records} records")

                # Handle pagination
                header = self._get_json_header(response)
                current_url = header.get("next_page")

                # Handle various representations of "no next page"
                if current_url and str(current_url).lower() not in ["null", "none", ""]:
                    logger.debug(f"Next page available: {current_url}")
                else:
                    logger.debug("No more pages available")
                    current_url = None

            except httpx.HTTPStatusError as e:
                error_detail = e.response.text if hasattr(e.response, "text") else str(e)
                logger.error(f"HTTP error {e.response.status_code}: {error_detail}")
                logger.debug(f"Failed URL: {current_url}")
                raise e
            except Exception as e:
                logger.error(f"Streaming error: {e}")
                logger.debug(f"Failed URL: {current_url}", exc_info=True)
                raise e

        total_duration = time.time() - start_time
        if total_records > 0 and total_duration > 0:
            overall_rate = total_records / total_duration
            logger.info(
                f"Stream complete: {total_records} records across {page_count} pages in {total_duration:.2f}s ({overall_rate:.1f} rec/sec)"
            )
        else:
            logger.info(f"Stream complete: {total_records} records across {page_count} pages")

    def _stream_json_items(self, response: httpx.Response) -> Iterator[List]:
        """
        Stream individual items from ThetaData JSON response.
        Memory usage: <1KB per record vs 50MB+ for full page.

        Args:
            response: httpx Response object

        Yields:
            Individual record arrays from response.response
        """
        try:
            # Parse: response.response[0], response.response[1], etc.
            content = response.content
            items = ijson.items(content, "response.item")

            for item in items:
                if isinstance(item, list):
                    yield item
                else:
                    logger.warning(f"Unexpected item format: {type(item)}")

        except ijson.JSONError as e:
            logger.error(f"JSON streaming error: {e}")
            raise e
        except Exception as e:
            logger.error(f"Streaming error: {e}")
            raise e

    def _get_json_header(self, response: httpx.Response) -> Dict[str, Any]:
        """
        Extract header from ThetaData response for pagination.

        Args:
            response: httpx Response object

        Returns:
            Header dict or empty dict if parsing fails
        """
        try:
            # Reset response stream by creating a new response from content
            content = response.content
            header_parser = ijson.items(content, "header")
            return next(header_parser, {})
        except Exception as e:
            logger.warning(f"Could not extract header: {e}")
            return {}

    def _convert_ohlc_to_dict(self, item: List) -> Dict[str, Any]:
        """
        Convert OHLC array to dict for JSON serialization.
        Uses tuple unpacking for optimal performance.

        Args:
            item: JSON array representing OHLC data

        Returns:
            Dict ready for JSON serialization
        """
        if len(item) < 8:
            raise ValueError(f"Expected 8 OHLC fields, got {len(item)}")

        ms_of_day, open_val, high, low, close, volume, count, date = item[:8]
        return {
            "ms_of_day": ms_of_day,
            "open": open_val,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "count": count,
            "date": date,
        }

    def _convert_quote_to_dict(self, item: List) -> Dict[str, Any]:
        """
        Convert Quote array to dict for JSON serialization.
        Uses tuple unpacking for optimal performance.

        Args:
            item: JSON array representing quote data

        Returns:
            Dict ready for JSON serialization
        """
        if len(item) < 10:
            raise ValueError(f"Expected 10 Quote fields, got {len(item)}")

        ms_of_day, bid_size, bid_exchange, bid, bid_condition, ask_size, ask_exchange, ask, ask_condition, date = item[
            :10
        ]
        return {
            "ms_of_day": ms_of_day,
            "bid_size": bid_size,
            "bid_exchange": bid_exchange,
            "bid": bid,
            "bid_condition": bid_condition,
            "ask_size": ask_size,
            "ask_exchange": ask_exchange,
            "ask": ask,
            "ask_condition": ask_condition,
            "date": date,
        }

    def _validate_date_range(self, start_date: str, end_date: str) -> None:
        """
        Validate date range parameters.

        Args:
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format

        Raises:
            ValueError: If date range is invalid
        """
        # Basic format validation
        if len(start_date) != 8 or not start_date.isdigit():
            raise ValueError(f"Invalid start_date format: {start_date}. Expected YYYYMMDD")
        if len(end_date) != 8 or not end_date.isdigit():
            raise ValueError(f"Invalid end_date format: {end_date}. Expected YYYYMMDD")

        # Range validation
        if start_date > end_date:
            raise ValueError(f"start_date {start_date} must be <= end_date {end_date}")

    def _stream_single_ticker_ohlc(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        interval: Optional[int] = None,
        rth: bool = True,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        venue: Optional[str] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Helper method for streaming single ticker OHLC data.
        Used internally by threading implementation.

        Args:
            ticker: Single stock symbol
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            interval: Interval size in milliseconds
            rth: Regular trading hours only
            start_time: Start time in milliseconds since midnight ET
            end_time: End time in milliseconds since midnight ET
            venue: Data venue

        Yields:
            Dict records with OHLC data
        """
        request = HistoricalStockRequest(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            interval=interval or self.config.stock_interval,
            rth=rth,
            start_time=start_time,
            end_time=end_time,
            venue=venue,
        )

        self._validate_date_range(request.start_date, request.end_date)

        url = self._build_ohlc_url(request)
        logger.debug(f"Streaming OHLC for single ticker {ticker}")

        yield from self._stream_records(url, self._convert_ohlc_to_dict)

    def _stream_single_ticker_quotes(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
        rth: bool = True,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        venue: Optional[str] = None,
        interval: Optional[int] = None,
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Helper method for streaming single ticker Quote data.
        Used internally by threading implementation.

        Args:
            ticker: Single stock symbol
            start_date: Start date in YYYYMMDD format
            end_date: End date in YYYYMMDD format
            rth: Regular trading hours only
            start_time: Start time in milliseconds since midnight ET
            end_time: End time in milliseconds since midnight ET
            venue: Data venue
            interval: Interval size in milliseconds

        Yields:
            Dict records with Quote data
        """
        request = HistoricalStockRequest(
            ticker=ticker,
            start_date=start_date,
            end_date=end_date,
            interval=interval or self.config.stock_interval,
            rth=rth,
            start_time=start_time,
            end_time=end_time,
            venue=venue,
        )

        self._validate_date_range(request.start_date, request.end_date)

        url = self._build_quote_url(request)
        logger.debug(f"Streaming Quote for single ticker {ticker}")

        yield from self._stream_records(url, self._convert_quote_to_dict)

    def _stream_multiple_tickers(
        self, tickers: List[str], stream_func: Callable, max_workers: Optional[int] = None, **kwargs
    ) -> Generator[Tuple[str, Dict[str, Any]], None, None]:
        """
        Generic multi-ticker threading implementation.

        Args:
            tickers: List of stock symbols
            stream_func: Single ticker streaming function
            max_workers: Maximum number of threads
            **kwargs: Arguments to pass to stream_func

        Yields:
            Tuple[ticker, record] for each record from all tickers
        """
        if not tickers:
            return

        # Default to config max_concurrent_requests, respect user override
        workers = max_workers or min(len(tickers), self.config.max_concurrent_requests)

        logger.info(f"Starting multi-ticker stream for {len(tickers)} tickers with {workers} threads")

        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit one task per ticker
            future_to_ticker = {executor.submit(list, stream_func(ticker, **kwargs)): ticker for ticker in tickers}

            # Yield records as they complete
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    records = future.result()
                    for record in records:
                        yield ticker, record
                except Exception as e:
                    logger.error(f"Failed to stream ticker {ticker}: {e}")
                    # Continue with other tickers
                    continue

    def close(self) -> None:
        """Close the HTTP client."""
        if hasattr(self, "client"):
            self.client.close()
