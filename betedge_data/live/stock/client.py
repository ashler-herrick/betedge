"""
Live stock data client for ThetaData API.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, Any, List, Union, Tuple
from urllib.parse import urlencode

import httpx

from betedge_data.live.config import LiveClientConfig
from .models import LiveQuoteRequest

logger = logging.getLogger(__name__)


class LiveStockClient:
    """Client for retrieving live stock data from ThetaData API."""

    def __init__(self):
        """
        Initialize the live stock client.

        Args:
            config: Configuration instance, uses default if None
        """
        self.config = LiveClientConfig()
        self.client = httpx.Client(
            timeout=self.config.timeout,
            limits=httpx.Limits(max_connections=50, max_keepalive_connections=8, keepalive_expiry=30.0),
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

    def get_quote_at_time(
        self,
        ticker: Union[str, List[str]],
        date: str,
        time_ms: int,
        venue: Optional[str] = None,
        max_workers: Optional[int] = None,
    ) -> Union[Dict[str, Any], List[Tuple[str, Dict[str, Any]]]]:
        """
        Get quote data at specific time for one or more tickers.

        Args:
            ticker: Stock symbol (e.g., 'AAPL') or list of symbols (e.g., ['AAPL', 'MSFT'])
            date: Date in YYYYMMDD format
            time_ms: Time in milliseconds since midnight ET (e.g. 34200000 for 9:30 AM ET)
            venue: Data venue (default uses config)
            max_workers: Maximum number of threads for multi-ticker requests

        Returns:
            Single ticker: Dict with quote data
            Multiple tickers: List of Tuple[ticker, Dict] with quote data
        """
        # Handle single ticker vs multiple tickers
        if isinstance(ticker, str):
            # Single ticker
            return self._get_single_quote(ticker, date, time_ms, venue)
        else:
            # Multiple tickers - use threading
            logger.info(f"Getting quotes for {len(ticker)} tickers on {date} at time {time_ms}")

            kwargs = {"date": date, "time_ms": time_ms, "venue": venue}

            return list(self._get_multiple_quotes(ticker, max_workers, **kwargs))

    def _get_single_quote(self, ticker: str, date: str, time_ms: int, venue: Optional[str] = None) -> Dict[str, Any]:
        """
        Get quote data for a single ticker at specific time.

        Args:
            ticker: Single stock symbol
            date: Date in YYYYMMDD format
            time_ms: Time in milliseconds since midnight ET
            venue: Data venue

        Returns:
            Dict with quote data
        """
        request = LiveQuoteRequest(
            ticker=ticker,
            start_date=date,
            end_date=date,
            ivl=time_ms,
            use_csv=self.config.use_csv,
            venue=venue,
            rth=True,
        )

        url = self._build_quote_url(request)
        logger.debug(f"Getting live quote for {ticker} on {date} at time {time_ms}")

        try:
            response = self.client.get(url)
            response.raise_for_status()

            # Parse JSON response
            data = response.json()

            # Extract quote from response
            if "response" in data and isinstance(data["response"], list) and len(data["response"]) > 0:
                quote_data = data["response"][0]
                return self._convert_quote_to_dict(quote_data)
            else:
                logger.warning(f"No quote data found for {ticker} on {date} at time {time_ms}")
                return {}

        except httpx.HTTPStatusError as e:
            error_detail = e.response.text if hasattr(e.response, "text") else str(e)
            logger.error(f"HTTP error for {ticker}: {e.response.status_code}: {error_detail}")
            raise e
        except Exception as e:
            logger.error(f"Error getting quote for {ticker}: {e}")
            raise e

    def _get_multiple_quotes(
        self, tickers: List[str], max_workers: Optional[int] = None, **kwargs
    ) -> List[Tuple[str, Dict[str, Any]]]:
        """
        Get quotes for multiple tickers using threading.

        Args:
            tickers: List of stock symbols
            max_workers: Maximum number of threads
            **kwargs: Arguments to pass to _get_single_quote

        Returns:
            List of Tuple[ticker, quote_data] for each ticker
        """
        if not tickers:
            return []

        # Default to config max_concurrent_requests, respect user override
        workers = max_workers or min(len(tickers), self.config.max_concurrent_requests)

        logger.info(f"Getting quotes for {len(tickers)} tickers with {workers} threads")

        results = []

        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Submit one task per ticker
            future_to_ticker = {executor.submit(self._get_single_quote, ticker, **kwargs): ticker for ticker in tickers}

            # Collect results as they complete
            for future in as_completed(future_to_ticker):
                ticker = future_to_ticker[future]
                try:
                    quote_data = future.result()
                    results.append((ticker, quote_data))
                except Exception as e:
                    logger.error(f"Failed to get quote for ticker {ticker}: {e}")
                    # Continue with other tickers, add empty result
                    results.append((ticker, {}))

        return results

    def _build_quote_url(self, request: LiveQuoteRequest) -> str:
        """Build URL for at_time quote endpoint."""
        params = {
            "root": request.ticker,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "ivl": request.ivl,
            "rth": str(request.rth).lower(),
            "use_csv": str(request.use_csv).lower(),
            "venue": request.venue or self.config.default_venue,
        }

        base_url = f"{self.config.base_url}/at_time/stock/quote"
        return f"{base_url}?{urlencode(params)}"

    def _convert_quote_to_dict(self, item: List) -> Dict[str, Any]:
        """
        Convert quote array to dict for JSON serialization.
        Uses tuple unpacking for optimal performance.

        Args:
            item: JSON array representing quote data

        Returns:
            Dict ready for JSON serialization
        """
        if len(item) < 10:
            logger.warning(f"Expected 10 quote fields, got {len(item)}")
            # Return partial data if available
            if len(item) >= 4:
                return {
                    "ms_of_day": item[0] if len(item) > 0 else None,
                    "bid_size": item[1] if len(item) > 1 else None,
                    "bid_exchange": item[2] if len(item) > 2 else None,
                    "bid": item[3] if len(item) > 3 else None,
                    "bid_condition": item[4] if len(item) > 4 else None,
                    "ask_size": item[5] if len(item) > 5 else None,
                    "ask_exchange": item[6] if len(item) > 6 else None,
                    "ask": item[7] if len(item) > 7 else None,
                    "ask_condition": item[8] if len(item) > 8 else None,
                    "date": item[9] if len(item) > 9 else None,
                }
            else:
                return {}

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

    def close(self) -> None:
        """Close the HTTP client."""
        if hasattr(self, "client"):
            self.client.close()
