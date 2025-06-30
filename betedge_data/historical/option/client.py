import io
import logging
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import Dict, Any, Iterator
from urllib.parse import urlencode

import httpx
import ijson
import orjson
import fast_parser

from betedge_data.historical.config import HistoricalClientConfig
from betedge_data.historical.option.models import HistoricalOptionRequest

logger = logging.getLogger(__name__)


def _decimal_default(obj):
    """Default function for orjson to handle Decimal objects."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


class HistoricalOptionClient:
    """Client for fetching and filtering historical option data from ThetaData API."""

    def __init__(self):
        """
        Initialize the historical option client.

        Args:
            config: Configuration instance, uses default if None
        """
        self.config = HistoricalClientConfig()
        self.max_workers = self.config.max_concurrent_requests
        self.client = httpx.Client(
            timeout=self.config.timeout,
            limits=httpx.Limits(max_connections=100, max_keepalive_connections=50),
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

    def get_filtered_bulk_quote_as_parquet(self, request: HistoricalOptionRequest) -> io.BytesIO:
        """
        Get filtered option data as Parquet bytes for streaming.

        Uses time-matched underlying prices from historical stock data for accurate
        moneyness filtering.

        Args:
            request: HistoricalOptionRequest with all parameters and validation

        Returns:
            BytesIO containing filtered Parquet data ready for streaming
        """
        logger.info(f"Starting parquet data fetch for {request.root} from {request.start_date} to {request.end_date}")

        # Validate return format
        if request.return_format != "parquet":
            raise ValueError(f"Expected return_format='parquet', got '{request.return_format}'")

        # Fetch option and stock data
        logger.debug(f"Fetching option and stock data for {request.root}")
        option_json, stock_json = self._fetch_option_and_stock_data(request)
        logger.debug(
            f"Data fetch complete - option JSON length: {len(option_json)}, stock JSON length: {len(stock_json)}"
        )

        # Apply Rust filtering with both option and stock data
        current_yyyymmdd = int(request.start_date)  # Use start_date for DTE calculation

        try:
            logger.debug(
                f"Starting Rust filtering with params: current_date={current_yyyymmdd}, max_dte={request.max_dte}, base_pct={request.base_pct}"
            )
            # Pass both option and stock JSON to Rust parser
            parquet_bytes = fast_parser.filter_contracts_to_parquet_bytes(
                option_json, stock_json, current_yyyymmdd, request.max_dte, request.base_pct
            )

            if isinstance(parquet_bytes, list):
                parquet_bytes = bytes(parquet_bytes)

            # Wrap in BytesIO for streaming
            parquet_buffer = io.BytesIO(parquet_bytes)

            logger.info(f"Filtering complete: {len(parquet_bytes)} bytes Parquet data generated")
            return parquet_buffer

        except Exception as e:
            logger.error(f"Rust filtering failed for {request.root}: {str(e)}")
            logger.debug(f"Rust filtering error details for {request.root}", exc_info=True)
            raise RuntimeError(f"Option filtering failed: {e}") from e

    def get_filtered_bulk_quote_as_ipc(self, request: HistoricalOptionRequest) -> io.BytesIO:
        """
        Get filtered option data as Arrow IPC bytes for streaming.

        Uses time-matched underlying prices from historical stock data for accurate
        moneyness filtering.

        Args:
            request: HistoricalOptionRequest with all parameters and validation

        Returns:
            BytesIO containing filtered Arrow IPC data ready for streaming
        """
        # Validate return format
        if request.return_format != "ipc":
            raise ValueError(f"Expected return_format='ipc', got '{request.return_format}'")

        # Fetch option and stock data
        option_json, stock_json = self._fetch_option_and_stock_data(request)

        # Apply Rust filtering with both option and stock data
        current_yyyymmdd = int(request.start_date)  # Use start_date for DTE calculation

        try:
            # Pass both option and stock JSON to Rust parser
            ipc_bytes = fast_parser.filter_contracts_to_ipc_bytes(
                option_json, stock_json, current_yyyymmdd, request.max_dte, request.base_pct
            )

            if isinstance(ipc_bytes, list):
                ipc_bytes = bytes(ipc_bytes)

            # Wrap in BytesIO for streaming
            ipc_buffer = io.BytesIO(ipc_bytes)

            logger.info(f"Filtering complete: {len(ipc_bytes)} bytes Arrow IPC data generated")
            return ipc_buffer

        except Exception as e:
            logger.error(f"Rust filtering failed: {e}")
            raise RuntimeError(f"Option filtering failed: {e}") from e

    def _fetch_option_and_stock_data(self, request: HistoricalOptionRequest) -> tuple[str, str]:
        """
        Fetch option and stock data in parallel and return as JSON strings.

        Args:
            request: HistoricalOptionRequest with all parameters

        Returns:
            Tuple of (option_json, stock_json)
        """
        logger.info(
            f"Fetching option and stock data for {request.root} exp={request.exp} "
            f"({request.start_date} to {request.end_date})"
        )

        # Fetch stock and option data in parallel
        with ThreadPoolExecutor(max_workers=2) as executor:
            # Submit both tasks
            stock_future = executor.submit(self._fetch_stock_json, request)
            option_future = executor.submit(self._fetch_option_bulk_quote, request)

            # Collect results
            try:
                stock_json = stock_future.result()
                logger.info(f"Successfully fetched stock data for {request.root}: {len(stock_json)} characters")
            except Exception as e:
                logger.error(f"Failed to fetch stock data for {request.root}: {str(e)}")
                logger.debug(f"Stock data fetch error details for {request.root}", exc_info=True)
                raise RuntimeError(f"Stock data fetch failed: {e}") from e

            try:
                option_json = option_future.result()
                logger.info(
                    f"Successfully fetched option data for {request.root} exp {request.exp}: {len(option_json)} characters"
                )
            except Exception as e:
                logger.error(f"Failed to fetch option data for {request.root} exp {request.exp}: {str(e)}")
                logger.debug(f"Option data fetch error details for {request.root} exp {request.exp}", exc_info=True)
                raise RuntimeError(f"Option data fetch failed: {e}") from e
        return option_json, stock_json

    def _fetch_option_bulk_quote(self, request: HistoricalOptionRequest) -> str:
        """
        Fetch option data and return as JSON string.

        Args:
            request: HistoricalOptionRequest with all parameters

        Returns:
            JSON string containing all option data
        """
        url = self._build_option_url(request)
        logger.debug(f"Fetching option data for {request.root} exp {request.exp}")

        # Collect all records for this expiration
        all_records = []
        current_url = url
        page_count = 0

        while current_url:
            page_count += 1
            logger.debug(f"Fetching page {page_count} for exp {request.exp}")

            try:
                response = self.client.get(current_url)
                response.raise_for_status()

                # Stream and collect all records from this page
                for item in self._stream_json_items(response):
                    all_records.append(item)

                # Handle pagination
                header = self._get_json_header(response)
                current_url = header.get("next_page")

                if current_url and str(current_url).lower() not in ["null", "none", ""]:
                    logger.debug(f"Next page available for exp {request.exp}")
                else:
                    current_url = None

            except httpx.HTTPStatusError as e:
                error_detail = e.response.text if hasattr(e.response, "text") else str(e)
                logger.error(f"HTTP error for exp {request.exp}: {e.response.status_code}: {error_detail}")
                raise e
            except Exception as e:
                logger.error(f"Error fetching exp {request.exp}: {e}")
                raise e

        logger.info(f"Collected {len(all_records)} records for exp {request.exp} across {page_count} pages")

        # Convert to ThetaData-compatible JSON format
        response_data = {
            "header": {
                "latency_ms": 0,
                "format": [
                    "ms_of_day",
                    "bid_size",
                    "bid_exchange",
                    "bid",
                    "bid_condition",
                    "ask_size",
                    "ask_exchange",
                    "ask",
                    "ask_condition",
                    "date",
                ],
            },
            "response": all_records,
        }

        return orjson.dumps(response_data, default=_decimal_default).decode("utf-8")

    def _fetch_stock_json(self, request: HistoricalOptionRequest) -> str:
        """
        Fetch stock quote data and return as JSON string.

        Args:
            request: HistoricalOptionRequest with all parameters

        Returns:
            JSON string containing stock quote data
        """
        url = self._build_stock_url(request)
        logger.debug(f"Fetching stock data for {request.root}")

        # Collect all stock ticks
        all_ticks = []
        current_url = url
        page_count = 0

        while current_url:
            page_count += 1
            logger.debug(f"Fetching stock page {page_count}")

            try:
                response = self.client.get(current_url)
                response.raise_for_status()

                # Parse the response to get ticks
                data = response.json()
                if "response" in data and isinstance(data["response"], list):
                    all_ticks.extend(data["response"])

                # Handle pagination
                header = data.get("header", {})
                current_url = header.get("next_page")

                if current_url and str(current_url).lower() not in ["null", "none", ""]:
                    logger.debug("Next page available for stock data")
                else:
                    current_url = None

            except httpx.HTTPStatusError as e:
                error_detail = e.response.text if hasattr(e.response, "text") else str(e)
                logger.error(f"HTTP error fetching stock data: {e.response.status_code}: {error_detail}")
                raise e
            except Exception as e:
                logger.error(f"Error fetching stock data: {e}")
                raise e

        logger.info(f"Collected {len(all_ticks)} stock ticks across {page_count} pages")

        # Convert to expected format
        stock_response = {
            "header": {
                "latency_ms": 0,
                "format": [
                    "ms_of_day",
                    "bid_size",
                    "bid_exchange",
                    "bid",
                    "bid_condition",
                    "ask_size",
                    "ask_exchange",
                    "ask",
                    "ask_condition",
                    "date",
                ],
            },
            "response": all_ticks,
        }

        return orjson.dumps(stock_response, default=_decimal_default).decode("utf-8")

    def _build_option_url(self, request: HistoricalOptionRequest) -> str:
        """Build URL for ThetaData bulk option quote endpoint."""
        params = {
            "root": request.root,
            "exp": request.exp,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "use_csv": str(self.config.use_csv).lower(),
            "pretty_time": str(self.config.pretty_time).lower(),
        }

        if request.interval is not None:
            params["ivl"] = request.interval
        if request.start_time is not None:
            params["start_time"] = request.start_time
        if request.end_time is not None:
            params["end_time"] = request.end_time

        base_url = f"{self.config.base_url}/bulk_hist/option/quote"
        return f"{base_url}?{urlencode(params)}"

    def _build_stock_url(self, request: HistoricalOptionRequest) -> str:
        """Build URL for ThetaData historical stock quote endpoint."""
        params = {
            "root": request.root,
            "start_date": request.start_date,
            "end_date": request.end_date,
            "use_csv": str(self.config.use_csv).lower(),
            "pretty_time": str(self.config.pretty_time).lower(),
        }

        if request.interval is not None:
            params["ivl"] = request.interval
        if request.start_time is not None:
            params["start_time"] = request.start_time
        if request.end_time is not None:
            params["end_time"] = request.end_time

        base_url = f"{self.config.base_url}/hist/stock/quote"
        return f"{base_url}?{urlencode(params)}"

    def _stream_json_items(self, response: httpx.Response) -> Iterator[Dict[str, Any]]:
        """
        Stream individual option contract items from ThetaData JSON response.

        Args:
            response: httpx Response object

        Yields:
            Individual contract records with ticks and contract info
        """
        try:
            content = response.content
            items = ijson.items(content, "response.item")

            for item in items:
                if isinstance(item, dict):
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
            content = response.content
            header_parser = ijson.items(content, "header")
            return next(header_parser, {})
        except Exception as e:
            logger.warning(f"Could not extract header: {e}")
            return {}

    def close(self) -> None:
        """Close the HTTP client."""
        if hasattr(self, "client"):
            self.client.close()
