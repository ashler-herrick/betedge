"""
Simple HTTP client for JSON and CSV responses.
"""

from io import BytesIO
from typing import Any, Dict, Optional
import logging
import time

import httpx

from betedge_data.exceptions import NoDataAvailableError
from betedge_data.job import HTTPJob, ReturnType

logger = logging.getLogger(__name__)


class HTTPClient:
    """
    Simple HTTP client for fetching JSON and CSV responses.

    Supports:
    - JSON responses with optional Pydantic validation
    - CSV responses returned as StringIO for easy parsing
    - Raw responses for custom handling
    """

    def __init__(
        self,
        timeout: float = 120.0,
        headers: Optional[Dict[str, str]] = None,
        max_connections: int = 100,
        max_keepalive_connections: int = 50,
        http2: bool = True,
    ):
        """
        Initialize the HTTP client.

        Args:
            timeout: Request timeout in seconds
            headers: Default headers to include in requests
            max_connections: Maximum number of concurrent connections
            max_keepalive_connections: Maximum keepalive connections
            http2: Whether to use HTTP/2
        """
        self.client = httpx.Client(
            timeout=timeout,
            limits=httpx.Limits(
                max_connections=max_connections,
                max_keepalive_connections=max_keepalive_connections,
            ),
            transport=httpx.HTTPTransport(retries=0),
            http2=http2,
            headers=headers or {},
        )

    def fetch(self, job: HTTPJob) -> Optional[HTTPJob]:
        """
        Fetch data for an HTTPJob.

        Args:
            job: HTTPJob containing URL, return type, and headers

        Returns:
            HTTPJob with populated data (csv_buffer or json)
        """
        start_time = time.time()
        logger.debug(
            f"Processing {job.return_type.value.upper()} job for URL: {job.url}"
        )

        try:
            if job.return_type == ReturnType.CSV:
                job.csv_buffer = self.fetch_csv(job.url, job.headers)
            elif job.return_type == ReturnType.JSON:
                job.json = self.fetch_json(job.url, job.headers)

            duration_ms = (time.time() - start_time) * 1000
            logger.debug(
                f"Completed {job.return_type.value.upper()} job for {job.url} in {duration_ms:.1f}ms"
            )

            return job
        except NoDataAvailableError as e:
            logger.warning(
                f"Failed {job.return_type.value.upper()} job for {job.url} due to no data available."
            )
            raise e
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            logger.error(
                f"Failed {job.return_type.value.upper()} job for {job.url} after {duration_ms:.1f}ms: {e}"
            )
            raise

    def fetch_json(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Fetch JSON data from a URL.

        Args:
            url: The URL to fetch, with parameters encoded.
            headers: Optional headers to override defaults

        Returns:
            Parsed JSON dict or validated Pydantic model instance

        Raises:
            httpx.HTTPStatusError: For HTTP errors
            ValueError: If response is not valid JSON
        """
        response = self.fetch_raw(url, headers=headers)

        parse_start = time.time()
        try:
            data = response.json()
            parse_duration_ms = (time.time() - parse_start) * 1000
            logger.debug(
                f"JSON parsing completed for {url} in {parse_duration_ms:.1f}ms"
            )
        except Exception as e:
            parse_duration_ms = (time.time() - parse_start) * 1000
            logger.error(
                f"JSON parsing failed for {url} after {parse_duration_ms:.1f}ms: {e}"
            )
            raise ValueError(f"Invalid JSON response: {e}") from e

        return data

    def fetch_csv(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> BytesIO:
        """
        Fetch CSV data from a URL and return as BytesIO.

        Args:
            url: The URL to fetch
            params: Optional query parameters
            headers: Optional headers to override defaults

        Returns:
            BytesIO object containing CSV data, ready for parsing

        Raises:
            httpx.HTTPStatusError: For HTTP errors

        """
        response = self.fetch_raw(url, headers=headers)

        parse_start = time.time()
        csv_buffer = BytesIO(response.content)
        parse_duration_ms = (time.time() - parse_start) * 1000

        logger.debug(
            f"CSV buffer creation completed for {url} in {parse_duration_ms:.1f}ms"
        )
        return csv_buffer

    def fetch_raw(
        self,
        url: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.Response:
        """
        Fetch raw response from a URL.

        Args:
            url: The URL to fetch
            params: Optional query parameters
            headers: Optional headers to override defaults

        Returns:
            Raw httpx Response object

        Raises:
            httpx.HTTPStatusError: For HTTP errors
            httpx.RequestError: For connection/timeout errors
        """
        start_time = time.time()
        logger.debug(f"Starting HTTP request to: {url}")

        try:
            response = self.client.get(url, headers=headers)
            duration_ms = (time.time() - start_time) * 1000

            # Check for ThetaData "No data" response (status 472)
            if response.status_code == 472:
                response_text = response.text
                ":No data for the specified timeframe & contract."
                if ":No data for the specified timeframe" in response_text:
                    logger.info(f"No data response from {url} ({duration_ms:.1f}ms)")
                    raise NoDataAvailableError(f"No data available: {response_text}")

            response.raise_for_status()

            # Log successful request with timing and response info
            content_length = len(response.content)
            logger.info(
                f"HTTP {response.status_code} {url} - {content_length} bytes in {duration_ms:.1f}ms"
            )

            return response
        except httpx.HTTPStatusError as e:
            duration_ms = (time.time() - start_time) * 1000
            logger.error(
                f"HTTP error {e.response.status_code} for {url} after {duration_ms:.1f}ms: {e.response.text}"
            )
            raise
        except httpx.RequestError as e:
            duration_ms = (time.time() - start_time) * 1000
            logger.error(f"Request error for {url} after {duration_ms:.1f}ms: {e}")
            raise
