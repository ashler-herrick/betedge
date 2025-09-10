"""
Generalized HTTP client with pagination support for API interactions.
"""

import logging
from typing import Any, Dict, Iterator, Optional, Type, TypeVar

import httpx
import ijson
from pydantic import BaseModel, ValidationError

from betedge_data.data_service.common.exceptions import NoDataAvailableError

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)

# Module-level HTTP client instance
_http_client: Optional["PaginatedHTTPClient"] = None


def get_http_client() -> "PaginatedHTTPClient":
    """
    Get the shared HTTP client instance.

    Returns:
        Shared PaginatedHTTPClient instance
    """
    global _http_client
    if _http_client is None:
        _http_client = PaginatedHTTPClient(
            timeout=60.0,
            max_connections=100,
            max_keepalive_connections=50,
            http2=True,
        )
    return _http_client


class PaginatedHTTPClient:
    """
    A generalized HTTP client that handles paginated API responses.

    Supports both regular JSON responses and streaming large responses with ijson.
    """

    def __init__(
        self,
        timeout: float = 30.0,
        max_connections: int = 100,
        max_keepalive_connections: int = 50,
        max_pages: int = 100,
        headers: Optional[Dict[str, str]] = None,
        http2: bool = True,
    ):
        """
        Initialize the paginated HTTP client.

        Args:
            timeout: Request timeout in seconds
            max_connections: Maximum number of concurrent connections
            max_keepalive_connections: Maximum keepalive connections
            max_pages: Maximum number of pages to fetch (safety limit)
            headers: Default headers to include in requests
            http2: Whether to use HTTP/2
        """
        self.max_pages = max_pages
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

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.close()

    def close(self) -> None:
        """Close the HTTP client."""
        if hasattr(self, "client"):
            self.client.close()

    def fetch_paginated(
        self,
        url: str,
        response_model: Type[T],
        stream_response: bool = False,
        item_path: Optional[str] = None,
        next_page_path: str = "header.next_page",
        collect_items: bool = True,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> T:
        """
        Fetch paginated data from an API endpoint and validate with Pydantic model.

        Args:
            url: The API endpoint URL
            response_model: Pydantic model to validate responses (required)
            stream_response: Whether to stream parse large responses with ijson
            item_path: ijson path to items when streaming (e.g., "response.item")
            next_page_path: Path to next page URL in response
            collect_items: Whether to collect all items across pages
            params: Optional query parameters
            headers: Optional headers to override defaults

        Returns:
            Validated Pydantic model instance

        Raises:
            httpx.HTTPStatusError: For HTTP errors
            ValidationError: If response validation fails
            RuntimeError: For other processing errors
        """
        all_items = [] if collect_items else None
        current_url = url
        page_count = 0
        first_response_header = None

        header: Dict[str, Any] = {}
        data: Dict[str, Any] = {}
        # Build initial URL with params
        if params:
            request = self.client.build_request("GET", current_url, params=params)
            current_url = str(request.url)

        while current_url and page_count < self.max_pages:
            page_count += 1
            logger.debug(f"Fetching page {page_count} from {current_url}")

            try:
                # Fetch single page
                response = self._fetch_single_page(current_url, headers)

                # Process response based on format
                if stream_response and item_path:
                    page_items = list(self._stream_items(response, item_path))
                    if collect_items and all_items is not None:
                        all_items.extend(page_items)

                    # Extract header for pagination
                    header = self._extract_json_value(response.content, "header")
                    if page_count == 1:
                        first_response_header = header
                else:
                    # Check if response is "No data" text before parsing JSON
                    response_text = response.text
                    if response_text.startswith(":No data for the specified timeframe"):
                        raise NoDataAvailableError(f"No data available: {response_text}")

                    # Parse as regular JSON
                    data = response.json()

                    try:
                        validated = response_model(**data)
                        data = validated.model_dump()
                    except ValidationError as e:
                        logger.error(f"Response validation failed: {e}")
                        raise

                    # Extract items and header
                    if page_count == 1:
                        first_response_header = data.get("header", {})

                    if collect_items and all_items is not None:
                        # Try common response patterns
                        items = data.get("response", data.get("data", data.get("items", [])))
                        if isinstance(items, list):
                            all_items.extend(items)

                # Get next page URL
                if stream_response:
                    current_url = self._get_nested_value(header, next_page_path)
                else:
                    current_url = self._get_nested_value(data, next_page_path)

                # Check if next page is valid
                if current_url and str(current_url).lower() in ["null", "none", ""]:
                    current_url = None

            except httpx.HTTPStatusError as e:
                error_detail = e.response.text if hasattr(e.response, "text") else str(e)
                logger.error(f"HTTP error on page {page_count}: {e.response.status_code}: {error_detail}")
                raise
            except Exception as e:

                if isinstance(e, NoDataAvailableError):
                    raise
                logger.error(f"Error fetching page {page_count}: {e}")
                raise RuntimeError(f"Failed to fetch page {page_count}: {e}") from e

        logger.info(f"Fetched {page_count} pages" + (f" with {len(all_items)} total items" if all_items else ""))

        # Build result dictionary
        result = {
            "header": first_response_header or {"pages_fetched": page_count},
            "response": all_items if collect_items else [],
        }

        # Always validate and return model instance
        try:
            validated_model = response_model(**result)
            return validated_model
        except ValidationError as e:
            logger.error(f"Response validation failed: {e}")
            raise RuntimeError(f"Failed to validate response with {response_model.__name__}: {e}") from e

    def _fetch_single_page(self, url: str, headers: Optional[Dict[str, str]] = None) -> httpx.Response:
        """
        Fetch a single page of data.

        Args:
            url: The URL to fetch
            headers: Optional headers to use

        Returns:
            httpx Response object

        Raises:
            httpx.HTTPStatusError: For HTTP errors
        """
        try:
            response = self.client.get(url, headers=headers)

            # Check for ThetaData "No data" response (status 472)
            if response.status_code == 472:
                response_text = response.text
                if "No data for the specified timeframe" in response_text:

                    raise NoDataAvailableError(f"No data available: {response_text}")

            # Check for ThetaData "Wrong IP" response (status 476)
            if response.status_code == 476:
                error_msg = (
                    "ThetaData IP mismatch error (HTTP 476). "
                    "This occurs when requests come from different IP addresses. "
                    "In Docker environments, ensure THETA_BASE_URL uses 'http://172.18.0.1:25510/v2' "
                    "to route all requests through the same network interface."
                )
                raise RuntimeError(error_msg)

            response.raise_for_status()
            return response
        except httpx.HTTPStatusError:
            raise
        except Exception as e:

            if isinstance(e, NoDataAvailableError):
                raise
            logger.error(f"Request failed for {url}: {e}")
            raise

    def _stream_items(self, response: httpx.Response, item_path: str) -> Iterator[Dict[str, Any]]:
        """
        Stream individual items from a JSON response.

        Args:
            response: httpx Response object
            item_path: ijson path to items (e.g., "response.item")

        Yields:
            Individual items from the response
        """
        try:
            content = response.content
            items = ijson.items(content, item_path)

            for item in items:
                if isinstance(item, dict):
                    yield item
                else:
                    logger.warning(f"Unexpected item format: {type(item)}")

        except ijson.JSONError as e:
            logger.error(f"JSON streaming error: {e}")
            raise
        except Exception as e:
            logger.error(f"Streaming error: {e}")
            raise

    def _extract_json_value(self, content: bytes, path: str) -> Any:
        """
        Extract a value from JSON content using ijson.

        Args:
            content: Raw response content
            path: Path to extract (e.g., "header")

        Returns:
            Extracted value or None if not found
        """
        try:
            parser = ijson.items(content, path)
            return next(parser, None)
        except Exception as e:
            logger.warning(f"Could not extract {path}: {e}")
            return None

    def _get_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """
        Get a nested value from a dictionary using dot notation.

        Args:
            data: Dictionary to search
            path: Dot-separated path (e.g., "header.next_page")

        Returns:
            Value at path or None if not found
        """
        try:
            keys = path.split(".")
            value = data

            for key in keys:
                if isinstance(value, dict):
                    value = value.get(key)
                else:
                    return None

            return value
        except Exception:
            return None
