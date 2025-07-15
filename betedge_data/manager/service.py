"""
Service layer for data processing operations.

This module provides a unified approach to processing all data request types
using the get_subrequests() and get_client() pattern.
"""

import logging
from uuid import UUID

from betedge_data.manager.models import ExternalBaseRequest
from betedge_data.storage.publisher import MinIOPublisher
from betedge_data.common.exceptions import NoDataAvailableError

logger = logging.getLogger(__name__)


class DataProcessingService:
    """Simplified service for processing data requests using unified pattern."""

    def __init__(self, force_refresh: bool = False):
        """
        Initialize the data processing service.

        Args:
            force_refresh: If True, reprocess existing files (overwrite)
        """
        self.force_refresh = force_refresh
        self.publisher = MinIOPublisher()
        logger.info("DataProcessingService initialized")

    async def process_request(self, request: ExternalBaseRequest, request_id: UUID) -> None:
        """
        Unified method to process any external request type.

        Uses request.generate_requests() and request.get_client() pattern.

        Args:
            request: Any External*Request instance
            request_id: Unique request identifier
        """
        logger.info(f"Processing unified request {request_id}")

        individual_requests = request.get_subrequests()
        logger.info(f"Generated {len(individual_requests)} individual requests")

        # Get the appropriate client using the request's method
        client = request.get_client()
        for req in individual_requests:
            try:
                # Get the object key for this request
                object_key = req.generate_object_key()
                
                # Check if file already exists (unless force_refresh is True)
                if not self.force_refresh and self.publisher.file_exists(object_key):
                    logger.info(
                        f"File already exists for {getattr(req, 'root', 'unknown')} "
                        f"on {getattr(req, 'date', 'unknown date')} - skipping"
                    )
                    continue

                # Use unified client API - all clients have get_data() method
                result = client.get_data(req)

                # Publish the data to MinIO
                await self.publisher.publish(result, object_key)

                logger.info(f"Successfully processed and published request for {getattr(req, 'root', 'unknown')}")
            except NoDataAvailableError:
                logger.info(
                    f"No data available for {getattr(req, 'root', 'unknown')} "
                    f"on {getattr(req, 'date', 'unknown date')} - skipping file creation"
                )
                continue  # Skip to next request without creating empty file
            except Exception as e:
                logger.error(f"Failed to process individual request: {e}")
                continue

    async def close(self) -> None:
        """Close service connections and clean up resources."""
        await self.publisher.close()
        logger.info("DataProcessingService closed")

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
