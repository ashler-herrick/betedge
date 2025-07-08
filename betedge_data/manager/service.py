"""
Service layer for data processing operations.

This module provides a unified approach to processing all data request types
using the generate_requests() and get_client() pattern.
"""

import logging
from uuid import UUID

from betedge_data.manager.models import ExternalBaseRequest
from betedge_data.storage.publisher import MinIOPublisher

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

        # Generate individual requests using the request's method
        individual_requests = request.generate_requests()
        logger.info(f"Generated {len(individual_requests)} individual requests")

        # Get the appropriate client using the request's method
        with request.get_client() as client:
            for req in individual_requests:
                try:
                    # Use unified client API - all clients have get_data() method
                    result = client.get_data(req)
                    
                    # Publish the data to MinIO
                    object_key = req.generate_object_key()
                    await self.publisher.publish(result, object_key)
                    
                    logger.info(f"Successfully processed and published request for {getattr(req, 'root', 'unknown')}")
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
