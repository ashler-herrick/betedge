"""
Service layer for data processing operations.

This module provides a unified approach to processing all data request types
using async background jobs with progress tracking.
"""

import asyncio
import logging
from typing import Optional, Set
from uuid import UUID

from betedge_data.manager.external_models import ExternalBaseRequest
from betedge_data.manager.job_tracker import JobTracker, JobInfo, JobStatus
from betedge_data.storage.publisher import MinIOPublisher
from betedge_data.common.exceptions import NoDataAvailableError

logger = logging.getLogger(__name__)


class DataProcessingService:
    """Service for processing data requests using async background jobs."""

    def __init__(self, force_refresh: bool = False):
        """
        Initialize the data processing service.

        Args:
            force_refresh: If True, reprocess existing files (overwrite)
        """
        self.force_refresh = force_refresh
        self.publisher = MinIOPublisher()
        self.job_tracker = JobTracker()
        self._background_tasks: Set[asyncio.Task] = set()
        logger.info("DataProcessingService initialized.")

    async def process_request(self, request: ExternalBaseRequest, request_id: UUID) -> None:
        """
        Create background job for processing request.

        Args:
            request: Any External*Request instance
            request_id: Unique request identifier
        """
        logger.info(f"Creating background job for request {request_id}")

        # Create job entry
        individual_requests = request.get_subrequests()
        self.job_tracker.create_job(request_id, len(individual_requests))

        # Start background processing
        task = asyncio.create_task(self._process_request_background(request, request_id))
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

        logger.info(f"Background job {request_id} started with {len(individual_requests)} items")

    async def _process_request_background(self, request: ExternalBaseRequest, request_id: UUID) -> None:
        """
        Background processing with progress tracking.

        Args:
            request: Any External*Request instance
            request_id: Unique request identifier
        """
        try:
            self.job_tracker.update_status(request_id, JobStatus.RUNNING)
            individual_requests = request.get_subrequests()
            client = request.get_client()

            completed = 0
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
                        completed += 1
                        self.job_tracker.update_progress(request_id, completed)
                        continue

                    # Use unified client API - all clients have get_data() method
                    result = client.get_data(req)

                    # Publish the data to MinIO
                    await self.publisher.publish(result, object_key)

                    completed += 1
                    self.job_tracker.update_progress(request_id, completed)

                    logger.info(f"Processed {completed}/{len(individual_requests)} items for job {request_id}")

                except NoDataAvailableError:
                    logger.info(
                        f"No data available for {getattr(req, 'root', 'unknown')} "
                        f"on {getattr(req, 'date', 'unknown date')} - skipping file creation"
                    )
                    completed += 1
                    self.job_tracker.update_progress(request_id, completed)
                    continue
                except Exception as e:
                    logger.error(f"Failed to process item in job {request_id}: {e}")
                    completed += 1
                    self.job_tracker.update_progress(request_id, completed)
                    continue

            self.job_tracker.mark_completed(request_id)
            logger.info(f"Background job {request_id} completed successfully")

        except Exception as e:
            self.job_tracker.mark_failed(request_id, str(e))
            logger.error(f"Background job {request_id} failed: {e}")

    def get_job_status(self, job_id: UUID) -> Optional[JobInfo]:
        """
        Get job status for API endpoint.

        Args:
            job_id: Job identifier

        Returns:
            JobInfo object if job exists, None otherwise
        """
        return self.job_tracker.get_job(job_id)

    async def close(self) -> None:
        """Close service connections and clean up resources."""
        # Cancel all background tasks
        if self._background_tasks:
            logger.info(f"Cancelling {len(self._background_tasks)} background tasks")
            for task in self._background_tasks.copy():
                task.cancel()

            # Wait for tasks to complete cancellation
            if self._background_tasks:
                await asyncio.gather(*self._background_tasks, return_exceptions=True)

        await self.publisher.close()
        logger.info("DataProcessingService closed")

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
