"""
MinIO publisher for object storage operations.
"""

import io
import logging
from typing import Set

from minio import Minio
from minio.error import S3Error

from betedge_data.storage.config import MinIOConfig
from betedge_data.manager.models import ExternalBaseRequest

logger = logging.getLogger(__name__)


class MinIOPublisher:
    """Publisher for uploading data to MinIO S3-compatible object storage."""

    def __init__(self):
        """
        Initialize MinIO publisher.

        Args:
            config: MinIO configuration
        """
        config = MinIOConfig()
        self.config = config
        self.bucket = config.bucket

        # Initialize MinIO client
        self.client = Minio(
            endpoint=config.endpoint,
            access_key=config.access_key,
            secret_key=config.secret_key,
            secure=config.secure,
            region=config.region,
        )

        # Ensure bucket exists
        self._ensure_bucket_exists()

        logger.info(f"MinIOPublisher initialized for bucket: {self.bucket}")

    def _ensure_bucket_exists(self):
        """Ensure the configured bucket exists, create if not."""
        try:
            if not self.client.bucket_exists(self.bucket):
                logger.info(f"Creating bucket: {self.bucket}")
                self.client.make_bucket(self.bucket, location=self.config.region)
                logger.info(f"Bucket {self.bucket} created successfully")
            else:
                logger.debug(f"Bucket {self.bucket} already exists")
        except S3Error as e:
            logger.error(f"Failed to ensure bucket exists: {e}")
            raise RuntimeError(f"MinIO bucket setup failed: {e}") from e

    async def publish(self, data: io.BytesIO, object_key: str) -> None:
        """
        Publish Parquet data to MinIO with explicit object key.

        Args:
            data: BytesIO containing Parquet data
            object_key: Direct object key/path for storage

        """
        try:
            # Get buffer size
            data.seek(0, io.SEEK_END)
            data_size = data.tell()
            data.seek(0)

            logger.debug(f"Uploading {data_size} bytes to {object_key}")

            # Upload to MinIO
            result = self.client.put_object(
                bucket_name=self.bucket,
                object_name=object_key,
                data=data,
                length=data_size,
                content_type="application/octet-stream",
            )

            logger.info(f"Successfully uploaded {data_size} bytes to {object_key} (etag: {result.etag})")

        except Exception as e:
            logger.error(f"Failed to upload parquet data to {object_key}: {e}", exc_info=True)

    def list_existing_files(
        self,
        request: ExternalBaseRequest,
    ) -> Set[str]:
        """
        List existing files for given parameters and return set of dates that exist.

        Args:
            request: Subclass of ExternalBaseRequest

        Returns:
            Set of dates that already have files
        """

    def delete_file(self, object_path: str) -> bool:
        """
        Delete a file from MinIO.

        Args:
            object_path: Full object path to delete

        Returns:
            True if deleted successfully
        """
        try:
            self.client.remove_object(self.bucket, object_path)
            logger.info(f"Deleted file: {object_path}")
            return True
        except S3Error as e:
            logger.error(f"Failed to delete {object_path}: {e}")
            return False

    async def close(self):
        """Close the MinIO publisher (no-op for MinIO client)."""
        logger.info("MinIOPublisher closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        # MinIO client doesn't need explicit closing
        pass
