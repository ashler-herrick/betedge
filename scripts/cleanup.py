#!/usr/bin/env python3
"""
Script to delete all files with /historical-options/ prefix from MinIO bucket.
"""

import logging
from typing import List
from minio import Minio
from minio.error import S3Error
from pydantic import Field
from pydantic_settings import BaseSettings


class MinIOConfig(BaseSettings):
    """Configuration for MinIO S3-compatible object storage."""

    endpoint: str = Field(default="minio:9000", description="MinIO server endpoint")
    access_key: str = Field(default="minioadmin", description="MinIO access key")
    secret_key: str = Field(default="minioadmin123", description="MinIO secret key")
    bucket: str = Field(
        default="betedge-data",
        description="MinIO bucket name for data storage",
    )
    secure: bool = Field(
        default=False,
        description="Use HTTPS for MinIO connections",
    )
    region: str = Field(default="us-east-1", description="MinIO region")

    model_config = {
        "env_prefix": "MINIO_",
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "case_sensitive": False,
        "extra": "ignore",
    }


class MinIOCleaner:
    """Class to handle MinIO cleanup operations."""

    def __init__(self, config: MinIOConfig):
        self.config = config
        self.client = Minio(
            endpoint=config.endpoint,
            access_key=config.access_key,
            secret_key=config.secret_key,
            secure=config.secure,
            region=config.region,
        )

        # Set up logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

    def verify_bucket_exists(self) -> bool:
        """Verify that the specified bucket exists."""
        try:
            exists = self.client.bucket_exists(self.config.bucket)
            if not exists:
                self.logger.error(f"Bucket '{self.config.bucket}' does not exist")
                return False
            return True
        except S3Error as e:
            self.logger.error(f"Error checking bucket existence: {e}")
            return False

    def list_objects_with_prefix(self, prefix: str) -> List[str]:
        """List all objects in the bucket with the specified prefix."""
        try:
            objects = []
            object_iterator = self.client.list_objects(
                self.config.bucket, prefix=prefix, recursive=True
            )

            for obj in object_iterator:
                objects.append(obj.object_name)
                self.logger.info(f"Found object: {obj.object_name}")

            return objects
        except S3Error as e:
            self.logger.error(f"Error listing objects: {e}")
            return []

    def delete_objects_batch(
        self, object_names: List[str], batch_size: int = 1000
    ) -> bool:
        """Delete objects in batches for better performance."""
        if not object_names:
            self.logger.info("No objects to delete")
            return True

        total_objects = len(object_names)
        deleted_count = 0

        try:
            # Process objects in batches
            for i in range(0, total_objects, batch_size):
                batch = object_names[i : i + batch_size]

                # MinIO client expects a list of DeleteObject instances for batch deletion
                from minio.deleteobjects import DeleteObject

                delete_object_list = [DeleteObject(name) for name in batch]

                # Perform batch deletion
                errors = self.client.remove_objects(
                    self.config.bucket, delete_object_list
                )

                # Check for errors
                error_occurred = False
                for error in errors:
                    self.logger.error(f"Error deleting {error.object_name}: {error}")
                    error_occurred = True

                if not error_occurred:
                    deleted_count += len(batch)
                    self.logger.info(
                        f"Successfully deleted batch of {len(batch)} objects"
                    )
                    self.logger.info(
                        f"Progress: {deleted_count}/{total_objects} objects deleted"
                    )

            if deleted_count == total_objects:
                self.logger.info(f"Successfully deleted all {deleted_count} objects")
                return True
            else:
                self.logger.warning(f"Deleted {deleted_count}/{total_objects} objects")
                return False

        except S3Error as e:
            self.logger.error(f"Error during batch deletion: {e}")
            return False

    def delete_objects_individual(self, object_names: List[str]) -> bool:
        """Delete objects individually (slower but more reliable for error handling)."""
        if not object_names:
            self.logger.info("No objects to delete")
            return True

        total_objects = len(object_names)
        deleted_count = 0
        failed_objects = []

        for i, object_name in enumerate(object_names, 1):
            try:
                self.client.remove_object(self.config.bucket, object_name)
                deleted_count += 1
                self.logger.info(f"Deleted ({i}/{total_objects}): {object_name}")
            except S3Error as e:
                failed_objects.append(object_name)
                self.logger.error(f"Failed to delete {object_name}: {e}")

        self.logger.info(
            f"Deletion complete: {deleted_count} successful, {len(failed_objects)} failed"
        )

        if failed_objects:
            self.logger.error("Failed to delete the following objects:")
            for obj in failed_objects:
                self.logger.error(f"  - {obj}")

        return len(failed_objects) == 0

    def cleanup_historical_options(
        self,
        prefix: str = "historical-options/",
        dry_run: bool = False,
        use_batch_delete: bool = True,
    ) -> bool:
        """
        Main method to cleanup historical options files.

        Args:
            prefix: The prefix to search for (default: "historical-options/")
            dry_run: If True, only list objects without deleting them
            use_batch_delete: If True, use batch deletion (faster), otherwise delete individually
        """
        self.logger.info(f"Starting cleanup process for prefix: {prefix}")
        self.logger.info(f"Dry run mode: {dry_run}")
        self.logger.info(f"Batch delete mode: {use_batch_delete}")

        # Verify bucket exists
        if not self.verify_bucket_exists():
            return False

        # List objects with the specified prefix
        self.logger.info("Listing objects to delete...")
        objects_to_delete = self.list_objects_with_prefix(prefix)

        if not objects_to_delete:
            self.logger.info(f"No objects found with prefix '{prefix}'")
            return True

        self.logger.info(f"Found {len(objects_to_delete)} objects to delete")

        if dry_run:
            self.logger.info("DRY RUN - Objects that would be deleted:")
            for obj in objects_to_delete:
                self.logger.info(f"  - {obj}")
            return True

        # Confirm deletion
        self.logger.warning(f"About to delete {len(objects_to_delete)} objects!")

        # Perform deletion
        if use_batch_delete:
            success = self.delete_objects_batch(objects_to_delete)
        else:
            success = self.delete_objects_individual(objects_to_delete)

        if success:
            self.logger.info("Cleanup completed successfully")
        else:
            self.logger.error("Cleanup completed with errors")

        return success


def main():
    """Main function to run the cleanup script."""
    # Load configuration
    config = MinIOConfig()

    # Create cleaner instance
    cleaner = MinIOCleaner(config)

    # Configuration options
    PREFIX = "historical-options/"  # Change this if you need a different prefix
    DRY_RUN = False  # Set to False to actually delete files
    USE_BATCH_DELETE = (
        True  # Set to False for individual deletion (slower but more reliable)
    )

    print("MinIO Cleanup Script")
    print(f"Endpoint: {config.endpoint}")
    print(f"Bucket: {config.bucket}")
    print(f"Prefix: {PREFIX}")
    print(f"Dry Run: {DRY_RUN}")
    print("-" * 50)

    # Run cleanup
    success = cleaner.cleanup_historical_options(
        prefix=PREFIX, dry_run=DRY_RUN, use_batch_delete=USE_BATCH_DELETE
    )

    if success:
        print("Script completed successfully")
    else:
        print("Script completed with errors")


if __name__ == "__main__":
    main()
