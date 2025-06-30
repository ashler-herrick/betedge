"""
MinIO publisher for object storage operations.
"""

import io
import logging
from typing import List, Set, Dict, Any

from minio import Minio
from minio.error import S3Error

from .config import MinIOConfig, MinIOPublishConfig

logger = logging.getLogger(__name__)


class MinIOPublisher:
    """Publisher for uploading data to MinIO S3-compatible object storage."""
    
    def __init__(self, config: MinIOConfig):
        """
        Initialize MinIO publisher.
        
        Args:
            config: MinIO configuration
        """
        self.config = config
        self.bucket = config.bucket
        
        # Initialize MinIO client
        self.client = Minio(
            endpoint=config.endpoint,
            access_key=config.access_key,
            secret_key=config.secret_key,
            secure=config.secure,
            region=config.region
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
    
    async def publish_parquet_data(
        self,
        parquet_buffer: io.BytesIO,
        config: MinIOPublishConfig
    ) -> int:
        """
        Publish Parquet data to MinIO.
        
        Args:
            parquet_buffer: BytesIO containing Parquet data
            config: Publishing configuration
            
        Returns:
            Number of bytes uploaded
        """
        try:
            # Generate object path
            object_path = config.generate_object_path()
            
            # Get buffer size
            parquet_buffer.seek(0, io.SEEK_END)
            data_size = parquet_buffer.tell()
            parquet_buffer.seek(0)
            
            logger.debug(f"Uploading {data_size} bytes to {object_path}")
            
            # Upload to MinIO
            result = self.client.put_object(
                bucket_name=self.bucket,
                object_name=object_path,
                data=parquet_buffer,
                length=data_size,
                content_type="application/octet-stream",
                metadata={
                    "schema": config.schema,
                    "root": config.root,
                    "date": config.date,
                    "interval": str(config.interval),
                    "exp": config.exp,
                    "filter_type": config.filter_type
                }
            )
            
            logger.info(f"Successfully uploaded {data_size} bytes to {object_path} (etag: {result.etag})")
            return data_size
            
        except S3Error as e:
            logger.error(f"Failed to upload to MinIO: {e}")
            raise RuntimeError(f"MinIO upload failed: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error during upload: {e}")
            raise
    
    def list_existing_files(
        self,
        root: str,
        dates: List[str],
        interval: int,
        exp: str,
        schema: str = "quote",
        filter_type: str = "filtered"
    ) -> Set[str]:
        """
        List existing files for given parameters and return set of dates that exist.
        
        Args:
            root: Option root symbol
            dates: List of dates to check
            interval: Interval in milliseconds
            exp: Expiration string
            schema: Data schema type (e.g., 'quote', 'ohlc', 'trade')
            filter_type: Filter type applied to data
            
        Returns:
            Set of dates that already have files
        """
        existing_dates = set()
        
        try:
            for date in dates:
                # Create config to generate object path
                config = MinIOPublishConfig(
                    schema=schema,
                    root=root,
                    date=date,
                    interval=interval,
                    exp=exp,
                    filter_type=filter_type
                )
                
                object_path = config.generate_object_path()
                
                # Check if object exists
                try:
                    self.client.stat_object(self.bucket, object_path)
                    existing_dates.add(date)
                    logger.debug(f"Found existing file: {object_path}")
                except S3Error as e:
                    if e.code == "NoSuchKey":
                        logger.debug(f"File does not exist: {object_path}")
                    else:
                        logger.warning(f"Error checking file {object_path}: {e}")
            
            if existing_dates:
                logger.info(f"Found {len(existing_dates)} existing files for {root}: {sorted(existing_dates)}")
            
            return existing_dates
            
        except Exception as e:
            logger.error(f"Error listing existing files: {e}")
            # Return empty set on error to allow processing to continue
            return set()
    
    def list_files_for_symbol(
        self,
        root: str,
        schema: str = None,
        start_date: str = None,
        end_date: str = None,
        limit: int = None
    ) -> List[Dict[str, Any]]:
        """
        List files for a symbol with optional schema and date filtering.
        
        Args:
            root: Option root symbol
            schema: Optional schema filter (e.g., 'quote', 'ohlc', 'trade')
            start_date: Optional start date filter (YYYYMMDD)
            end_date: Optional end date filter (YYYYMMDD)
            limit: Optional limit on number of results
            
        Returns:
            List of file information dictionaries
        """
        try:
            # Build prefix based on schema filter
            if schema:
                prefix = f"historical-options/{schema}/{root}/"
            else:
                prefix = f"historical-options/"
            
            objects = self.client.list_objects(
                bucket_name=self.bucket,
                prefix=prefix,
                recursive=True
            )
            
            files = []
            for obj in objects:
                # Parse path to extract metadata
                if obj.object_name.endswith('/data.parquet'):
                    try:
                        # Parse path: historical-options/{schema}/{root}/{year}/{month}/{day}/{interval}/{exp}/{filter}/data.parquet
                        path_parts = obj.object_name.split('/')
                        if len(path_parts) >= 9 and path_parts[0] == "historical-options":
                            # Extract date from year/month/day parts
                            year, month, day = path_parts[3], path_parts[4], path_parts[5]
                            date_str = f"{year}{month.zfill(2)}{day.zfill(2)}"
                            
                            file_info = {
                                "object_name": obj.object_name,
                                "size": obj.size,
                                "last_modified": obj.last_modified,
                                "schema": path_parts[1],
                                "root": path_parts[2],
                                "date": date_str,
                                "year": int(year),
                                "month": int(month),
                                "day": int(day),
                                "interval_str": path_parts[6],
                                "exp": path_parts[7],
                                "filter_type": path_parts[8]
                            }
                            
                            # Apply filters
                            if root and file_info["root"] != root:
                                continue
                            if start_date and file_info["date"] < start_date:
                                continue
                            if end_date and file_info["date"] > end_date:
                                continue
                            
                            files.append(file_info)
                    except (ValueError, IndexError) as e:
                        logger.warning(f"Could not parse path {obj.object_name}: {e}")
                
                # Apply limit
                if limit and len(files) >= limit:
                    break
            
            # Sort by date
            files.sort(key=lambda x: x["date"])
            
            logger.info(f"Found {len(files)} files for {root}")
            return files
            
        except S3Error as e:
            logger.error(f"Error listing files for {root}: {e}")
            return []
    
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
    
    def get_bucket_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the bucket.
        
        Returns:
            Dictionary with bucket statistics
        """
        try:
            objects = self.client.list_objects(
                bucket_name=self.bucket,
                prefix="historical-options/",
                recursive=True
            )
            
            total_files = 0
            total_size = 0
            symbols = set()
            
            for obj in objects:
                total_files += 1
                total_size += obj.size
                
                # Extract symbol from path: historical-options/{schema}/{symbol}/...
                path_parts = obj.object_name.split('/')
                if len(path_parts) >= 3:
                    symbols.add(path_parts[2])  # historical-options/{schema}/{symbol}/...
            
            return {
                "bucket": self.bucket,
                "total_files": total_files,
                "total_size_bytes": total_size,
                "total_size_mb": round(total_size / (1024 * 1024), 2),
                "unique_symbols": len(symbols),
                "symbols": sorted(symbols)
            }
            
        except S3Error as e:
            logger.error(f"Error getting bucket stats: {e}")
            return {"error": str(e)}
    
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