"""
Service layer for data processing operations.

This module orchestrates calls to the historical option client and handles
the business logic for data processing requests.
"""


import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Optional, Tuple
from uuid import UUID

from betedge_data.historical.option.client import HistoricalOptionClient
from betedge_data.historical.option.models import HistoricalOptionRequest as ClientHistoricalOptionRequest
from betedge_data.storage import MinIOConfig, MinIOPublisher, MinIOPublishConfig
from betedge_data.manager.models import (
    DataProcessingResponse,
    ErrorResponse,
    HistoricalOptionRequest,
    generate_date_list,
)

logger = logging.getLogger(__name__)


class DataProcessingService:
    """Service for processing historical option data requests and publishing to MinIO object storage."""
    
    def __init__(
        self,
        force_refresh: bool = False,
        minio_config: Optional[MinIOConfig] = None
    ):
        """
        Initialize the data processing service.
        
        Args:
            force_refresh: If True, reprocess existing files (overwrite)
            minio_config: MinIO configuration, uses default if None
        """
        self.force_refresh = force_refresh
        
        # Initialize MinIO publisher
        try:
            self.minio_publisher = MinIOPublisher(minio_config or MinIOConfig())
            logger.info(f"MinIOPublisher initialized (force_refresh={force_refresh})")
        except Exception as e:
            logger.error(f"Failed to initialize MinIO publisher: {e}")
            raise RuntimeError(f"MinIO initialization failed: {e}") from e
        
        logger.info("DataProcessingService initialized")
    
    async def process_historical_option_request(
        self, 
        request: HistoricalOptionRequest,
        request_id: UUID
    ) -> DataProcessingResponse:
        """
        Process historical option data request with date range support.
        
        Splits date ranges into individual days and processes each separately
        to work with the underlying client's single-day limitation.
        
        Args:
            request: Historical option data request
            request_id: Unique request identifier
            
        Returns:
            Processing response with metadata
        """
        start_time = time.time()
        logger.info(f"Processing historical option request {request_id} for {request.root}")
        
        try:
            # Generate list of dates to process
            dates = generate_date_list(request.start_date, request.end_date)
            logger.info(f"Processing {len(dates)} days from {request.start_date} to {request.end_date}")
            
            total_bytes_published = 0
            days_processed = 0
            
            # Process with HistoricalOptionClient
            with HistoricalOptionClient() as client:
                # Phase 1: Filter out already published dates (unless force refresh)
                dates_to_fetch = self._filter_unpublished_dates(dates, request)
                
                if len(dates_to_fetch) == 0:
                    logger.info(f"All {len(dates)} days already exist for {request.root} (use force_refresh=True to reprocess)")
                    storage_location = f"s3://{self.minio_publisher.bucket}/historical-options/quote/{request.root}/"
                    return DataProcessingResponse(
                        status="success",
                        request_id=request_id,
                        message=f"All {len(dates)} days already exist for {request.root}",
                        data_type="parquet",
                        storage_location=storage_location,
                        records_count=0,
                        processing_time_ms=int((time.time() - start_time) * 1000)
                    )
                
                # Phase 2: Fetch unpublished days in parallel
                fetch_start = time.time()
                logger.info(f"Starting parallel fetch of {len(dates_to_fetch)}/{len(dates)} days for {request.root}")
                
                day_results = await self._parallel_fetch_days(dates_to_fetch, request, client)
                
                fetch_time = time.time() - fetch_start
                logger.info(f"Parallel fetch completed in {fetch_time:.2f}s - fetched {len(day_results)}/{len(dates_to_fetch)} days")
                
                # Phase 3: Upload to MinIO (can be done in parallel since no ordering constraints)
                upload_start = time.time()
                upload_results = await self._parallel_upload_days(day_results, request, request_id)
                
                # Count successful uploads
                for date, upload_size in upload_results.items():
                    if upload_size > 0:
                        total_bytes_published += upload_size
                        days_processed += 1
                
                upload_time = time.time() - upload_start
                logger.info(f"Parallel upload completed in {upload_time:.2f}s - uploaded {days_processed} days")
            
            processing_time = int((time.time() - start_time) * 1000)
            
            if days_processed == 0:
                logger.error(f"Complete failure: no days processed for {request.root} across {len(dates)} dates ({dates})")
                return DataProcessingResponse(
                    status="error",
                    request_id=request_id,
                    message=f"Failed to process any days for {request.root} across {len(dates)} dates",
                    processing_time_ms=processing_time
                )
            
            # Generate storage location info
            storage_location = f"s3://{self.minio_publisher.bucket}/historical-options/quote/{request.root}/"
            
            return DataProcessingResponse(
                status="success",
                request_id=request_id,
                message=f"Successfully processed {days_processed}/{len(dates)} days of option data for {request.root}",
                data_type="parquet",
                storage_location=storage_location,
                records_count=total_bytes_published,  # Total bytes across all days
                processing_time_ms=processing_time
            )
            
        except Exception as e:
            logger.error(f"Error processing historical option request {request_id}: {str(e)}")
            logger.debug(f"Full error details for request {request_id}", exc_info=True)
            processing_time = int((time.time() - start_time) * 1000)
            
            return DataProcessingResponse(
                status="error",
                request_id=request_id,
                message=f"Failed to process historical option request: {str(e)}",
                processing_time_ms=processing_time
            )
    
    def _filter_unpublished_dates(
        self,
        dates: list[str],
        request: HistoricalOptionRequest
    ) -> list[str]:
        """
        Filter out dates that already have files in MinIO (unless force refresh).
        
        Args:
            dates: List of dates to check
            request: Historical option request
            
        Returns:
            List of dates that need to be fetched
        """
        # If force refresh, return all dates to reprocess
        if self.force_refresh:
            logger.info(f"Force refresh enabled - will reprocess all {len(dates)} dates")
            return dates
        
        # Check MinIO for existing files
        existing_dates = self.minio_publisher.list_existing_files(
            root=request.root,
            dates=dates,
            interval=request.interval,
            exp=str(request.exp),
            schema="quote",  # Default to quote schema
            filter_type="filtered"
        )
        
        unpublished_dates = [date for date in dates if date not in existing_dates]
        
        if existing_dates:
            logger.info(f"Skipping {len(existing_dates)} existing files for {request.root}: {sorted(existing_dates)}")
        
        return unpublished_dates
    
    def _fetch_single_day(
        self,
        date: str,
        request: HistoricalOptionRequest,
        client: HistoricalOptionClient
    ) -> Tuple[Optional[bytes], int]:
        """
        Fetch and process single day's data.
        
        Args:
            date: Date string in YYYYMMDD format
            request: Historical option request
            client: Historical option client instance
            
        Returns:
            Tuple of (parquet_buffer, data_size) or (None, 0) on error
        """
        try:
            # Create single-day request for the client
            client_request = ClientHistoricalOptionRequest(
                root=request.root,
                start_date=date,
                end_date=date,  # Single day
                exp=request.exp,
                max_dte=request.max_dte,
                base_pct=request.base_pct,
                interval=request.interval,
                start_time=request.start_time,
                end_time=request.end_time,
                use_csv=False,
                return_format="parquet"
            )
            
            # Fetch filtered data as parquet
            logger.debug(f"Fetching data for {request.root} on {date} with params: exp={request.exp}, max_dte={request.max_dte}, base_pct={request.base_pct}")
            parquet_buffer = client.get_filtered_bulk_quote_as_parquet(client_request)
            
            # Get the buffer as bytes for storage
            parquet_buffer.seek(0)
            parquet_bytes = parquet_buffer.read()
            data_size = len(parquet_bytes)
            
            return parquet_bytes, data_size
            
        except Exception as e:
            logger.error(f"Failed to fetch data for {request.root} on date {date}: {str(e)}")
            logger.debug(f"Full error details for {request.root} on {date}", exc_info=True)
            return None, 0

    async def _parallel_fetch_days(
        self,
        dates: list[str],
        request: HistoricalOptionRequest,
        client: HistoricalOptionClient
    ) -> Dict[str, Tuple[bytes, int]]:
        """
        Fetch all days in parallel, return dict keyed by date.
        
        Args:
            dates: List of date strings to process
            request: Historical option request
            client: Historical option client instance
            
        Returns:
            Dict mapping date to (parquet_bytes, data_size)
        """
        results = {}
        
        # Use ThreadPoolExecutor for parallel HTTP requests
        with ThreadPoolExecutor(max_workers=min(30, len(dates))) as executor:
            # Submit all fetch tasks
            future_to_date = {
                executor.submit(self._fetch_single_day, date, request, client): date
                for date in dates
            }
            
            # Collect results as they complete
            for future in future_to_date:
                date = future_to_date[future]
                try:
                    parquet_bytes, data_size = future.result()
                    if parquet_bytes is not None:
                        results[date] = (parquet_bytes, data_size)
                        logger.info(f"Fetched {data_size} bytes for {request.root} on {date}")
                except Exception as e:
                    logger.error(f"Parallel fetch failed for {request.root} on {date}: {str(e)}")
                    
        return results

    async def _parallel_upload_days(
        self,
        day_results: Dict[str, Tuple[bytes, int]],
        request: HistoricalOptionRequest,
        request_id: UUID
    ) -> Dict[str, int]:
        """
        Upload all day results to MinIO in parallel.
        
        Args:
            day_results: Dict mapping date to (parquet_bytes, data_size)
            request: Historical option request
            request_id: Request UUID
            
        Returns:
            Dict mapping date to uploaded bytes (0 on failure)
        """
        upload_results = {}
        
        # Use ThreadPoolExecutor for parallel uploads
        with ThreadPoolExecutor(max_workers=min(10, len(day_results))) as executor:
            # Submit all upload tasks
            future_to_date = {}
            for date, (parquet_bytes, data_size) in day_results.items():
                future = executor.submit(
                    self._upload_single_day, parquet_bytes, date, request, request_id
                )
                future_to_date[future] = date
            
            # Collect results as they complete
            for future in future_to_date:
                date = future_to_date[future]
                try:
                    upload_size = future.result()
                    upload_results[date] = upload_size
                    if upload_size > 0:
                        logger.info(f"Uploaded {upload_size} bytes for {request.root} on {date}")
                except Exception as e:
                    logger.error(f"Parallel upload failed for {request.root} on {date}: {str(e)}")
                    upload_results[date] = 0
        
        return upload_results

    def _upload_single_day(
        self,
        parquet_bytes: bytes,
        date: str,
        request: HistoricalOptionRequest,
        request_id: UUID
    ) -> int:
        """
        Upload single day's result to MinIO.
        
        Args:
            parquet_bytes: Parquet data as bytes
            date: Date string
            request: Original request
            request_id: Request UUID
            
        Returns:
            Bytes uploaded (0 on failure)
        """
        try:
            # Convert bytes to BytesIO for uploading
            import io
            parquet_buffer = io.BytesIO(parquet_bytes)
            
            # Set up MinIO publishing config for this day
            publish_config = MinIOPublishConfig(
                schema="quote",  # Default to quote schema
                root=request.root,
                date=date,
                interval=request.interval,
                exp=str(request.exp),
                filter_type="filtered"
            )
            
            # Upload Parquet data to MinIO (this is synchronous but called from ThreadPoolExecutor)
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                upload_size = loop.run_until_complete(
                    self.minio_publisher.publish_parquet_data(parquet_buffer, publish_config)
                )
            finally:
                loop.close()
            
            if upload_size > 0:
                object_path = publish_config.generate_object_path()
                logger.info(f"Successfully uploaded {request.root} on {date}: {upload_size} bytes to {object_path}")
            
            return upload_size
            
        except Exception as e:
            logger.error(f"Failed to upload {request.root} for date {date}: {str(e)}")
            return 0

    def create_error_response(
        self, 
        request_id: UUID,
        error_code: str,
        message: str,
        details: Optional[str] = None
    ) -> ErrorResponse:
        """
        Create a standardized error response.
        
        Args:
            request_id: Request identifier
            error_code: Machine-readable error code
            message: Human-readable error message
            details: Additional error details
            
        Returns:
            Error response object
        """
        return ErrorResponse(
            request_id=request_id,
            error_code=error_code,
            message=message,
            details=details
        )
    
    async def close(self) -> None:
        """Close service connections and clean up resources."""
        await self.minio_publisher.close()
        logger.info("DataProcessingService closed")
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()