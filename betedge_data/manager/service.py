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
from betedge_data.kafka import KafkaConfig, KafkaPublisher, KafkaPublishConfig
from betedge_data.manager.models import (
    DataProcessingResponse,
    ErrorResponse,
    HistoricalOptionRequest,
    generate_topic_name,
    generate_date_list,
)

logger = logging.getLogger(__name__)


class DataProcessingService:
    """Service for processing historical option data requests and publishing to Kafka."""
    
    def __init__(
        self
    ):
        """
        Initialize the data processing service.
        """
        self.kafka_publisher = KafkaPublisher(KafkaConfig())
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
            # Generate topic name based on root and interval
            topic = generate_topic_name(request.root, request.interval)
            
            # Generate list of dates to process
            dates = generate_date_list(request.start_date, request.end_date)
            logger.info(f"Processing {len(dates)} days from {request.start_date} to {request.end_date}")
            
            total_bytes_published = 0
            days_processed = 0
            
            # Process with HistoricalOptionClient
            with HistoricalOptionClient() as client:
                # Phase 1: Fetch all days in parallel
                fetch_start = time.time()
                logger.info(f"Starting parallel fetch of {len(dates)} days for {request.root}")
                
                day_results = await self._parallel_fetch_days(dates, request, client)
                
                fetch_time = time.time() - fetch_start
                logger.info(f"Parallel fetch completed in {fetch_time:.2f}s - fetched {len(day_results)}/{len(dates)} days")
                
                # Phase 2: Publish in chronological order to maintain time series
                publish_start = time.time()
                for date in dates:
                    if date in day_results:
                        parquet_bytes, data_size = day_results[date]
                        published_size = await self._publish_day_result(
                            parquet_bytes, data_size, date, topic, request_id, request
                        )
                        
                        if published_size > 0:
                            total_bytes_published += published_size
                            days_processed += 1
                    else:
                        logger.warning(f"No data available for {request.root} on {date} - skipping")
                
                publish_time = time.time() - publish_start
                logger.info(f"Sequential publish completed in {publish_time:.2f}s - published {days_processed} days")
            
            processing_time = int((time.time() - start_time) * 1000)
            
            if days_processed == 0:
                logger.error(f"Complete failure: no days processed for {request.root} across {len(dates)} dates ({dates})")
                return DataProcessingResponse(
                    status="error",
                    request_id=request_id,
                    message=f"Failed to process any days for {request.root} across {len(dates)} dates",
                    processing_time_ms=processing_time
                )
            
            return DataProcessingResponse(
                status="success",
                request_id=request_id,
                message=f"Successfully processed {days_processed}/{len(dates)} days of option data for {request.root}",
                data_type="parquet",
                kafka_topic=topic,
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

    async def _publish_day_result(
        self,
        parquet_bytes: bytes,
        data_size: int,
        date: str,
        topic: str,
        request_id: UUID,
        request: HistoricalOptionRequest
    ) -> int:
        """
        Publish single day's result to Kafka.
        
        Args:
            parquet_bytes: Parquet data as bytes
            data_size: Size of the data
            date: Date string
            topic: Kafka topic name
            request_id: Request UUID
            request: Original request
            
        Returns:
            Bytes published (0 on failure)
        """
        try:
            # Convert bytes back to BytesIO for publishing
            import io
            parquet_buffer = io.BytesIO(parquet_bytes)
            
            # Set up Kafka publishing config for this day
            publish_config = KafkaPublishConfig(
                topic=topic,
                key=f"{request.root}_{date}",  # Use root_date as key for ordering
                headers={
                    "request_id": str(request_id),
                    "data_type": "parquet",
                    "root": request.root,
                    "date": date,
                    "interval": str(request.interval)
                }
            )
            
            # Publish Parquet data to Kafka
            published_size = await self.kafka_publisher.publish_parquet_data(
                parquet_buffer, publish_config
            )
            
            if published_size > 0:
                logger.info(f"Successfully processed {request.root} on {date}: published {published_size} bytes to topic {topic}")
            
            return published_size
            
        except Exception as e:
            logger.error(f"Failed to publish {request.root} for date {date}: {str(e)}")
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
        await self.kafka_publisher.close()
        logger.info("DataProcessingService closed")
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()