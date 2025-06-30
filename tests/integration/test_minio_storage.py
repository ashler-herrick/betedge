"""
Integration tests for MinIO storage workflows.

These tests require a running MinIO instance.
"""

import io
import logging
from datetime import datetime

import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from betedge_data.storage.config import MinIOConfig, MinIOPublishConfig
from betedge_data.storage.publisher import MinIOPublisher

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)


def handle_minio_error(e):
    """Helper to handle MinIO connection errors by skipping tests."""
    if "AccessDenied" in str(e) or "connection" in str(e).lower() or "NoSuchBucket" in str(e):
        pytest.skip(f"MinIO not available or access denied: {e}")
    else:
        raise


@pytest.fixture
def minio_integration_config():
    """Real MinIO configuration for integration tests."""
    return MinIOConfig(
        endpoint="localhost:9000",
        access_key="minioadmin",
        secret_key="minioadmin",
        bucket="test-integration",
        secure=False,
        region="us-east-1",
    )


@pytest.fixture
def sample_option_data():
    """Generate sample option data as Parquet."""
    data = {
        "ms_of_day": [34200000, 35100000, 36000000],
        "bid_price": [149.75, 148.95, 147.50],
        "ask_price": [149.85, 149.05, 147.60],
        "bid_size": [100, 150, 200],
        "ask_size": [100, 120, 180],
        "root": ["AAPL", "AAPL", "AAPL"],
        "expiration": [20231117, 20231117, 20231117],
        "strike": [1500000, 1500000, 1500000],
        "right": ["C", "C", "C"],
    }

    table = pa.table(data)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression='snappy')
    buffer.seek(0)
    return buffer


def test_minio_connection(minio_integration_config):
    """Test basic MinIO connection and bucket operations."""
    try:
        with MinIOPublisher(minio_integration_config) as publisher:
            # Test bucket exists
            assert publisher.bucket == "test-integration"
            assert publisher.client.bucket_exists(publisher.bucket)
            
            # Test bucket stats (should work even if empty)
            stats = publisher.get_bucket_stats()
            assert "bucket" in stats
            assert stats["bucket"] == "test-integration"
            assert "total_files" in stats
            assert "total_size_bytes" in stats
    except Exception as e:
        handle_minio_error(e)


@pytest.mark.asyncio
async def test_publish_and_retrieve_parquet(minio_integration_config, sample_option_data):
    """Test publishing Parquet data and retrieving it."""
    try:
        date_str = datetime.now().strftime("%Y%m%d")
        
        config = MinIOPublishConfig(
            schema="quote",
            root="AAPL", 
            date=date_str,
            interval=900000,
            exp="0",
            filter_type="filtered"
        )
        
        with MinIOPublisher(minio_integration_config) as publisher:
            # Publish data
            size = await publisher.publish_parquet_data(sample_option_data, config)
            assert size > 0
            logger.info(f"Published {size} bytes to MinIO")
            
            # Verify it exists
            existing_files = publisher.list_existing_files(
                root="AAPL",
                dates=[date_str],
                interval=900000,
                exp="0",
                schema="quote",
                filter_type="filtered"
            )
            assert date_str in existing_files
            
            # List files for symbol
            files = publisher.list_files_for_symbol(root="AAPL", schema="quote")
            assert len(files) >= 1
            
            # Find our file
            our_file = next((f for f in files if f["date"] == date_str), None)
            assert our_file is not None
            assert our_file["schema"] == "quote"
            assert our_file["root"] == "AAPL"
            assert our_file["interval_str"] == "15m"
            assert our_file["exp"] == "all"
            assert our_file["filter_type"] == "filtered"
    except Exception as e:
        handle_minio_error(e)


def test_bucket_stats_with_data(minio_integration_config, sample_option_data):
    """Test bucket statistics after uploading data."""
    try:
        date_str = datetime.now().strftime("%Y%m%d")
        
        config = MinIOPublishConfig(
            schema="quote",
            root="TSLA",  # Use different symbol to avoid conflicts
            date=date_str,
            interval=60000,  # 1 minute
            exp="20231201", 
            filter_type="raw"
        )
        
        with MinIOPublisher(minio_integration_config) as publisher:
            # Get initial stats
            initial_stats = publisher.get_bucket_stats()
            initial_files = initial_stats["total_files"]
            initial_size = initial_stats["total_size_bytes"]
            
            # Publish data
            import asyncio
            size = asyncio.run(publisher.publish_parquet_data(sample_option_data, config))
            
            # Get updated stats
            final_stats = publisher.get_bucket_stats()
            assert final_stats["total_files"] == initial_files + 1
            assert final_stats["total_size_bytes"] == initial_size + size
            assert "TSLA" in final_stats["symbols"]
    except Exception as e:
        handle_minio_error(e)


def test_file_deletion(minio_integration_config, sample_option_data):
    """Test file deletion functionality."""
    try:
        date_str = datetime.now().strftime("%Y%m%d")
        
        config = MinIOPublishConfig(
            schema="trade",
            root="MSFT",
            date=date_str,
            interval=3600000,  # 1 hour
            exp="0",
            filter_type="filtered"
        )
        
        with MinIOPublisher(minio_integration_config) as publisher:
            # Publish data
            import asyncio
            asyncio.run(publisher.publish_parquet_data(sample_option_data, config))
            
            # Verify it exists
            object_path = config.generate_object_path()
            files = publisher.list_files_for_symbol(root="MSFT", schema="trade")
            assert any(f["object_name"] == object_path for f in files)
            
            # Delete it
            success = publisher.delete_file(object_path)
            assert success
            
            # Verify it's gone
            files_after = publisher.list_files_for_symbol(root="MSFT", schema="trade")
            assert not any(f["object_name"] == object_path for f in files_after)
    except Exception as e:
        handle_minio_error(e)


def test_list_files_with_filters(minio_integration_config, sample_option_data):
    """Test listing files with date and schema filters."""
    try:
        base_date = datetime.now()
        dates = [
            base_date.strftime("%Y%m%d"),
            (base_date.replace(day=base_date.day + 1) if base_date.day < 28 else base_date.replace(day=1, month=base_date.month + 1)).strftime("%Y%m%d")
        ]
        
        with MinIOPublisher(minio_integration_config) as publisher:
            # Upload data for multiple dates and schemas
            for i, date_str in enumerate(dates):
                for schema in ["quote", "ohlc"]:
                    config = MinIOPublishConfig(
                        schema=schema,
                        root="NVDA",
                        date=date_str,
                        interval=900000,
                        exp="0",
                        filter_type="filtered"
                    )
                    
                    import asyncio
                    asyncio.run(publisher.publish_parquet_data(sample_option_data, config))
            
            # Test schema filtering
            quote_files = publisher.list_files_for_symbol(root="NVDA", schema="quote")
            ohlc_files = publisher.list_files_for_symbol(root="NVDA", schema="ohlc")
            
            assert len(quote_files) == 2  # 2 dates
            assert len(ohlc_files) == 2   # 2 dates
            assert all(f["schema"] == "quote" for f in quote_files)
            assert all(f["schema"] == "ohlc" for f in ohlc_files)
            
            # Test date filtering
            first_date_files = publisher.list_files_for_symbol(
                root="NVDA", 
                start_date=dates[0], 
                end_date=dates[0]
            )
            assert len(first_date_files) == 2  # quote + ohlc for first date
            assert all(f["date"] == dates[0] for f in first_date_files)
    except Exception as e:
        handle_minio_error(e)


def test_path_metadata_extraction(minio_integration_config, sample_option_data):
    """Test that file paths contain correct metadata."""
    try:
        config = MinIOPublishConfig(
            schema="quote",
            root="AMD",
            date="20231117",
            interval=1800000,  # 30 minutes
            exp="20231201",
            filter_type="raw"
        )
        
        with MinIOPublisher(minio_integration_config) as publisher:
            # Upload file
            import asyncio
            asyncio.run(publisher.publish_parquet_data(sample_option_data, config))
            
            # List and verify metadata
            files = publisher.list_files_for_symbol(root="AMD", schema="quote")
            assert len(files) >= 1
            
            our_file = next(f for f in files if f["date"] == "20231117")
            
            # Verify extracted metadata
            assert our_file["schema"] == "quote"
            assert our_file["root"] == "AMD"
            assert our_file["date"] == "20231117"
            assert our_file["year"] == 2023
            assert our_file["month"] == 11
            assert our_file["day"] == 17
            assert our_file["interval_str"] == "30m"
            assert our_file["exp"] == "20231201"
            assert our_file["filter_type"] == "raw"
            
            # Verify path structure
            expected_path = "historical-options/quote/AMD/2023/11/17/30m/20231201/raw/data.parquet"
            assert our_file["object_name"] == expected_path
    except Exception as e:
        handle_minio_error(e)