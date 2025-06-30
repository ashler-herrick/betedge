"""
Unit tests for MinIO publisher module.
"""

from unittest.mock import patch

import pytest

from betedge_data.storage.publisher import MinIOPublisher
from betedge_data.storage.config import MinIOConfig, MinIOPublishConfig
from tests.utils.mocks import MockMinIOClient


@pytest.mark.unit
class TestMinIOPublisher:
    """Test MinIOPublisher class."""

    @patch("betedge_data.storage.publisher.Minio")
    def test_initialization(self, mock_minio_class):
        """Test MinIOPublisher initialization."""
        # Setup mock
        mock_client = MockMinIOClient()
        mock_minio_class.return_value = mock_client

        config = MinIOConfig(
            endpoint="test:9000", access_key="test-key", secret_key="test-secret", bucket="test-bucket"
        )

        # Create publisher
        publisher = MinIOPublisher(config)

        # Verify Minio client was created with correct params
        mock_minio_class.assert_called_once_with(
            endpoint="test:9000", access_key="test-key", secret_key="test-secret", secure=False, region="us-east-1"
        )

        # Verify bucket existence was checked
        assert publisher.bucket == "test-bucket"

    @patch("betedge_data.storage.publisher.Minio")
    def test_bucket_creation(self, mock_minio_class):
        """Test bucket creation when it doesn't exist."""
        # Setup mock where bucket doesn't exist
        mock_client = MockMinIOClient(bucket_exists=False)
        mock_minio_class.return_value = mock_client

        config = MinIOConfig(bucket="new-bucket")

        # Create publisher
        publisher = MinIOPublisher(config)

        # Verify bucket was created
        assert mock_client.bucket_exists_value is True

    @patch("betedge_data.storage.publisher.Minio")
    def test_bucket_creation_error(self, mock_minio_class):
        """Test error handling during bucket creation."""
        # Setup mock that raises error on bucket operations
        mock_client = MockMinIOClient(bucket_exists=False, raise_errors=True)
        mock_minio_class.return_value = mock_client

        config = MinIOConfig()

        # Should raise RuntimeError
        with pytest.raises(RuntimeError) as exc_info:
            MinIOPublisher(config)

        assert "MinIO bucket setup failed" in str(exc_info.value)

    @pytest.mark.asyncio
    @patch("betedge_data.storage.publisher.Minio")
    async def test_publish_parquet_data(self, mock_minio_class, sample_parquet_data):
        """Test publishing Parquet data."""
        # Setup mock
        mock_client = MockMinIOClient()
        mock_minio_class.return_value = mock_client

        publisher = MinIOPublisher(MinIOConfig())

        # Create publish config
        config = MinIOPublishConfig(schema="quote", root="AAPL", date="20231117", interval=900000, exp="0")

        # Publish data
        size = await publisher.publish_parquet_data(sample_parquet_data, config)

        # Verify upload
        assert size > 0
        expected_path = "historical-options/quote/AAPL/2023/11/17/15m/all/filtered/data.parquet"
        assert expected_path in mock_client.uploaded_objects

        # Verify metadata
        uploaded = mock_client.uploaded_objects[expected_path]
        assert uploaded["metadata"]["schema"] == "quote"
        assert uploaded["metadata"]["root"] == "AAPL"
        assert uploaded["metadata"]["date"] == "20231117"
        assert uploaded["metadata"]["interval"] == "900000"
        assert uploaded["metadata"]["exp"] == "0"
        assert uploaded["metadata"]["filter_type"] == "filtered"

    @pytest.mark.asyncio
    @patch("betedge_data.storage.publisher.Minio")
    async def test_publish_error_handling(self, mock_minio_class, sample_parquet_data):
        """Test error handling during publish."""
        # Setup mock that works for initialization but fails on put_object
        mock_client = MockMinIOClient(bucket_exists=True)
        mock_client.raise_errors = False  # Allow init to work
        mock_minio_class.return_value = mock_client

        publisher = MinIOPublisher(MinIOConfig())
        
        # Now make put_object fail
        mock_client.raise_errors = True
        config = MinIOPublishConfig(root="AAPL", date="20231117", interval=900000, exp="0")

        # Should raise RuntimeError
        with pytest.raises(RuntimeError) as exc_info:
            await publisher.publish_parquet_data(sample_parquet_data, config)

        assert "MinIO upload failed" in str(exc_info.value)

    @patch("betedge_data.storage.publisher.Minio")
    def test_list_existing_files(self, mock_minio_class):
        """Test listing existing files."""
        # Setup mock with existing objects
        mock_client = MockMinIOClient()
        mock_minio_class.return_value = mock_client

        # Pre-populate some objects in the format expected by stat_object
        test_dates = ["20231113", "20231114", "20231115"]
        for date in test_dates:
            obj_path = f"historical-options/quote/AAPL/2023/11/{date[-2:]}/15m/all/filtered/data.parquet"
            mock_client.objects[obj_path] = b"test data"

        publisher = MinIOPublisher(MinIOConfig())

        # List existing files
        dates_to_check = ["20231113", "20231114", "20231115", "20231116", "20231117"]
        existing = publisher.list_existing_files(
            root="AAPL", dates=dates_to_check, interval=900000, exp="0", schema="quote", filter_type="filtered"
        )

        # Should find the first 3 dates that we added to mock_client.objects
        assert existing == {"20231113", "20231114", "20231115"}

    @patch("betedge_data.storage.publisher.Minio")
    def test_list_files_for_symbol(self, mock_minio_class):
        """Test listing files for a symbol."""
        # Setup mock
        mock_client = MockMinIOClient()
        mock_minio_class.return_value = mock_client

        # Create test objects
        test_objects = [
            "historical-options/quote/AAPL/2023/11/13/15m/all/filtered/data.parquet",
            "historical-options/quote/AAPL/2023/11/14/15m/all/filtered/data.parquet",
            "historical-options/ohlc/AAPL/2023/11/13/1h/20231117/raw/data.parquet",
            "historical-options/quote/MSFT/2023/11/13/15m/all/filtered/data.parquet",  # Different symbol
        ]

        for obj_path in test_objects:
            mock_client.objects[obj_path] = b"test data"

        publisher = MinIOPublisher(MinIOConfig())

        # List files for AAPL with quote schema
        files = publisher.list_files_for_symbol(root="AAPL", schema="quote")

        # Should return 2 AAPL quote files
        assert len(files) == 2
        assert all(f["root"] == "AAPL" for f in files)
        assert all(f["schema"] == "quote" for f in files)

        # Test date filtering
        files = publisher.list_files_for_symbol(root="AAPL", start_date="20231114", end_date="20231114")

        # Should return only the file from 2023-11-14
        assert len(files) == 1
        assert files[0]["date"] == "20231114"

    @patch("betedge_data.storage.publisher.Minio")
    def test_delete_file(self, mock_minio_class):
        """Test file deletion."""
        # Setup mock
        mock_client = MockMinIOClient()
        mock_minio_class.return_value = mock_client

        # Add test object
        test_path = "historical-options/quote/AAPL/2023/11/13/15m/all/filtered/data.parquet"
        mock_client.objects[test_path] = b"test data"

        publisher = MinIOPublisher(MinIOConfig())

        # Delete file
        result = publisher.delete_file(test_path)

        assert result is True
        assert test_path in mock_client.deleted_objects
        assert test_path not in mock_client.objects

    @patch("betedge_data.storage.publisher.Minio")
    def test_delete_file_error(self, mock_minio_class):
        """Test error handling during file deletion."""
        # Setup mock that works for initialization but fails on remove_object
        mock_client = MockMinIOClient(bucket_exists=True)
        mock_client.raise_errors = False  # Allow init to work
        mock_minio_class.return_value = mock_client

        publisher = MinIOPublisher(MinIOConfig())
        
        # Now make remove_object fail
        mock_client.raise_errors = True

        # Delete should return False on error
        result = publisher.delete_file("any/path")
        assert result is False

    @patch("betedge_data.storage.publisher.Minio")
    def test_get_bucket_stats(self, mock_minio_class):
        """Test getting bucket statistics."""
        # Setup mock with various objects
        mock_client = MockMinIOClient()
        mock_minio_class.return_value = mock_client

        # Add test objects for multiple symbols
        test_objects = [
            ("historical-options/quote/AAPL/2023/11/13/15m/all/filtered/data.parquet", 1024),
            ("historical-options/quote/AAPL/2023/11/14/15m/all/filtered/data.parquet", 2048),
            ("historical-options/quote/MSFT/2023/11/13/15m/all/filtered/data.parquet", 1500),
            ("historical-options/ohlc/TSLA/2023/11/13/1h/all/filtered/data.parquet", 3000),
        ]

        for obj_path, size in test_objects:
            mock_client.objects[obj_path] = b"x" * size

        publisher = MinIOPublisher(MinIOConfig())

        # Get stats
        stats = publisher.get_bucket_stats()

        assert stats["bucket"] == "betedge-data"
        assert stats["total_files"] == 4
        assert stats["total_size_bytes"] == 7572  # Sum of sizes
        assert stats["total_size_mb"] == 0.01  # Rounded
        assert stats["unique_symbols"] == 3
        assert set(stats["symbols"]) == {"AAPL", "MSFT", "TSLA"}

    @patch("betedge_data.storage.publisher.Minio")
    def test_path_parsing_in_list_files(self, mock_minio_class):
        """Test path parsing logic in list_files_for_symbol."""
        # Setup mock
        mock_client = MockMinIOClient()
        mock_minio_class.return_value = mock_client

        # Add test object with complete path structure
        test_path = "historical-options/quote/AAPL/2023/11/13/15m/all/filtered/data.parquet"
        mock_client.objects[test_path] = b"test data"

        publisher = MinIOPublisher(MinIOConfig())

        # List files
        files = publisher.list_files_for_symbol(root="AAPL")

        assert len(files) == 1
        file_info = files[0]

        # Verify all parsed fields
        assert file_info["schema"] == "quote"
        assert file_info["root"] == "AAPL"
        assert file_info["date"] == "20231113"
        assert file_info["year"] == 2023
        assert file_info["month"] == 11
        assert file_info["day"] == 13
        assert file_info["interval_str"] == "15m"
        assert file_info["exp"] == "all"
        assert file_info["filter_type"] == "filtered"

    @patch("betedge_data.storage.publisher.Minio")
    def test_context_manager(self, mock_minio_class):
        """Test context manager functionality."""
        mock_client = MockMinIOClient()
        mock_minio_class.return_value = mock_client

        config = MinIOConfig()

        # Test sync context manager
        with MinIOPublisher(config) as publisher:
            assert publisher is not None
            assert publisher.bucket == "betedge-data"
