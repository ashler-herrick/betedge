"""
Integration tests for MinIO storage operations.
"""

import pytest
from betedge_data.storage.publisher import MinIOPublisher
from betedge_data.storage.config import MinIOConfig


@pytest.mark.integration
class TestMinIOIntegration:
    """Integration tests for MinIO operations requiring real MinIO instance."""

    @pytest.fixture
    def minio_publisher(self, integration_env_vars):
        """Create MinIOPublisher with integration environment variables."""
        return MinIOPublisher()

    @pytest.fixture  
    def minio_config(self, integration_env_vars):
        """Create MinIOConfig with integration environment variables."""
        return MinIOConfig()

    def test_minio_config_reads_env_vars(self, minio_config):
        """Test that MinIOConfig reads environment variables correctly."""
        assert minio_config.endpoint == "localhost:9000"
        assert minio_config.access_key == "minioadmin"
        assert minio_config.secret_key == "minioadmin123"
        assert minio_config.bucket == "test-integration-bucket"
        assert minio_config.secure is False
        assert minio_config.region == "us-east-1"


    def test_file_exists_with_real_minio(self, minio_publisher):
        """Test file_exists method with real MinIO connection."""
        # Test non-existent file
        assert minio_publisher.file_exists("non/existent/file.parquet") is False

    async def test_publish_and_exists_integration(self, minio_publisher):
        """Test full publish and exists workflow with real MinIO."""
        import io
        
        # Create test data
        test_data = io.BytesIO(b"test data for integration")
        test_key = "test/integration/data.parquet"
        
        # Initially file shouldn't exist
        assert minio_publisher.file_exists(test_key) is False
        
        # Publish the file
        await minio_publisher.publish(test_data, test_key)
        
        # Now file should exist
        assert minio_publisher.file_exists(test_key) is True
        
        # Clean up
        minio_publisher.delete_file(test_key)
        assert minio_publisher.file_exists(test_key) is False