"""
Unit tests for storage configuration module.
"""

import pytest

from betedge_data.storage.config import MinIOConfig, MinIOPublishConfig, interval_ms_to_string, expiration_to_string


@pytest.mark.unit
class TestMinIOConfig:
    """Test MinIOConfig class."""

    def test_default_config(self):
        """Test default configuration values."""
        config = MinIOConfig()

        assert config.endpoint == "localhost:9000"
        assert config.access_key == "minioadmin"
        assert config.secret_key == "minioadmin"
        assert config.bucket == "betedge-data"
        assert config.secure is False
        assert config.region == "us-east-1"

    def test_config_from_env(self, mock_env_vars):
        """Test configuration from environment variables."""
        config = MinIOConfig()

        assert config.endpoint == "test-minio:9000"
        assert config.access_key == "test-access"
        assert config.secret_key == "test-secret"
        assert config.bucket == "test-bucket"
        assert config.secure is False
        assert config.region == "us-test-1"

    def test_config_override(self):
        """Test configuration with explicit values."""
        config = MinIOConfig(
            endpoint="custom:9000",
            access_key="custom-key",
            secret_key="custom-secret",
            bucket="custom-bucket",
            secure=True,
            region="eu-west-1",
        )

        assert config.endpoint == "custom:9000"
        assert config.access_key == "custom-key"
        assert config.secret_key == "custom-secret"
        assert config.bucket == "custom-bucket"
        assert config.secure is True
        assert config.region == "eu-west-1"

    def test_secure_from_env(self, monkeypatch):
        """Test secure flag parsing from environment."""
        # Test "true" value
        monkeypatch.setenv("MINIO_SECURE", "true")
        config = MinIOConfig()
        assert config.secure is True

        # Test "True" value
        monkeypatch.setenv("MINIO_SECURE", "True")
        config = MinIOConfig()
        assert config.secure is True

        # Test "false" value
        monkeypatch.setenv("MINIO_SECURE", "false")
        config = MinIOConfig()
        assert config.secure is False

        # Test any other value
        monkeypatch.setenv("MINIO_SECURE", "yes")
        config = MinIOConfig()
        assert config.secure is False


@pytest.mark.unit
class TestMinIOPublishConfig:
    """Test MinIOPublishConfig class."""

    def test_default_config(self):
        """Test default configuration values."""
        config = MinIOPublishConfig(root="AAPL", date="20231117", interval=900000, exp="0")

        assert config.schema == "quote"
        assert config.root == "AAPL"
        assert config.date == "20231117"
        assert config.interval == 900000
        assert config.exp == "0"
        assert config.filter_type == "filtered"

    def test_date_validation(self):
        """Test date format validation."""
        # Valid date
        config = MinIOPublishConfig(root="AAPL", date="20231117", interval=900000, exp="0")
        assert config.date == "20231117"

        # Invalid date format
        with pytest.raises(ValueError):
            MinIOPublishConfig(
                root="AAPL",
                date="2023-11-17",  # Wrong format
                interval=900000,
                exp="0",
            )

    def test_interval_validation(self):
        """Test interval validation."""
        # Valid intervals
        for interval in [0, 60000, 900000, 3600000]:
            config = MinIOPublishConfig(root="AAPL", date="20231117", interval=interval, exp="0")
            assert config.interval == interval

        # Invalid interval (negative)
        with pytest.raises(ValueError):
            MinIOPublishConfig(root="AAPL", date="20231117", interval=-1, exp="0")

    def test_generate_object_path(self):
        """Test object path generation."""
        config = MinIOPublishConfig(
            schema="quote",
            root="AAPL",
            date="20231117",
            interval=900000,  # 15 minutes
            exp="0",
            filter_type="filtered",
        )

        path = config.generate_object_path()
        expected = "historical-options/quote/AAPL/2023/11/17/15m/all/filtered/data.parquet"
        assert path == expected

    def test_generate_object_path_with_exp(self):
        """Test object path generation with specific expiration."""
        config = MinIOPublishConfig(
            schema="ohlc",
            root="MSFT",
            date="20240105",
            interval=3600000,  # 1 hour
            exp="20240119",
            filter_type="raw",
        )

        path = config.generate_object_path()
        expected = "historical-options/ohlc/MSFT/2024/01/05/1h/20240119/raw/data.parquet"
        assert path == expected

    def test_generate_prefix(self):
        """Test prefix generation for listing."""
        config = MinIOPublishConfig(schema="quote", root="AAPL", date="20231117", interval=900000, exp="0")

        prefix = config.generate_prefix()
        expected = "historical-options/quote/AAPL/2023/11/17/"
        assert prefix == expected


@pytest.mark.unit
class TestUtilityFunctions:
    """Test utility functions."""

    def test_interval_ms_to_string(self):
        """Test interval milliseconds to string conversion."""
        test_cases = [
            (0, "tick"),
            (1000, "1s"),
            (30000, "30s"),
            (60000, "1m"),
            (300000, "5m"),
            (900000, "15m"),
            (1800000, "30m"),
            (3600000, "1h"),
            (7200000, "2h"),
            (86400000, "1d"),
            (172800000, "2d"),
        ]

        for ms, expected in test_cases:
            assert interval_ms_to_string(ms) == expected

    def test_expiration_to_string(self):
        """Test expiration to string conversion."""
        assert expiration_to_string("0") == "all"
        assert expiration_to_string("20231117") == "20231117"
        assert expiration_to_string("20240315") == "20240315"
        assert expiration_to_string("") == ""
        assert expiration_to_string("any_other_value") == "any_other_value"


@pytest.mark.unit
class TestPathStructure:
    """Test path-based metadata structure."""

    def test_comprehensive_path_examples(self):
        """Test various path generation scenarios."""
        test_cases = [
            # (config_params, expected_path)
            (
                {
                    "schema": "quote",
                    "root": "AAPL",
                    "date": "20231113",
                    "interval": 0,  # tick data
                    "exp": "0",
                    "filter_type": "filtered",
                },
                "historical-options/quote/AAPL/2023/11/13/tick/all/filtered/data.parquet",
            ),
            (
                {
                    "schema": "ohlc",
                    "root": "SPX",
                    "date": "20240102",
                    "interval": 60000,  # 1 minute
                    "exp": "20240119",
                    "filter_type": "raw",
                },
                "historical-options/ohlc/SPX/2024/01/02/1m/20240119/raw/data.parquet",
            ),
            (
                {
                    "schema": "trade",
                    "root": "TSLA",
                    "date": "20231215",
                    "interval": 86400000,  # 1 day
                    "exp": "0",
                    "filter_type": "filtered",
                },
                "historical-options/trade/TSLA/2023/12/15/1d/all/filtered/data.parquet",
            ),
        ]

        for params, expected_path in test_cases:
            config = MinIOPublishConfig(**params)
            assert config.generate_object_path() == expected_path

    def test_path_parsing_compatibility(self):
        """Test that paths can be parsed back to extract metadata."""
        config = MinIOPublishConfig(
            schema="quote", root="AAPL", date="20231117", interval=900000, exp="0", filter_type="filtered"
        )

        path = config.generate_object_path()
        parts = path.split("/")

        assert parts[0] == "historical-options"
        assert parts[1] == "quote"  # schema
        assert parts[2] == "AAPL"  # root
        assert parts[3] == "2023"  # year
        assert parts[4] == "11"  # month
        assert parts[5] == "17"  # day
        assert parts[6] == "15m"  # interval
        assert parts[7] == "all"  # exp (0 -> all)
        assert parts[8] == "filtered"  # filter_type
        assert parts[9] == "data.parquet"  # filename
