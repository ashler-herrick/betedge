"""
MinIO publisher for object storage operations.
"""

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
        "extra": "ignore",  # Ignore extra environment variables
    }


def get_minio_storage_options(config: MinIOConfig) -> dict:
    """Convert MinIO config vars to Polars-compatible storage options"""
    endpoint = config.endpoint
    secure = config.secure

    # Build the endpoint URL
    protocol = "https" if secure else "http"
    endpoint_url = f"{protocol}://{endpoint}"

    return {
        "aws_access_key_id": config.access_key,
        "aws_secret_access_key": config.secret_key,
        "aws_endpoint_url": endpoint_url,
        "aws_region": "us-east-1",
        "aws_allow_http": str(not secure).lower(),
    }
