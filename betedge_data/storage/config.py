"""
Configuration models for MinIO object storage.
"""

from pydantic import BaseModel, Field


class MinIOConfig(BaseModel):
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
