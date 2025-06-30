"""
Configuration models for MinIO object storage.
"""

import os
from typing import Literal
from pydantic import BaseModel, Field


class MinIOConfig(BaseModel):
    """Configuration for MinIO S3-compatible object storage."""
    
    endpoint: str = Field(
        default_factory=lambda: os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        description="MinIO server endpoint"
    )
    access_key: str = Field(
        default_factory=lambda: os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        description="MinIO access key"
    )
    secret_key: str = Field(
        default_factory=lambda: os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        description="MinIO secret key"
    )
    bucket: str = Field(
        default_factory=lambda: os.getenv("MINIO_BUCKET", "betedge-data"),
        description="MinIO bucket name for data storage"
    )
    secure: bool = Field(
        default_factory=lambda: os.getenv("MINIO_SECURE", "false").lower() == "true",
        description="Use HTTPS for MinIO connections"
    )
    region: str = Field(
        default_factory=lambda: os.getenv("MINIO_REGION", "us-east-1"),
        description="MinIO region"
    )
    
    class Config:
        """Pydantic configuration."""
        env_prefix = "MINIO_"


def interval_ms_to_string(interval_ms: int) -> str:
    """
    Convert interval in milliseconds to human-readable format.
    
    Args:
        interval_ms: Interval in milliseconds
        
    Returns:
        Human-readable interval string (e.g., "15m", "1h", "1d")
    """
    if interval_ms == 0:
        return "tick"
    elif interval_ms < 60000:  # Less than 1 minute
        return f"{interval_ms // 1000}s"
    elif interval_ms < 3600000:  # Less than 1 hour
        minutes = interval_ms // 60000
        return f"{minutes}m"
    elif interval_ms < 86400000:  # Less than 1 day
        hours = interval_ms // 3600000
        return f"{hours}h"
    else:
        days = interval_ms // 86400000
        return f"{days}d"


def expiration_to_string(exp: str) -> str:
    """
    Convert expiration to human-readable format.
    
    Args:
        exp: Expiration string (e.g., "0" or "20231117")
        
    Returns:
        Human-readable expiration ("all" for 0, otherwise unchanged)
    """
    return "all" if exp == "0" else exp


class MinIOPublishConfig(BaseModel):
    """Configuration for publishing data to MinIO."""
    
    schema: str = Field(
        default="quote",
        description="Data schema type (e.g., 'quote', 'ohlc', 'trade')"
    )
    root: str = Field(
        ...,
        description="Option root symbol (e.g., 'AAPL')"
    )
    date: str = Field(
        ...,
        description="Date in YYYYMMDD format",
        pattern=r"^\d{8}$"
    )
    interval: int = Field(
        ...,
        description="Interval in milliseconds",
        ge=0
    )
    exp: str = Field(
        ...,
        description="Expiration as string (e.g., '0' for all expirations)"
    )
    filter_type: str = Field(
        default="filtered",
        description="Filter type applied to data"
    )
    
    def generate_object_path(self) -> str:
        """
        Generate S3 object path for this configuration with path-based metadata.
        
        Returns:
            S3 object path in format: historical-options/{schema}/{root}/{year}/{month}/{day}/{interval}/{exp}/{filter}/data.parquet
        """
        from datetime import datetime
        
        # Parse date
        date_obj = datetime.strptime(self.date, "%Y%m%d")
        
        # Convert interval and expiration to human-readable format
        interval_str = interval_ms_to_string(self.interval)
        exp_str = expiration_to_string(self.exp)
        
        # Generate hierarchical path with all metadata in the path
        return f"historical-options/{self.schema}/{self.root}/{date_obj.year}/{date_obj.month:02d}/{date_obj.day:02d}/{interval_str}/{exp_str}/{self.filter_type}/data.parquet"
    
    def generate_prefix(self) -> str:
        """
        Generate S3 prefix for listing objects with same schema/root/date.
        
        Returns:
            S3 prefix for listing
        """
        from datetime import datetime
        
        date_obj = datetime.strptime(self.date, "%Y%m%d")
        return f"historical-options/{self.schema}/{self.root}/{date_obj.year}/{date_obj.month:02d}/{date_obj.day:02d}/"