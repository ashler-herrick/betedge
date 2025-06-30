"""
Data models for Kafka publishing.
"""

from typing import Optional
from pydantic import BaseModel, Field


class KafkaPublishConfig(BaseModel):
    """Configuration for Kafka publishing."""
    
    topic: str = Field(
        ...,
        description="Kafka topic name"
    )
    key: Optional[str] = Field(
        None,
        description="Kafka message key"
    )
    headers: Optional[dict] = Field(
        None,
        description="Kafka message headers"
    )