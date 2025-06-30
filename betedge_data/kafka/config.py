"""
Kafka configuration settings.
"""

from typing import Any, Dict
from pydantic import Field
from pydantic_settings import BaseSettings


class KafkaConfig(BaseSettings):
    """Configuration for Kafka producer and topics."""
    
    # Connection settings
    bootstrap_servers: str = Field(
        default="kafka:29092",
        description="Kafka broker addresses"
    )
    
    # Producer configuration for ordering guarantees
    producer_config: Dict[str, Any] = Field(
        default_factory=lambda: {
            "bootstrap.servers": "kafka:29092",
            "enable.idempotence": True,  # Exactly-once semantics
            "acks": "all",  # Wait for all replicas
            "max.in.flight.requests.per.connection": 1,  # Preserve ordering
            "compression.type": "snappy",  # Efficient compression
            "batch.size": 1,  # Minimal batching for immediate sends
            "linger.ms": 0,  # Send immediately
            "retries": 3,  # Retry on failure
            "retry.backoff.ms": 100,  # Backoff between retries
            "request.timeout.ms": 30000,  # 30 second timeout
            "delivery.timeout.ms": 60000,  # 60 second delivery timeout
            "message.max.bytes": 10485760,  # 10MB max message size
        },
        description="Kafka producer configuration"
    )
    
    # Flush settings
    flush_timeout: float = Field(
        default=10.0,
        description="Timeout for producer flush operations in seconds"
    )
    
    # Topic configuration
    topic_config: Dict[str, Any] = Field(
        default_factory=lambda: {
            "num_partitions": 1,  # Single partition for ordering
            "replication_factor": 1,  # Single replica for development
            "retention_ms": 604800000,  # 7 days retention
            "segment_ms": 86400000,  # 1 day segments
            "compression_type": "snappy",  # Match producer compression
        },
        description="Default topic configuration for auto-created topics"
    )
    
    # Topic management
    auto_create_topics: bool = Field(
        default=True,
        description="Automatically create topics if they don't exist"
    )
    
    topic_prefix: str = Field(
        default="",
        description="Optional prefix for all topic names (e.g., 'dev-', 'prod-')"
    )
    
    # Error handling
    error_topic: str = Field(
        default="data-processing-errors",
        description="Dead letter topic for failed messages"
    )
    
    max_retries: int = Field(
        default=3,
        description="Maximum number of send retries"
    )
    
    # Monitoring
    enable_metrics: bool = Field(
        default=True,
        description="Enable Kafka producer metrics"
    )
    
    metrics_interval_ms: int = Field(
        default=30000,  # 30 seconds
        description="Metrics reporting interval"
    )
    
    model_config = {
        "case_sensitive": False,
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "env_prefix": "KAFKA_",
        "extra": "ignore"
    }
    
    def get_producer_config(self) -> Dict[str, Any]:
        """
        Get producer configuration with bootstrap servers override.
        
        Returns:
            Complete producer configuration dict
        """
        config = self.producer_config.copy()
        config["bootstrap.servers"] = self.bootstrap_servers
        return config
    
    def apply_topic_prefix(self, topic_name: str) -> str:
        """
        Apply configured prefix to topic name.
        
        Args:
            topic_name: Base topic name
            
        Returns:
            Topic name with prefix applied
        """
        if self.topic_prefix and not topic_name.startswith(self.topic_prefix):
            return f"{self.topic_prefix}{topic_name}"
        return topic_name