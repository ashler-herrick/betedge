"""
Kafka publisher for different data types using synchronous producer.
"""

import io
import logging
from typing import Any, Dict, Optional

from .models import KafkaPublishConfig
from .config import KafkaConfig
from .producer import KafkaProducer

logger = logging.getLogger(__name__)


class KafkaPublisher:
    """Kafka publisher for streaming data to topics with ordering guarantees."""
    
    def __init__(self, config: Optional[KafkaConfig] = None):
        """
        Initialize the Kafka publisher.
        
        Args:
            config: Kafka configuration
        """
        self.config = config or KafkaConfig()
        self.producer = KafkaProducer(self.config)
        self.producer.connect()
        logger.info("KafkaPublisher initialized with synchronous producer")
    
    
    async def publish_parquet_data(
        self,
        parquet_buffer: io.BytesIO,
        publish_config: KafkaPublishConfig
    ) -> int:
        """
        Publish Parquet data to Kafka topic.
        
        Args:
            parquet_buffer: BytesIO containing Parquet data
            publish_config: Kafka publishing configuration
            
        Returns:
            Size of published data in bytes
        """
        logger.info(f"Publishing Parquet data to topic '{publish_config.topic}'")
        
        parquet_buffer.seek(0)
        data = parquet_buffer.getvalue()
        data_size = len(data)
        
        # Optional: Warn about large messages approaching Kafka limits
        if data_size > 8 * 1024 * 1024:  # 8MB warning threshold
            logger.warning(f"Large Parquet data size: {data_size / (1024*1024):.1f}MB - may approach Kafka message limits")
        
        # Set up headers
        headers = publish_config.headers or {}
        headers["content-type"] = "application/octet-stream"
        headers["data-type"] = "parquet"
        headers["total-size"] = str(data_size)
        
        # Send as single message
        success = self.producer.send_with_retry(
            topic=publish_config.topic,
            value=data,
            key=publish_config.key,
            headers=headers
        )
        
        if success:
            logger.info(f"Published {data_size} bytes of Parquet data")
            return data_size
        else:
            logger.error("Failed to publish Parquet data")
            return 0
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get publisher metrics.
        
        Returns:
            Dictionary containing publisher and producer metrics
        """
        return self.producer.get_metrics()
    
    async def close(self) -> None:
        """Close Kafka connections and clean up resources."""
        logger.info("Closing KafkaPublisher")
        self.producer.close()
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()