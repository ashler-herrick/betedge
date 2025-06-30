"""
Synchronous Kafka producer with ordering guarantees.
"""

import logging
import time
from typing import Any, Dict, Optional

from confluent_kafka import Producer, KafkaException
import orjson

from .config import KafkaConfig
from .topics import TopicManager

logger = logging.getLogger(__name__)


class KafkaProducer:
    """Synchronous Kafka producer with message ordering guarantees."""
    
    def __init__(self, config: Optional[KafkaConfig] = None):
        """
        Initialize Kafka producer.
        
        Args:
            config: Kafka configuration, uses default if None
        """
        self.config = config or KafkaConfig()
        self._producer = None
        self._delivery_reports = []
        self._error_count = 0
        self._success_count = 0
        self._topic_manager = TopicManager(self.config)
        
    def connect(self) -> None:
        """Connect to Kafka broker."""
        if self._producer is not None:
            logger.warning("Producer already connected")
            return
            
        try:
            producer_config = self.config.get_producer_config()
            self._producer = Producer(producer_config)
            logger.info(f"Connected to Kafka brokers: {self.config.bootstrap_servers}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise
    
    def send_message(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        partition: Optional[int] = None
    ) -> bool:
        """
        Send a message synchronously with ordering guarantees.
        
        Args:
            topic: Kafka topic name
            value: Message value (will be JSON serialized)
            key: Message key for partitioning
            headers: Optional message headers
            partition: Specific partition (default uses key-based partitioning)
            
        Returns:
            True if message was successfully delivered, False otherwise
        """
        if self._producer is None:
            self.connect()
        
        # Ensure topic exists before sending
        if not self._topic_manager.ensure_topic_exists(topic):
            logger.error(f"Failed to ensure topic '{topic}' exists")
            return False
        
        # Apply prefix to topic name for actual sending
        full_topic = self.config.apply_topic_prefix(topic)
        
        # Serialize value
        try:
            if isinstance(value, (dict, list)):
                # Use orjson with custom default for Decimal support
                def default(obj):
                    if hasattr(obj, '__float__'):
                        return float(obj)
                    raise TypeError
                
                serialized_value = orjson.dumps(value, default=default)
            elif isinstance(value, bytes):
                serialized_value = value
            else:
                serialized_value = str(value).encode('utf-8')
        except Exception as e:
            logger.error(f"Failed to serialize message value: {e}")
            return False
        
        # Prepare headers
        kafka_headers = []
        if headers:
            for k, v in headers.items():
                kafka_headers.append((k, v.encode('utf-8') if isinstance(v, str) else v))
        
        # Delivery report storage
        delivery_report = {"delivered": False, "error": None}
        
        def delivery_callback(err, msg):
            """Callback for delivery reports."""
            if err is not None:
                delivery_report["error"] = str(err)
                logger.error(f"Message delivery failed: {err}")
                self._error_count += 1
            else:
                delivery_report["delivered"] = True
                logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")
                self._success_count += 1
        
        try:
            # Produce message
            self._producer.produce(
                topic=full_topic,
                value=serialized_value,
                key=key.encode('utf-8') if key else None,
                headers=kafka_headers if kafka_headers else None,
                partition=partition if partition is not None else -1,
                callback=delivery_callback
            )
            
            # Flush to ensure synchronous delivery
            # This blocks until the message is delivered or times out
            remaining = self._producer.flush(timeout=self.config.flush_timeout)
            
            if remaining > 0:
                logger.error(f"Failed to deliver {remaining} messages within timeout")
                return False
            
            # Check delivery status
            if delivery_report["delivered"]:
                return True
            else:
                logger.error(f"Message not delivered: {delivery_report['error']}")
                return False
                
        except KafkaException as e:
            logger.error(f"Kafka exception: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error sending message: {e}")
            return False
    
    def send_batch(
        self,
        topic: str,
        messages: list[Dict[str, Any]],
        key_field: Optional[str] = None
    ) -> tuple[int, int]:
        """
        Send a batch of messages synchronously, preserving order.
        
        Args:
            topic: Kafka topic name
            messages: List of message dictionaries
            key_field: Field name to use as message key
            
        Returns:
            Tuple of (successful_count, failed_count)
        """
        if not messages:
            return 0, 0
            
        successful = 0
        failed = 0
        
        for message in messages:
            # Extract key if specified
            key = None
            if key_field and key_field in message:
                key = str(message[key_field])
            
            # Send message
            if self.send_message(topic, message, key=key):
                successful += 1
            else:
                failed += 1
                # Log failed message for debugging
                logger.error(f"Failed to send message: {message}")
        
        logger.info(f"Batch send complete: {successful} successful, {failed} failed")
        return successful, failed
    
    def send_with_retry(
        self,
        topic: str,
        value: Any,
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        max_retries: Optional[int] = None
    ) -> bool:
        """
        Send a message with retry logic.
        
        Args:
            topic: Kafka topic name
            value: Message value
            key: Message key
            headers: Optional headers
            max_retries: Maximum retry attempts (uses config default if None)
            
        Returns:
            True if message was eventually delivered, False otherwise
        """
        max_retries = max_retries or self.config.max_retries
        
        for attempt in range(max_retries + 1):
            if attempt > 0:
                # Exponential backoff
                sleep_time = min(2 ** (attempt - 1), 30)  # Max 30 seconds
                logger.info(f"Retrying message send (attempt {attempt + 1}/{max_retries + 1}) after {sleep_time}s")
                time.sleep(sleep_time)
            
            if self.send_message(topic, value, key=key, headers=headers):
                if attempt > 0:
                    logger.info(f"Message delivered successfully after {attempt + 1} attempts")
                return True
        
        logger.error(f"Failed to deliver message after {max_retries + 1} attempts")
        
        # Send to error topic if configured
        if self.config.error_topic:
            error_record = {
                "original_topic": topic,
                "original_key": key,
                "original_value": value,
                "error": "Max retries exceeded",
                "timestamp": int(time.time() * 1000),
                "attempts": max_retries + 1
            }
            self.send_message(self.config.error_topic, error_record)
        
        return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get producer metrics.
        
        Returns:
            Dictionary containing producer metrics
        """
        metrics = {
            "messages_sent": self._success_count,
            "messages_failed": self._error_count,
            "success_rate": self._success_count / (self._success_count + self._error_count) 
                           if (self._success_count + self._error_count) > 0 else 0
        }
        
        if self._producer and self.config.enable_metrics:
            try:
                # Get internal producer metrics
                kafka_metrics = self._producer.list_topics(timeout=1)
                if hasattr(kafka_metrics, 'brokers'):
                    metrics["broker_count"] = len(kafka_metrics.brokers)
                if hasattr(kafka_metrics, 'topics'):
                    metrics["topic_count"] = len(kafka_metrics.topics)
            except Exception:
                # Skip if metrics not available
                pass
        
        return metrics
    
    def close(self) -> None:
        """Close producer and clean up resources."""
        if self._producer is not None:
            # Flush any remaining messages
            remaining = self._producer.flush(timeout=self.config.flush_timeout)
            if remaining > 0:
                logger.warning(f"Closed producer with {remaining} messages still in queue")
            
            self._producer = None
            
        # Close topic manager
        if self._topic_manager is not None:
            self._topic_manager.close()
            
        logger.info(f"Kafka producer closed. Sent: {self._success_count}, Failed: {self._error_count}")
    
    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()