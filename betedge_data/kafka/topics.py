"""
Topic management utilities for Kafka.
"""

import logging
from typing import Dict, List, Optional, Set
from confluent_kafka.admin import AdminClient, NewTopic, ConfigResource, ConfigSource

from .config import KafkaConfig

logger = logging.getLogger(__name__)


class TopicManager:
    """Manages Kafka topic operations including creation and configuration."""
    
    def __init__(self, config: Optional[KafkaConfig] = None):
        """
        Initialize topic manager.
        
        Args:
            config: Kafka configuration, uses default if None
        """
        self.config = config or KafkaConfig()
        self._admin_client = None
        self._topic_cache: Set[str] = set()
    
    def _get_admin_client(self) -> AdminClient:
        """
        Get or create admin client.
        
        Returns:
            AdminClient instance
        """
        if self._admin_client is None:
            admin_config = {
                'bootstrap.servers': self.config.bootstrap_servers,
                'socket.timeout.ms': 10000,
                'request.timeout.ms': 10000
            }
            self._admin_client = AdminClient(admin_config)
            logger.info(f"Created Kafka AdminClient for {self.config.bootstrap_servers}")
        return self._admin_client
    
    def topic_exists(self, topic_name: str) -> bool:
        """
        Check if a topic exists.
        
        Args:
            topic_name: Name of the topic to check
            
        Returns:
            True if topic exists, False otherwise
        """
        # Apply prefix if configured
        full_topic_name = self.config.apply_topic_prefix(topic_name)
        
        # Check cache first
        if full_topic_name in self._topic_cache:
            return True
        
        try:
            admin = self._get_admin_client()
            metadata = admin.list_topics(timeout=5)
            
            if full_topic_name in metadata.topics:
                self._topic_cache.add(full_topic_name)
                return True
            return False
            
        except Exception as e:
            logger.error(f"Error checking if topic '{full_topic_name}' exists: {e}")
            return False
    
    def create_topic(
        self, 
        topic_name: str,
        num_partitions: Optional[int] = None,
        replication_factor: Optional[int] = None,
        config: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Create a new topic with specified configuration.
        
        Args:
            topic_name: Name of the topic to create
            num_partitions: Number of partitions (uses config default if None)
            replication_factor: Replication factor (uses config default if None)
            config: Additional topic configuration (uses config default if None)
            
        Returns:
            True if topic was created or already exists, False on error
        """
        # Apply prefix if configured
        full_topic_name = self.config.apply_topic_prefix(topic_name)
        
        # Check if already exists
        if self.topic_exists(topic_name):
            logger.debug(f"Topic '{full_topic_name}' already exists")
            return True
        
        # Use defaults from config
        num_partitions = num_partitions or self.config.topic_config.get("num_partitions", 1)
        replication_factor = replication_factor or self.config.topic_config.get("replication_factor", 1)
        
        # Build topic config
        topic_config = {}
        if config:
            topic_config.update(config)
        
        # Apply defaults from config
        for key, value in self.config.topic_config.items():
            if key not in ["num_partitions", "replication_factor"] and key not in topic_config:
                # Convert retention_ms to string for Kafka config
                topic_config[key.replace("_", ".")] = str(value)
        
        try:
            admin = self._get_admin_client()
            new_topic = NewTopic(
                topic=full_topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor,
                config=topic_config
            )
            
            # Create topic
            futures = admin.create_topics([new_topic], operation_timeout=30)
            
            # Wait for operation to complete
            for topic, future in futures.items():
                try:
                    future.result()  # The result itself is None
                    logger.info(f"Successfully created topic '{topic}' with {num_partitions} partition(s)")
                    self._topic_cache.add(full_topic_name)
                    return True
                except Exception as e:
                    if "already exists" in str(e).lower():
                        logger.debug(f"Topic '{topic}' already exists")
                        self._topic_cache.add(full_topic_name)
                        return True
                    else:
                        logger.error(f"Failed to create topic '{topic}': {e}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error creating topic '{full_topic_name}': {e}")
            return False
    
    def ensure_topic_exists(
        self,
        topic_name: str,
        num_partitions: Optional[int] = None,
        replication_factor: Optional[int] = None,
        config: Optional[Dict[str, str]] = None
    ) -> bool:
        """
        Ensure a topic exists, creating it if necessary.
        
        This is a convenience method that combines topic_exists and create_topic.
        
        Args:
            topic_name: Name of the topic
            num_partitions: Number of partitions (uses config default if None)
            replication_factor: Replication factor (uses config default if None)
            config: Additional topic configuration (uses config default if None)
            
        Returns:
            True if topic exists or was created, False on error
        """
        if not self.config.auto_create_topics:
            # Just check existence, don't create
            exists = self.topic_exists(topic_name)
            if not exists:
                logger.warning(f"Topic '{topic_name}' does not exist and auto_create_topics is disabled")
            return exists
        
        return self.create_topic(topic_name, num_partitions, replication_factor, config)
    
    def list_topics(self, pattern: Optional[str] = None) -> List[str]:
        """
        List all topics, optionally filtered by pattern.
        
        Args:
            pattern: Optional pattern to filter topics (simple string matching)
            
        Returns:
            List of topic names
        """
        try:
            admin = self._get_admin_client()
            metadata = admin.list_topics(timeout=10)
            
            topics = []
            for topic in metadata.topics:
                # Skip internal topics
                if topic.startswith('_'):
                    continue
                    
                # Apply pattern filter if provided
                if pattern is None or pattern in topic:
                    topics.append(topic)
            
            return sorted(topics)
            
        except Exception as e:
            logger.error(f"Error listing topics: {e}")
            return []
    
    def get_topic_config(self, topic_name: str) -> Dict[str, str]:
        """
        Get configuration for a specific topic.
        
        Args:
            topic_name: Name of the topic
            
        Returns:
            Dictionary of topic configuration
        """
        # Apply prefix if configured
        full_topic_name = self.config.apply_topic_prefix(topic_name)
        
        try:
            admin = self._get_admin_client()
            
            # Create config resource for the topic
            resource = ConfigResource(ConfigResource.Type.TOPIC, full_topic_name)
            
            # Get configs
            futures = admin.describe_configs([resource])
            
            configs = {}
            for resource, future in futures.items():
                try:
                    result = future.result()
                    for config_entry in result.values():
                        # Only include non-default configs
                        if config_entry.source != ConfigSource.DEFAULT_CONFIG:
                            configs[config_entry.name] = config_entry.value
                except Exception as e:
                    logger.error(f"Error getting config for topic '{full_topic_name}': {e}")
            
            return configs
            
        except Exception as e:
            logger.error(f"Error getting topic config: {e}")
            return {}
    
    def close(self) -> None:
        """Close admin client connections."""
        if self._admin_client is not None:
            # AdminClient doesn't have a close method, but we can clear the reference
            self._admin_client = None
            logger.info("TopicManager closed")


# Convenience functions
_default_manager: Optional[TopicManager] = None


def get_default_topic_manager() -> TopicManager:
    """
    Get the default topic manager instance.
    
    Returns:
        TopicManager instance
    """
    global _default_manager
    if _default_manager is None:
        _default_manager = TopicManager()
    return _default_manager


def ensure_topic_exists(
    topic_name: str,
    num_partitions: Optional[int] = None,
    replication_factor: Optional[int] = None,
    config: Optional[Dict[str, str]] = None
) -> bool:
    """
    Convenience function to ensure a topic exists using default manager.
    
    Args:
        topic_name: Name of the topic
        num_partitions: Number of partitions
        replication_factor: Replication factor
        config: Additional topic configuration
        
    Returns:
        True if topic exists or was created, False on error
    """
    manager = get_default_topic_manager()
    return manager.ensure_topic_exists(topic_name, num_partitions, replication_factor, config)