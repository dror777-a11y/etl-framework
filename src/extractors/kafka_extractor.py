from typing import Dict, Any, List
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable


from .base_extractor import BaseExtractor


class KafkaExtractor(BaseExtractor):
    """Special extractor for Kafka"""

    def __init__(self, config: Dict[str, Any]):
        """Set up the Kafka extractor with connection details."""
        super().__init__(config)
        self.consumer = None

    def connect(self) -> bool:
        """Connect to Kafka using the provided configuration.
        Returns: True if connection successful, False otherwise"""

        try:
            bootstrap_servers = self.config.get("bootstrap_servers", ["localhost:9092"])
            topic = self.config.get("topic")
            group_id = self.config.get("group_id", "etl_consumer_group")
            auto_offset_reset = self.config.get("auto_offset_reset", "earliest")

            # Validate required parameters
            if not topic:
                raise ValueError("Topic name is required in config")

            #creating Kafka consumer
            self.consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                group_id=group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=True,
                value_deserializer=lambda x: x.decode('utf-8') if x else None, #deserializer-translate bytes to text
                consumer_timeout_ms=self.config.get("timeout_ms", 10000)  #after 10 sec stop waiting
            )

            # Test connection by getting metadata
            metadata = self.consumer.list_consumer_group_offsets()

            print(f"Successfully connected to KAFKA topic {topic}")
            return True

        except NoBrokersAvailable:
            print("Faild to connect to KAFKA- No brokers available")
            return False

        except KafkaError as e:
            print(f"Kafka error: {e}")
            return False

        except Exception as e:
            print(f"Unexpected error connecting to Kafka: {e}")
            return False

    def extract(self) -> List[Dict[str, Any]]:
        """Extract messages from the Kafka topic.
        Returns: List of messages from the topic, each as a dictionary"""

        if not self.consumer:
            raise RuntimeError("Not connected to Kafka. Call connect() first.")

        try:
            messages=[]
            max_messages = self.config.get("max_messages", 100)

            print(f"Starting to consume messages from Kafka (max: {max_messages})...")

            #Consume messages
            message_count=0
            for message in self.consumer:
                try:
                    raw_message={
                        "raw_value": message.value,
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "timestamp": message.timestamp,
                        "key": message.key.decode('utf-8') if message.key else None, #decode('utf-8')- translate bytes to text
                        "headers": dict(message.headers) if message.headers else {} #extra info
                    }

                    messages.append(raw_message)
                    message_count+=1

                    #check if we reached the limit
                    if message_count >= max_messages:
                        break

                except Exception as e:
                    print(f"Error extracting message: {e}")
                    continue
            print(f"Extracted {len(messages)} raw messages from Kafka")
            return messages

        except KafkaError as e:
            print(f"Error extracting data from Kafka: {e}")
            return []

        except Exception as e:
            print(f"Unexpected error during extraction: {e}")
            return []

    def disconnect(self) -> bool:
        """
        Close the Kafka connection.

        Returns:
            True if disconnection successful, False otherwise
        """
        try:
            if self.consumer:
                self.consumer.close()
                self.consumer = None
                print("Disconnected from Kafka")
            return True

        except Exception as e:
            print(f"Error disconnecting from Kafka: {e}")
            return False





