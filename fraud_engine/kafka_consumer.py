"""
Confluent Kafka Consumer:
    - Initialize connection to the Kafka broker
    - Subscribe to the transactions topic
    - Poll for incoming transaction events with retry on transient errors
    - Close connection cleanly on shutdown
"""

import json
import sys
import logging
import time
import random
from confluent_kafka import Consumer, KafkaError, KafkaException

# Structured logging consistent with other services
logging.basicConfig(
    level=logging.INFO,
    format='{"timestamp": "%(asctime)s", "level": "%(levelname)s", "service": "fraud-engine-consumer", "message": "%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


class TransactionConsumer:
    """Kafka consumer for transaction events."""

    def __init__(self, host: str, port: int, topic: str):
        self.topic = topic
        self.consumer = Consumer({
            "bootstrap.servers": f"{host}:{port}",
            "group.id": "consumer-group-1",
            "auto.offset.reset": "earliest",
        })

    def subscribe(self):
        """Subscribe to the transactions topic."""
        try:
            self.consumer.subscribe([self.topic])
            logger.info(f"Subscribed to topic: {self.topic}")
        except Exception as e:
            logger.error(f"Failed to subscribe to topic {self.topic}: {e}")
            raise

    def poll(self, max_retries: int = 3):
        """
        Poll for the next transaction event.

        Retries on transient Kafka errors with exponential backoff and jitter.
        Jitter prevents multiple consumer instances from retrying simultaneously
        and hammering a recovering broker.

        Returns (key, value) tuple or None if no message is available yet.
        Raises on permanent errors or after exhausting retries.
        """
        for attempt in range(max_retries):
            try:
                trn = self.consumer.poll(timeout=1.0)

                if trn is None:
                    return None

                if trn.error():
                    # End of partition is informational, not an error
                    if trn.error().code() == KafkaError._PARTITION_EOF:
                        return None
                    # All other Kafka errors are re-raised for retry
                    raise KafkaException(trn.error())

                key = trn.key().decode("utf-8") if trn.key() else None
                value = json.loads(trn.value().decode("utf-8"))
                return key, value

            except KafkaException as e:
                if attempt == max_retries - 1:
                    logger.error(f"Kafka poll failed after {max_retries} attempts: {e}")
                    raise

                # Exponential backoff with jitter
                wait = (2 ** attempt) + random.uniform(0, 1)
                logger.warning(
                    f"Transient Kafka error on attempt {attempt + 1}/{max_retries}, "
                    f"retrying in {wait:.1f}s: {e}"
                )
                time.sleep(wait)

            except Exception as e:
                logger.error(f"Failed to decode message: {e}")
                raise

    def close(self):
        """Close the consumer connection cleanly."""
        self.consumer.close()
        logger.info("Kafka consumer connection closed")