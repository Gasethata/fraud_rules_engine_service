"""
Confluent Kafka Producer:
    - Initialize connection to the Kafka broker
    - Handle delivery reports (confirm message delivery)
    - Publish transactions to the Kafka topic
    - Close connection cleanly (flush buffered messages before exit)
"""

from confluent_kafka import Producer
import json, logging, sys

logger = logging.getLogger(__name__)

class TransactionProducer:
    """Initialization"""
    def __init__(self, host: int, port: int, topic: str):
        self.topic = topic
        self.producer = Producer({'bootstrap.servers': f"{host}:{port}",
                                  'acks': 1})

    def delivery_report(self, err, msg):
        """Events delivery report"""
        if err:
            logger.error(f"Delivery failed: {err}")
        else:
            logger.info(f"Transaction delivered to topic:{msg.topic()}  on partition:[{msg.partition()}]")

    def publish(self, key: str, value: dict):
        """Publish events to the topic"""
        self.producer.produce(
            topic=self.topic,
            key=key,
            value=json.dumps(value).encode("utf-8"),
            callback=self.delivery_report
        )
        self.producer.poll(0)

    def close(self):
        """Flush and close connections"""
        if self.producer:
            self.producer.flush()
        logger.info("Producer client connection closed.")