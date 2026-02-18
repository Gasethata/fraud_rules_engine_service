"""
Confluent Kafka Prodyucer class:
    Initialize the connection - Depends on Host IP and Port
    Handle delivary report - Checks for actual messages/streams
    Publish transactions to the Kafka topic - Depends on the availability of the Kafka topic
    Close connection - Empty and flushes the cache, and terminate the connection
"""

from confluent_kafka import Producer
import json, logging, sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s::: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%s',
    handlers=[
        logging.StreamHandler(sys.stdout)
        ]
    )   

logger = logging.getLogger(__name__)

class TransactionProducer:
    def __init__(self, host: int, port: int, topic: str):
        self.topic = topic
        self.producer = Producer({'bootstrap.servers': f"{host}:{port}"})

    def delivery_report(self, err, msg):
        if err:
            print(f"Delivery failed: {err}")
        else:
            print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def publish(self, key: str, value: dict):
        self.producer.produce(
            topic=self.topic,
            key=key,
            value=json.dumps(value).encode("utf-8"), #json.loads(value).decode("utf-8"),
            callback=self.delivery_report
        )
        self.producer.poll(0)

    def close(self):
        """Flush and close connections"""
        if self.producer:
            self.producer.flush
        logger.info("Producer client connection closed.")