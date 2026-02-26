"""
Confluent Kafka Consumer class:
    Initialize the connection - Depends on Host IP and Port
    Subscribe to the topic - validate the topic
    Consume events from the topic - All the transactions in the topic are streamed
    Close connection - Empty and flushes the cache, and terminate the connection
"""

import json, sys
import logging
from confluent_kafka import Consumer


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s::: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%s',
    handlers=[
        logging.StreamHandler(sys.stdout)
        ]
    ) 

logger = logging.getLogger(__name__)

class TransactionConsumer:
    """Consumer calls"""
    def __init__(self, host: int, port: int, topic: str):
        """Initialization"""
        self.topic = topic
        self.consumer = Consumer({'bootstrap.servers': f"{host}:{port}",
                                  'group.id': 'consumer-group-1',
                                  'auto.offset.reset': 'earliest'})

    def subscribe(self):
        """Validate the topic"""
        try:
            self.consumer.subscribe([self.topic])
            logger.info("Kafka consumer subscribed successfully.")
        except Exception as e:
            logger.info(f"Kafka consumer failed to subscribed. {e}")
            raise

    def poll(self):
        """Consumer events from the topic"""
        trn = self.consumer.poll(timeout=1.0)
        
        if trn is None:
            return None
        if trn.error():
            raise Exception(f"There is an error in the incoming trnsaction: {trn.error()}")
    
        try:
            value = json.loads(trn.value().decode("utf-8")) #trn.value()
            key = trn.key().decode('utf-8') if trn.key() else None
            
            return key, value #this is what is passed to the egine for comparison with the rules
        except Exception as e:
            logger.info(f"Failed to get the transaction{trn.topic()}[{trn.partition()}] offset {trn.offset()}: {e}")
            raise

    def close(self):
        """Close consumer connection"""

        self.consumer.close()
        logger.info("Consumer client connection closed.")
