from confluent_kafka import Consumer
import json, sys
import logging

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
    def __init__(self, host: int, port: int, topic: str):
        self.topic = topic
        self.consumer = Consumer({'bootstrap.servers': f"{host}:{port}",
                                  'group.id': 'consumer-group-1',
                                  'auto.offset.reset': 'earliest'})

    def subscribe(self):
        try:
            self.consumer.subscribe([self.topic])
            logger.info("Kafka consumer subscribed successfully.")
        except Exception as e:
            logger.info(f"Kafka consumer failed to subscribed. {e}")
            raise

    def poll(self):
        trn = self.consumer.poll(timeout=1.0)
        
        if trn is None:
            return None
        if trn.error():
            raise Exception(f"There is an error in the incoming trnsaction: {trn.error()}")
    
        try:
            value = json.loads(trn.value().decode("utf-8")) #trn.value()
            key = trn.key().decode('utf-8') if trn.key() else None
            
            return key, value #this is what I will pass to the egine for comparison with the rules
        except Exception as e:
            logger.info(f"Failed to get the transaction{trn.topic()}[{trn.partition()}] offset {trn.offset()}: {e}")
            raise

    def close(self):
        """Close consumer connection"""

        self.consumer.close()
        logger.info("Consumer client connection closed.")
