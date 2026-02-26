"""
Kafka topic creation logic inside the topic container.
    - ran by main.py
    - connects to the kafka broker container
"""

import logging
import time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError

logger = logging.getLogger(__name__)

def create_topic(topic_name: str, host: str, port: str):
    """Connect to Kafka broke and create a topic"""

    conf = {
        "bootstrap.servers": f"{host}:{port}",
        "security.protocol": "PLAINTEXT",
    }

    admin_client = AdminClient(conf)

    # check if the broker is ready
    while True:
        try:
            admin_client.list_topics(timeout=5)
            logger.info("Broker is ready for connections...")
            break
        except KafkaException:
            logger.info("Waiting for broker to be up...")
            time.sleep(2)
     # create a new topic
    topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)   
    futures = admin_client.create_topics([topic])

    for topic, future in futures.items():
        try:
            future.result()
            logger.info(f"Topic {topic} created")
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                logger.error(f"Topic {topic_name} already exists")
            else:
                raise

    


