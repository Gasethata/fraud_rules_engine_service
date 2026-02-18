#!/user/bin/env python3

import os , sys
import logging
import subprocess, time
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException, KafkaError

logger = logging.getLogger(__name__)

def create_topic(topic_name: str, host: str, port: str, admin_user: str, admin_pass: str):


    conf = {
        "bootstrap.servers": f"{host}:{port}",
        "security.protocol": "PLAINTEXT",
    }

    admin_client = AdminClient(conf)

    while True:
        try:
            admin_client.list_topics(timeout=5)
            print("Broker is ready for connections...")
            break
        except KafkaException:
            print("Waiting for broker to be up...")
            time.sleep(2)

    topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)   
    futures = admin_client.create_topics([topic])

    for topic, future in futures.items():
        try:
            future.result()
            print(f"Topic {topic} created")
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"Topic {topic_name} already exists")
            else:
                raise

    


