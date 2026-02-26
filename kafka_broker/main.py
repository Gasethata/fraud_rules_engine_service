"""
main script to run the topic container
    - uses topic.py script to create the actual topic.
    - exits after creating the topic
"""

import os
import sys, time, logging
from pathlib import Path
from topic import create_topic

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s::: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%s',
    handlers=[
        logging.StreamHandler(sys.stdout)
        ]
    )   

logger = logging.getLogger(__name__)

def main():

    host = os.getenv("HOST","0000")
    port = os.getenv("PORT","0000")
    topic_name = os.getenv("TOPIC","default-topic")

    create_topic(topic_name, host, port)


if __name__ == "__main__": 

    sys.exit(main())

