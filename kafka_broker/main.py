
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

    host = '192.168.229.138' #os.getenv("HOST","0000")
    port = '9092'  #os.getenv("PORT","0000")
    controller_port = os.getenv("CONTROLLER_PORT","0000")
    kafka_home = os.getenv("KAFKA_HOME","home/kafka/path")
    node_id = os.getenv("NODE_ID","1110")
    config_file = os.getenv("BROKER_PROPERTIES_CONFIG_FILE","home/kafka/path")
    topic_name = 'transactions' #os.getenv("TOPIC_NAME","default-topic")
    topic_on_startup = os.getenv("CREATE_TOPIC_ON_STARTUP","false").lower() == "true"
    

    admin_client_config = os.getenv("ADMIN_CLIENT_PROPERTIES_CONFIG_FILE","home/kafka/path")
    jaas_config = os.getenv("JAAS_FILE","home/kafka/path")

    admin_user = 'admin' #os.getenv("KAFKA_ADMIN_USER","user")
    admin_pass = 'admin' #os.getenv("KAFKA_ADMIN_PASSWORD","password")

    time.sleep(15)

    create_topic(topic_name, host, port, admin_user, admin_pass)


if __name__ == "__main__": 

    sys.exit(main())

