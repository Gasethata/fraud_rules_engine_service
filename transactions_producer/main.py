"""
main script to run the producer container
    - uses kafka_producer.py script to publish the transactions into Kafka.
"""

import pandas as pd
import time, sys, os, logging
from kafka_producer import TransactionProducer

# JSON logging
class StructuredFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "service": "transaction-producer",
            "message": record.getMessage(),
            "logger": record.name,
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(StructuredFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler])
logger = logging.getLogger(__name__)

def main():

    host = os.getenv("HOST","broke host")
    port = os.getenv("PORT","broker port")
    topic = os.getenv("TOPIC","topic name")
    transactions = os.getenv("TRANSACTIONS_SIMULATION_FILE","transactions file")
    delay = float(os.getenv("TRANSACTION_DELAY_SIMULATION","transactions delay"))
    

    producer = TransactionProducer(host, port, topic)
    df = pd.read_csv(transactions, index_col=0)

    # read the transactions from the csv file row by row
    for _, row in df.iterrows():
        transaction = row.to_dict()
        producer.publish(key=str(transaction.get("trans_num", "")), value=transaction)
        time.sleep(delay)

    producer.close()
       

if __name__ == "__main__":
    
    sys.exit(main())
