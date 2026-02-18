import pandas as pd
import time, sys, os, logging
from kafka_producer import TransactionProducer

logger = logging.getLogger(__name__)

def main():

    host = os.getenv("HOST","broke host")
    port = os.getenv("PORT","broker port")
    topic = os.getenv("TOPIC","opic name")
    transactions = os.getenv("TRANSACTIONS_SIMULATION_FILE","transactions file")
    delay = float(os.getenv("TRANSACTION_DELAY_SIMULATION","transactions delay"))
    

    producer = TransactionProducer(host, port, topic)
    df = pd.read_csv(transactions)

    for _, row in df.iterrows():
        transaction = row.to_dict()
        #transaction["transaction_id"] = transaction["trans_num"]
        producer.publish(key=str(transaction.get("TransactionID", "")), value=transaction)
        #producer.publish(key=transaction["transaction_id"],value=transaction)
        time.sleep(delay)

    producer.close()
       

if __name__ == "__main__":
    
    sys.exit(main())
