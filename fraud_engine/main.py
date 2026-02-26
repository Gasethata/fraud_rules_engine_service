"""
Main script that runs the Fraud Engine
    - Get the repository (database queries) results
    - Get the evaluation config fom engine
    - Run the evaluation
"""
import sys, os, logging
from psycopg2.extras import RealDictCursor
from kafka_consumer import TransactionConsumer
from repository import Repository
from engine import FraudEngine

logger = logging.getLogger(__name__)

def main():

    host = os.getenv("HOST","broke host")
    port = os.getenv("PORT","broker port")
    topic = os.getenv("TOPIC","opic name")  
    db = os.getenv("POSTGRES_DB","database name")  
    db_user = os.getenv("POSTGRES_USER","db username")
    db_password = os.getenv("POSTGRES_PASSWORD","db username")

    db_config = {
        "host": host,
        "database": db,
        "user": db_user,
        "password": db_password,
        }

    consumer = TransactionConsumer(host, port, topic)
    consumer.subscribe()

    repo = Repository(db_config)
    rules = repo.fetch_active_rules()
    engine = FraudEngine(rules, repo)

    # get the transaction from the consumer and ran it through the rules
    try:
        while True:
            data = consumer.poll()
            if not data:
               continue    
            key, value = data
            transaction = value 

            triggered = engine.process(transaction)

            if triggered:
                
                logger.info(f"Transaction flagged by rules")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    
    sys.exit(main())
