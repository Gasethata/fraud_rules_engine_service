"""
Main entry point for the Fraud Engine consumer service.

Responsibilities:
    - Connect to Kafka and PostgreSQL
    - Load active fraud rules from the database
    - Poll Kafka for transactions and run them through the engine
    - Handle SIGTERM and SIGINT for graceful shutdown
    - Structured JSON logging throughout
"""

import sys
import os
import signal
import logging
import json
import time
import threading
from datetime import datetime

from psycopg2.extras import RealDictCursor
from kafka_consumer import TransactionConsumer
from repository import Repository
from engine import FraudEngine

# -------------------------------------------------------
# Structured JSON logging
# -------------------------------------------------------
class StructuredFormatter(logging.Formatter):
    def format(self, record):
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "service": "fraud-engine",
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

def reload_rules(engine, repo, shutdown_requested, interval=30):
    """Background thread that reloads fraud rules from the DB every 30 seconds.
    Allows rule changes to take effect without restarting the consumer."""
    def _reload():
        while not shutdown_requested["value"]:
            time.sleep(interval)
            try:
                new_rules = repo.fetch_active_rules()
                engine.rules = new_rules
                logger.info("Fraud rules reloaded", extra={"rule_count": len(new_rules)})
            except Exception as e:
                logger.error("Failed to reload rules", extra={"error": str(e)})

    thread = threading.Thread(target=_reload, daemon=True)
    thread.start()

def main():
    host = os.getenv("HOST")
    port = os.getenv("PORT")
    topic = os.getenv("TOPIC")

    db_config = {
        "host": os.getenv("POSTGRES_HOST"),
        "database": os.getenv("POSTGRES_DB"),
        "user": os.getenv("POSTGRES_USER"),
        "password": os.getenv("POSTGRES_PASSWORD"),
        "connect_timeout": 5,
        "options": "-c statement_timeout=30000",
    }

    logger.info("Initialising fraud engine consumer")

    consumer = TransactionConsumer(host, port, topic)
    consumer.subscribe()

    repo = Repository(db_config)
    rules = repo.fetch_active_rules()
    logger.info("Active rules loaded", extra={"rule_count": len(rules)})

    engine = FraudEngine(rules, repo)

    # Graceful shutdown handler
    shutdown_requested = {"value": False}

    reload_rules(engine, repo, shutdown_requested, interval=30)

    def handle_shutdown(signum, frame):
        logger.info("Shutdown signal received, finishing current transaction before exit")
        shutdown_requested["value"] = True

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    # -------------------------------------------------------
    # Main processing loop
    # -------------------------------------------------------
    try:
        while not shutdown_requested["value"]:
            data = consumer.poll()
            if not data:
                continue

            key, transaction = data

            try:
                triggered = engine.process(transaction)
                if triggered:
                    logger.warning(
                        "Transaction flagged",
                        extra={
                            "transaction_id": transaction.get("trans_num"),
                            "rules_triggered": len(triggered),
                        },
                    )
                else:
                    logger.info(
                        "Transaction processed — no rules triggered",
                        extra={"transaction_id": transaction.get("trans_num")},
                    )
            except Exception as e:
                # Log and continue — one bad transaction should not kill the loop
                logger.error(
                    "Failed to process transaction",
                    extra={
                        "transaction_id": transaction.get("trans_num"),
                        "error": str(e),
                    },
                )

    finally:
        logger.info("Closing Kafka consumer")
        consumer.close()
        logger.info("Fraud engine shut down cleanly")


if __name__ == "__main__":
    sys.exit(main())