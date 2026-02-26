"""
Integration tests 
    - Use pytest to test Engine services integration
    - Uses the actual Engine services
    - Uses testdata.csv for test transactions
"""

import json, os
import time
import psycopg2
import pytest
from confluent_kafka import Producer
from contextlib import contextmanager
from dotenv import load_dotenv

load_dotenv()

pytestmark = pytest.mark.integration


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


@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = None
    # Connect to the db or raise an error if not connected
    try:
        conn = psycopg2.connect(**db_config)
        yield conn
    except psycopg2.Error as e:
        print(f"Database error: {e}")
        raise
    finally:
        if conn:
            conn.close()

def reset_db():
    """Database table reset"""
    with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "TRUNCATE transactions, flagged_transactions RESTART IDENTITY CASCADE;"
                )
                conn.commit()
                

def produce_transaction(txn):
    """Produce trasaction to Kafka"""
    producer = Producer({"bootstrap.servers": f"{host}:{port}"})
    producer.produce(topic, json.dumps(txn).encode("utf-8"))
    producer.flush()
    time.sleep(2)  # allow consumer to process


def get_flag_count():
    """Get number of flagged transactions"""
    with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM flagged_transactions;")
                count = cur.fetchone()[0]
                return count

@pytest.fixture(autouse=True)
def clean_db():
    """Clean the database"""
    reset_db()

def test_high_absolute_amount():
    """Test high amount transaction"""
    txn = {
        "trans_num": "int1",
        "trans_date_trans_time": "2020-12-31 10:00:00",
        "cc_num": "1111",
        "merchant": "Test",
        "category": "misc",
        "amt": 3000}

    produce_transaction(txn)

    assert get_flag_count() == 1

def test_weekend_high_amount():
    """Test weekend high amount"""
    txn = {
        "trans_num": "int2",
        "trans_date_trans_time": "2020-12-26 14:00:00",  # Saturday
        "cc_num": "1111",
        "merchant": "Test",
        "category": "misc",
        "amt": 1500}

    produce_transaction(txn)

    assert get_flag_count() == 1

def test_odd_transaction_hour():
    """Test transaction hour"""
    txn = {
        "trans_num": "int3",
        "trans_date_trans_time": "2020-12-31 02:00:00",
        "cc_num": "1111",
        "merchant": "Test",
        "category": "misc",
        "amt": 100}

    produce_transaction(txn)

    assert get_flag_count() == 1

def test_high_risk_category():
    """Test high risk category"""
    txn = {
        "trans_num": "int4",
        "trans_date_trans_time": "2020-12-31 12:00:00",
        "cc_num": "1111",
        "merchant": "Test",
        "category": "entertainment",
        "amt": 100}

    produce_transaction(txn)

    assert get_flag_count() == 1

def test_avg_last_n_rule():
    """Test avarage n transactions"""
    base_txns = []

    # get history
    for i in range(5):
        base_txns.append({
            "trans_num": f"hist{i}",
            "trans_date_trans_time": "2020-12-31 10:00:00",
            "cc_num": "9999",
            "merchant": "Test",
            "category": "misc",
            "amt": 100})

    for txn in base_txns:
        produce_transaction(txn)

    spike_txn = {
        "trans_num": "spike1",
        "trans_date_trans_time": "2020-12-31 10:05:00",
        "cc_num": "9999",
        "merchant": "Test",
        "category": "misc",
        "amt": 500}

    produce_transaction(spike_txn)

    assert get_flag_count() == 1

def test_trans_frequency_rule():
    """Test transaction frequency"""
    for i in range(6):
        txn = {
            "trans_num": f"freq{i}",
            "trans_date_trans_time": "2020-12-31 10:00:00",
            "cc_num": "2222",
            "merchant": "Test",
            "category": "misc",
            "amt": 100}
        produce_transaction(txn)

    assert get_flag_count() == 1