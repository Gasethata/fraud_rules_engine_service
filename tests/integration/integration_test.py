import json
import time
import psycopg2
import pytest
from confluent_kafka import Producer

pytestmark = pytest.mark.integration

# -------------------------
# CONFIG
# -------------------------

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "transactions"

DB_CONFIG = {
    "dbname": "frauddb",
    "user": "postgres",
    "password": "postgres",
    "host": "localhost",
    "port": 5432
}


# -------------------------
# HELPERS
# -------------------------

def reset_db():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        "TRUNCATE transactions, flagged_transactions RESTART IDENTITY CASCADE;"
    )
    conn.commit()
    cur.close()
    conn.close()


def produce_transaction(txn):
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    producer.produce(TOPIC, json.dumps(txn).encode("utf-8"))
    producer.flush()
    time.sleep(2)  # allow consumer to process


def get_flag_count():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM flagged_transactions;")
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count


# -------------------------
# TESTS
# -------------------------

@pytest.fixture(autouse=True)
def clean_db():
    reset_db()


# High absolute amount
def test_high_absolute_amount():
    txn = {
        "trans_num": "int1",
        "trans_date_trans_time": "2020-12-31 10:00:00",
        "cc_num": "1111",
        "merchant": "Test",
        "category": "misc",
        "amt": 3000}

    produce_transaction(txn)

    assert get_flag_count() == 1


# Weekend high amount
def test_weekend_high_amount():
    txn = {
        "trans_num": "int2",
        "trans_date_trans_time": "2020-12-26 14:00:00",  # Saturday
        "cc_num": "1111",
        "merchant": "Test",
        "category": "misc",
        "amt": 1500}

    produce_transaction(txn)

    assert get_flag_count() == 1


# Odd transaction hour
def test_odd_transaction_hour():
    txn = {
        "trans_num": "int3",
        "trans_date_trans_time": "2020-12-31 02:00:00",
        "cc_num": "1111",
        "merchant": "Test",
        "category": "misc",
        "amt": 100}

    produce_transaction(txn)

    assert get_flag_count() == 1


# High-risk category
def test_high_risk_category():
    txn = {
        "trans_num": "int4",
        "trans_date_trans_time": "2020-12-31 12:00:00",
        "cc_num": "1111",
        "merchant": "Test",
        "category": "entertainment",
        "amt": 100}

    produce_transaction(txn)

    assert get_flag_count() == 1


# Sudden increase vs last 5 avg
def test_avg_last_n_rule():
    base_txns = []

    # baseline history
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


# Rapid transaction frequency
def test_trans_frequency_rule():
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