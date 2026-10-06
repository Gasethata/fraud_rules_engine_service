"""
Integration tests for the full fraud detection pipeline.

Tests real service interactions — requires the stack to be running:
    docker compose up --scale transaction_producer=0 --build -d

What is tested end-to-end:
    - Kafka topic exists and is reachable
    - Fraud engine consumes a transaction, evaluates rules, writes to DB
    - API authenticates via JWT and returns flagged transactions
    - Pagination metadata is present on the /v1/flagged response
    - Health and readiness endpoints respond correctly

Run with:
    pytest -m integration -v
"""

import os
import time
import json
import pytest
import psycopg2
import requests
from psycopg2.extras import RealDictCursor
from confluent_kafka import Producer, Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

pytestmark = pytest.mark.integration

# -------------------------------------------------------
# Config from environment
# -------------------------------------------------------
HOST = "localhost"
PORT = os.getenv("PORT", "9092")
TOPIC = os.getenv("TOPIC", "transactions")
API_URL = "http://localhost:8000"

ADMIN_USERNAME = os.getenv("ADMIN_USERNAME")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD")

DB_CONFIG = {
    "host": "localhost",
    "database": os.getenv("POSTGRES_DB", "frauddb"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "cursor_factory": RealDictCursor,
}

SAMPLE_TRANSACTION = {
    "trans_num": "intg-test-001",
    "trans_date_trans_time": "2020-12-26 02:00:00",  # Saturday + odd hour - triggers rules
    "cc_num": "9999999999999999",
    "merchant": "Test Merchant",
    "category": "entertainment",
    "amt": 9999.99,
    "first": "Integration",
    "last": "Test",
}


# -------------------------------------------------------
# Helpers
# -------------------------------------------------------
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_api_token():
    """Login and return a valid JWT token."""
    response = requests.post(
        f"{API_URL}/v1/login",
        json={"username": ADMIN_USERNAME, "password": ADMIN_PASSWORD},
        timeout=10,
    )
    assert response.status_code == 200, f"Login failed: {response.text}"
    return response.json()["access_token"]


def publish_transaction(transaction: dict):
    """Publish a single transaction to the Kafka topic."""
    producer = Producer({"bootstrap.servers": f"localhost:{PORT}", "acks": 1})
    producer.produce(
        topic=TOPIC,
        key=transaction["trans_num"],
        value=json.dumps(transaction).encode("utf-8"),
    )
    producer.flush()


def wait_for_flag(transaction_id: str, timeout: int = 30) -> bool:
    """
    Poll the DB until the transaction appears in flagged_transactions
    or the timeout is reached. Returns True if found.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT id FROM flagged_transactions WHERE transaction_id = %s",
                    (transaction_id,),
                )
                if cur.fetchone():
                    return True
        time.sleep(1)
    return False


# -------------------------------------------------------
# Tests
# -------------------------------------------------------
def test_health_liveness():
    """Liveness probe must return 200 with status=alive."""
    response = requests.get(f"{API_URL}/health/live", timeout=5)
    assert response.status_code == 200
    assert response.json()["status"] == "alive"


def test_health_readiness():
    """Readiness probe must return 200 and confirm DB is connected."""
    response = requests.get(f"{API_URL}/health/ready", timeout=5)
    assert response.status_code == 200
    assert response.json()["database"] == "connected"


def test_metrics_endpoint():
    """Metrics endpoint must return Prometheus-format text."""
    response = requests.get(f"{API_URL}/metrics", timeout=5)
    assert response.status_code == 200
    assert "api_requests_total" in response.text


def test_login_returns_token():
    """Valid credentials must return a JWT access token."""
    response = requests.post(
        f"{API_URL}/v1/login",
        json={"username": ADMIN_USERNAME, "password": ADMIN_PASSWORD},
        timeout=10,
    )
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"


def test_login_rejects_bad_credentials():
    """Invalid credentials must return 401."""
    response = requests.post(
        f"{API_URL}/v1/login",
        json={"username": "wrong", "password": "wrong"},
        timeout=10,
    )
    assert response.status_code == 401


def test_flagged_requires_auth():
    """Accessing /v1/flagged without a token must return 401."""
    response = requests.get(f"{API_URL}/v1/flagged", timeout=5)
    assert response.status_code == 401


def test_full_pipeline_transaction_flagged():
    """
    End-to-end test: publish a high-risk transaction - wait for the engine
    to consume and flag it - verify it appears in the DB and via the API.

    The transaction is designed to trigger multiple rules:
    - High absolute amount (9999.99)
    - Odd hour (02:00)
    - Weekend (Saturday)
    - High-risk category (entertainment)
    """
    # Clean up any previous run of this test transaction
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM flagged_transactions WHERE transaction_id = %s",
                (SAMPLE_TRANSACTION["trans_num"],),
            )
            cur.execute(
                "DELETE FROM transactions WHERE transaction_id = %s",
                (SAMPLE_TRANSACTION["trans_num"],),
            )
        conn.commit()

    # Publish the transaction to Kafka
    publish_transaction(SAMPLE_TRANSACTION)

    # Wait for the fraud engine to process and flag it
    flagged = wait_for_flag(SAMPLE_TRANSACTION["trans_num"], timeout=30)
    assert flagged, "Transaction was not flagged within 30 seconds"

    # Verify it is retrievable via the API
    token = get_api_token()
    response = requests.get(
        f"{API_URL}/v1/flagged/{SAMPLE_TRANSACTION['trans_num']}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    assert response.status_code == 200
    data = response.json()
    assert data["transaction_id"] == SAMPLE_TRANSACTION["trans_num"]
    assert isinstance(data["reason"], list)
    assert len(data["reason"]) > 0


def test_flagged_list_returns_pagination_metadata():
    """
    GET /v1/flagged must return a paginated response with total, limit,
    offset, and items - not a flat list.
    """
    token = get_api_token()
    response = requests.get(
        f"{API_URL}/v1/flagged?limit=10&offset=0",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    assert response.status_code == 200
    data = response.json()

    # Pagination metadata must be present
    assert "total" in data, "Response missing 'total' field"
    assert "limit" in data, "Response missing 'limit' field"
    assert "offset" in data, "Response missing 'offset' field"
    assert "items" in data, "Response missing 'items' field"
    assert isinstance(data["items"], list)
    assert data["limit"] == 10
    assert data["offset"] == 0


def test_flagged_list_pagination_offset():
    """Offset pagination must return the correct window of results."""
    token = get_api_token()

    page1 = requests.get(
        f"{API_URL}/v1/flagged?limit=1&offset=0",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    ).json()

    page2 = requests.get(
        f"{API_URL}/v1/flagged?limit=1&offset=1",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    ).json()

    # If there are at least 2 records, the two pages must differ
    if page1["total"] >= 2:
        assert page1["items"][0]["id"] != page2["items"][0]["id"]


def test_correlation_id_header_present():
    """
    Every response must include the X-Correlation-ID header.
    This confirms the observability middleware is active.
    """
    response = requests.get(f"{API_URL}/health/live", timeout=5)
    assert "x-correlation-id" in response.headers


def test_correlation_id_propagated():
    """If the client sends an X-Correlation-ID, the server must echo it back."""
    client_id = "test-correlation-abc123"
    response = requests.get(
        f"{API_URL}/health/live",
        headers={"X-Correlation-ID": client_id},
        timeout=5,
    )
    assert response.headers.get("x-correlation-id") == client_id