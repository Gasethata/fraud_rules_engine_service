"""
Unit tests for the FraudEngine.
    - Tests individual rule operators in isolation.
    - MockRepo replaces all database calls  no real DB required.
"""

import pytest
import logging
from fraud_engine.engine import FraudEngine

logger = logging.getLogger(__name__)
pytestmark = pytest.mark.unit


class MockRepo:
    """In-memory mock of the Repository class.
       Captures what the engine writes so tests can assert on it."""

    def __init__(self):
        self.saved_transactions = []
        self.saved_flags = []

    def trans_frequency(self, cc_num, trans_time, minutes):
        """Always return 5 - simulates a card with recent transaction history."""
        return 5

    def get_last_n_amounts(self, cc_num, n):
        """Return a flat list of low amounts - any high transaction will beat the average."""
        return [10, 10, 10, 10, 10]

    def save_transaction(self, transaction):
        """Capture non-flagged transaction saves."""
        self.saved_transactions.append(transaction)

    def save_transaction_and_flag(self, transaction, transaction_id, rule_ids, reasons):
        """Captures the atomic save when rules trigger.
           Both the transaction and the flag are recorded together."""
        self.saved_transactions.append(transaction)
        self.saved_flags.append((transaction_id, rule_ids, reasons))


@pytest.fixture
def base_transaction():
    """Standard transaction used across multiple tests."""
    return {
        "trans_num": "abc123",
        "trans_date_trans_time": "2020-12-31 10:00:00",
        "cc_num": "1111",
        "merchant": "Test Merchant",
        "category": "entertainment",
        "amt": 3000,
        "first": "John",
        "last": "Doe",
    }


def test_high_absolute_amount(base_transaction):
    """Amount exceeds the absolute threshold - should trigger."""
    rule = {
        "id": 1,
        "description": "High absolute amount",
        "field": "amt",
        "condition": ">",
        "threshold": 2000,
        "threshold_window": None,
        "modifier": None,
    }
    engine = FraudEngine([rule], MockRepo())
    triggered = engine.process(base_transaction)
    assert len(triggered) == 1


def test_hour_not_between():
    """Transaction at 02:00 falls outside the allowed 08:00–19:00 window - should trigger."""
    rule = {
        "id": 2,
        "description": "Odd transaction hour",
        "field": "hour",
        "condition": "hour_not_between",
        "threshold": 8,
        "threshold_window": 19,
        "modifier": None,
    }
    transaction = {
        "trans_num": "t2",
        "trans_date_trans_time": "2020-12-31 02:00:00",
        "cc_num": "1111",
        "merchant": "Test",
        "category": "misc",
        "amt": 100,
    }
    engine = FraudEngine([rule], MockRepo())
    triggered = engine.process(transaction)
    assert len(triggered) == 1


def test_trans_frequency(base_transaction):
    """MockRepo returns 5 prior transactions; (5+1) > threshold of 5 - should trigger."""
    rule = {
        "id": 3,
        "description": "Rapid frequency",
        "field": "cc_num",
        "condition": "trans_frequency",
        "threshold": 5,
        "threshold_window": 10,
        "modifier": None,
    }
    engine = FraudEngine([rule], MockRepo())
    triggered = engine.process(base_transaction)
    assert len(triggered) == 1


def test_average_rule(base_transaction):
    """Current amount (3000) exceeds 2x average of last 5 (avg=10, threshold=20) - should trigger."""
    rule = {
        "id": 4,
        "description": "Sudden increase vs last 5 avg",
        "field": "amt",
        "condition": "avg_last_n",
        "threshold": 2,
        "threshold_window": 5,
        "modifier": None,
    }
    engine = FraudEngine([rule], MockRepo())
    triggered = engine.process(base_transaction)
    assert len(triggered) == 1


def test_no_trigger():
    """Amount (100) is below threshold (5000) - should not trigger."""
    rule = {
        "id": 5,
        "description": "High absolute amount",
        "field": "amt",
        "condition": ">",
        "threshold": 5000,
        "threshold_window": None,
        "modifier": None,
    }
    transaction = {
        "trans_num": "t3",
        "trans_date_trans_time": "2020-12-31 10:00:00",
        "cc_num": "1111",
        "merchant": "Test",
        "category": "misc",
        "amt": 100,
    }
    engine = FraudEngine([rule], MockRepo())
    triggered = engine.process(transaction)
    assert len(triggered) == 0


def test_weekend_high_amount_positive():
    """High amount on a Saturday - weekend modifier means it should trigger."""
    rule = {
        "id": 6,
        "description": "Weekend high amount",
        "field": "amt",
        "condition": ">",
        "threshold": 1000,
        "threshold_window": None,
        "modifier": "weekend",
    }
    transaction = {
        "trans_num": "t5",
        "trans_date_trans_time": "2020-12-26 14:00:00",  # Saturday
        "cc_num": "1111",
        "merchant": "Test",
        "category": "misc",
        "amt": 1500,
    }
    engine = FraudEngine([rule], MockRepo())
    triggered = engine.process(transaction)
    assert len(triggered) == 1


def test_high_risk_category_positive():
    """Category 'entertainment' is in the high_risk_categories set - should trigger."""
    rule = {
        "id": 7,
        "description": "High-risk category",
        "field": "category",
        "condition": "IN",
        "threshold": None,
        "threshold_window": None,
        "modifier": "high_risk_categories",
    }
    transaction = {
        "trans_num": "t4",
        "trans_date_trans_time": "2020-12-31 10:00:00",
        "cc_num": "1111",
        "merchant": "Test",
        "category": "entertainment",
        "amt": 100,
    }
    engine = FraudEngine([rule], MockRepo())
    triggered = engine.process(transaction)
    assert len(triggered) == 1


def test_atomic_save_on_flag(base_transaction):
    """
    When rules trigger, both the transaction and the flag must be saved together.
    Verifies the atomic save_transaction_and_flag is called, not separate methods.
    """
    rule = {
        "id": 1,
        "description": "High absolute amount",
        "field": "amt",
        "condition": ">",
        "threshold": 2000,
        "threshold_window": None,
        "modifier": None,
    }
    repo = MockRepo()
    engine = FraudEngine([rule], repo)
    engine.process(base_transaction)

    assert len(repo.saved_transactions) == 1
    assert len(repo.saved_flags) == 1
    assert repo.saved_flags[0][0] == base_transaction["trans_num"]


def test_no_flag_saved_when_no_trigger():
    """When no rules trigger, only the transaction is saved - no flag record."""
    rule = {
        "id": 1,
        "description": "High absolute amount",
        "field": "amt",
        "condition": ">",
        "threshold": 9999,
        "threshold_window": None,
        "modifier": None,
    }
    transaction = {
        "trans_num": "t_safe",
        "trans_date_trans_time": "2020-12-31 10:00:00",
        "cc_num": "2222",
        "merchant": "Safe Merchant",
        "category": "grocery",
        "amt": 50,
    }
    repo = MockRepo()
    engine = FraudEngine([rule], repo)
    engine.process(transaction)

    assert len(repo.saved_transactions) == 1
    assert len(repo.saved_flags) == 0