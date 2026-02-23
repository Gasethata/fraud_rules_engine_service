import pytest
from fraud_engine.engine import FraudEngine
from datetime import datetime

pytestmark = pytest.mark.unit

# ---------------------------
# Mock Repository
# ---------------------------
class MockRepo:
    def __init__(self):
        self.saved_transactions = []
        self.saved_flags = []

    def trans_frequency(self, cc_num, trans_time, minutes):
        return 5  # controlled value for velocity tests

    def get_last_n_amounts(self, cc_num, n):
        return [10, 10, 10, 10, 10]

    def save_transaction(self, transaction):
        self.saved_transactions.append(transaction)

    def save_flag(self, transaction_id, rule_ids, reasons):
        self.saved_flags.append((transaction_id, rule_ids, reasons))


# ---------------------------
# Common Transaction Fixture
# ---------------------------
@pytest.fixture
def base_transaction():
    return {
        "trans_num": "abc123",
        "trans_date_trans_time": "2020-12-31 10:00:00",
        "cc_num": "1111",
        "merchant": "Test Merchant",
        "category": "entertainment",
        "amt": 3000,
        "first": "John",
        "last": "Doe"
    }

# ---------------------------
# Test: High Absolute Amount
# ---------------------------
def test_high_absolute_amount(base_transaction):
    rule = {
        "id": 1,
        "description": "High absolute amount",
        "field": "amt",
        "condition": ">",
        "threshold": 2000,
        "threshold_window": None,
        "modifier": None
    }

    engine = FraudEngine([rule], MockRepo())
    triggered = engine.process(base_transaction)

    assert len(triggered) == 1


# ---------------------------
# Test: Hour Not Between
# ---------------------------
def test_hour_not_between():
    rule = {
        "id": 2,
        "description": "Odd transaction hour",
        "field": "hour",
        "condition": "hour_not_between",
        "threshold": 8,
        "threshold_window": 19,
        "modifier": None
    }

    transaction = {
        "trans_num": "t2",
        "trans_date_trans_time": "2020-12-31 02:00:00",
        "cc_num": "1111",
        "merchant": "Test",
        "category": "misc",
        "amt": 100
    }

    engine = FraudEngine([rule], MockRepo())
    triggered = engine.process(transaction)

    assert len(triggered) == 1


# ---------------------------
# Test: Velocity Rule
# ---------------------------
def test_trans_frequency(base_transaction):
    rule = {
        "id": 3,
        "description": "Rapid frequency",
        "field": "cc_num",
        "condition": "trans_frequency",
        "threshold": 5,
        "threshold_window": 10,
        "modifier": None
    }

    engine = FraudEngine([rule], MockRepo())

    triggered = engine.process(base_transaction)

    # count_last_minutes returns 4
    # so 4 previous + current = 5
    # should trigger if using (count + 1) >= threshold
    print(f"Triggered: {triggered}")
    assert len(triggered) == 1


# ---------------------------
# Test: Average Rule
# ---------------------------
def test_average_rule(base_transaction):
    rule = {
        "id": 4,
        "description": "Sudden increase vs last 5 avg",
        "field": "amt",
        "condition": "avg_last_n",
        "threshold": 2,
        "threshold_window": 5,
        "modifier": None
    }

    engine = FraudEngine([rule], MockRepo())

    # average = 10
    # threshold multiplier = 2
    # 2 * 10 = 20
    # amt = 3000 → should trigger
    triggered = engine.process(base_transaction)

    assert len(triggered) == 1


# ---------------------------
# Test: No Trigger Case
# ---------------------------
def test_no_trigger():
    rule = {
        "id": 5,
        "description": "High absolute amount",
        "field": "amt",
        "condition": ">",
        "threshold": 5000,
        "threshold_window": None,
        "modifier": None
    }

    transaction = {
        "trans_num": "t3",
        "trans_date_trans_time": "2020-12-31 10:00:00",
        "cc_num": "1111",
        "merchant": "Test",
        "category": "misc",
        "amt": 100
    }

    engine = FraudEngine([rule], MockRepo())
    triggered = engine.process(transaction)

    assert len(triggered) == 0

# ---------------------------
# Test: Weekend High Amount
# ---------------------------
def test_weekend_high_amount_positive():
    rule = {
        "id": 5,
        "description": "Weekend high amount",
        "field": "amt",
        "condition": ">",
        "threshold": 1000,
        "threshold_window": None,
        "modifier": "weekend"
    }

    transaction = {
        "trans_num": "t5",
        "trans_date_trans_time": "2020-12-26 14:00:00",  # Saturday
        "cc_num": "1111",
        "merchant": "Test",
        "category": "misc",
        "amt": 1500
    }

    engine = FraudEngine([rule], MockRepo())
    triggered = engine.process(transaction)

    assert len(triggered) == 1

# ---------------------------
# Test: High-risk Category
# ---------------------------
def test_high_risk_category_positive():
    rule = {
        "id": 4,
        "description": "High-risk category",
        "field": "category",
        "condition": "IN",
        "threshold": None,
        "threshold_window": None,
        "modifier": "high_risk_categories"
    }

    transaction = {
        "trans_num": "t4",
        "trans_date_trans_time": "2020-12-31 10:00:00",
        "cc_num": "1111",
        "merchant": "Test",
        "category": "entertainment",
        "amt": 100
    }

    engine = FraudEngine([rule], MockRepo())
    triggered = engine.process(transaction)

    assert len(triggered) == 1