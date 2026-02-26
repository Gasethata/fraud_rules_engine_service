"""
Unit tests coverage
    - Use pytest to test inividual units of the Engine
    - Mocks the database queries that would be ran on the Engine
"""

import pytest, logging
from fraud_engine.engine import FraudEngine
from datetime import datetime

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.unit

# Mock Repository
class MockRepo:
    def __init__(self):
        """Initialization"""
        self.saved_transactions = []
        self.saved_flags = []

    def trans_frequency(self, cc_num, trans_time, minutes):
        """Mimic 5 transactions"""
        return 5  

    def get_last_n_amounts(self, cc_num, n):
        """Mimic amounts for last n transaction"""
        return [10, 10, 10, 10, 10]

    def save_transaction(self, transaction):
        """Mimic persist transactions"""
        self.saved_transactions.append(transaction)

    def save_flag(self, transaction_id, rule_ids, reasons):
        """Mimic flagging the fraud transaction"""
        self.saved_flags.append((transaction_id, rule_ids, reasons))

@pytest.fixture
def base_transaction():
    """The base transaction to be used for the tests"""
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

def test_high_absolute_amount(base_transaction):
    """Test the high amount rule"""
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

def test_hour_not_between():
    """Test the transaction time rule"""
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

def test_trans_frequency(base_transaction):
    """Test the transaction frequency rule"""
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
    # the rule:  (count + 1) >= threshold
    logger.info(f"Triggered: {triggered}")
    assert len(triggered) == 1

def test_average_rule(base_transaction):
    """Test the average n transactions"""
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
    # triggered: amt = 3000
    triggered = engine.process(base_transaction)

    assert len(triggered) == 1

def test_no_trigger():
    """Test non fraud transaction"""
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

def test_weekend_high_amount_positive():
    """Test high amount on weeken rule"""
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

def test_high_risk_category_positive():
    """Test high risk merchant category"""
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