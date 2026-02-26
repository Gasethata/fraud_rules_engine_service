import ast
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class FraudEngine:
    """Fraud engine class"""
    def __init__(self, rules, repository):
        """Initialization"""
        self.rules = rules
        self.repo = repository

        # Operator map 
        self.operators = {
            ">": self._greater_than,
            "IN": self._in_operator,
            "trans_frequency": self._trans_frequency,
            "avg_last_n": self._avg_last_n,
            "hour_not_between": self._hour_not_between,
        }

    # Main engine process
    def process(self, transaction):
        """Get the transaction, run it through the rules, then raise if fraud"""

        context = self._build_context(transaction)

        triggered = []

        for rule in self.rules:
            operator = self.operators.get(rule["condition"])

            if operator and operator(transaction, rule, context):
                triggered.append(rule)

        # Save first
        self.repo.save_transaction(transaction)

        if triggered:
            rule_ids = [r["id"] for r in triggered]
            reasons = [r["description"] for r in triggered]

            self.repo.save_flag(
                transaction["trans_num"],
                rule_ids,
                reasons)

        return triggered

    # Context preparations for evaluations
    def _build_context(self, transaction):
        """Prepare context"""
        return {
            "hour": self._extract_hour(transaction),
            "is_weekend": self._is_weekend(transaction),
        }

    def _extract_hour(self, transaction):
        """Get the hour"""
        dt = datetime.strptime(transaction["trans_date_trans_time"],"%Y-%m-%d %H:%M:%S")
        return dt.hour

    def _is_weekend(self, transaction):
        """Check if weekend"""
        dt = datetime.strptime(transaction["trans_date_trans_time"],"%Y-%m-%d %H:%M:%S")
        return dt.weekday() >= 5

    # Operations from the rules
    def _greater_than(self, transaction, rule, context):
        """check is the amount is > than the given threshold and if it is weekend or not"""
        if float(transaction[rule["field"]]) <= float(rule["threshold"]):
            return False
        
        if rule.get("modifier") == "weekend":
            return context["is_weekend"] == True

        return True

    def _trans_frequency(self, transaction, rule, context):
        """Check how often the transactions where made"""
        trans_time = datetime.strptime(transaction["trans_date_trans_time"],"%Y-%m-%d %H:%M:%S")

        count = self.repo.trans_frequency(
            transaction["cc_num"],
            trans_time,
            int(rule["threshold_window"])
        )

        return (count + 1) > int(rule["threshold"])
    
    def _avg_last_n(self, transaction, rule, context):
        """Check the amount of the average n transactions"""
        last_n = self.repo.get_last_n_amounts(
            transaction["cc_num"],
            int(rule["threshold_window"])
        )

        if len(last_n) < int(rule["threshold_window"]):
            return False

        avg = sum(last_n) / len(last_n)

        return float(transaction[rule["field"]]) > (
            float(rule["threshold"]) * avg)

    def _hour_not_between(self, transaction, rule, context):
        """Check the time a transaction was made"""
        start = int(rule["threshold"])        
        end = int(rule["threshold_window"]) 

        hour = context["hour"]

        return not (start <= hour <= end)
    
    def _in_operator(self, transaction, rule, context):
        """Check transaction merchant category"""
        category_sets = {
            "high_risk_categories": [
                "shopping_net",
                "misc_net",
                "entertainment"]}

        if rule["modifier"]:
            allowed = category_sets.get(rule["modifier"], [])
        else:
            allowed = ast.literal_eval(rule["threshold"])

        return transaction[rule["field"]] in allowed
