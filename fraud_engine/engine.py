import ast
from datetime import datetime


class FraudEngine:

    def __init__(self, rules, repository):
        self.rules = rules
        self.repo = repository

        # Operator map 
        self.operators = {
            ">": self._greater_than,
            "<": self._less_than,
            "!=": self._not_equal,
            "IN": self._in_operator,
            "trans_frequency": self._trans_frequency,
            "avg_last_n": self._avg_last_n,
            "hour_not_between": self._hour_not_between,
        }

    # -------------------------
    # MAIN ENTRY
    # -------------------------
    def process(self, transaction):

        # Build context once
        context = self._build_context(transaction)

        triggered = []

        for rule in self.rules:
            operator = self.operators.get(rule["condition"])

            if operator and operator(transaction, rule, context):
                triggered.append(rule)

        # Save transaction first
        self.repo.save_transaction(transaction)

        if triggered:
            rule_ids = [r["id"] for r in triggered]
            reasons = [r["description"] for r in triggered]

            self.repo.save_flag(
                transaction["trans_num"],
                rule_ids,
                reasons)

        return triggered

    # -------------------------
    # CONTEXT
    # -------------------------  
    def _build_context(self, transaction):
        return {
            "hour": self._extract_hour(transaction),
            "is_weekend": self._is_weekend(transaction),
        }

    def _extract_hour(self, transaction):
        dt = datetime.strptime(transaction["trans_date_trans_time"],"%Y-%m-%d %H:%M:%S")
        return dt.hour

    def _is_weekend(self, transaction):
        dt = datetime.strptime(transaction["trans_date_trans_time"],"%Y-%m-%d %H:%M:%S")
        return dt.weekday() >= 5

    # -------------------------
    # OPERATORS
    # -------------------------
    def _greater_than(self, transaction, rule, context):
        return float(transaction[rule["field"]]) > float(rule["threshold"])

    def _less_than(self, transaction, rule, context):
        return float(transaction[rule["field"]]) < float(rule["threshold"])

    def _not_equal(self, transaction, rule, context):
        return transaction[rule["field"]] != rule["threshold"]

    def _trans_frequency(self, transaction, rule, context):

        trans_time = datetime.strptime(transaction["trans_date_trans_time"],"%Y-%m-%d %H:%M:%S")

        count = self.repo.trans_frequency(
            transaction["cc_num"],
            trans_time,
            int(rule["threshold_window"])
        )

        return (count + 1) > int(rule["threshold"])
    
    def _avg_last_n(self, transaction, rule, context):

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

        start = int(rule["threshold"])        
        end = int(rule["threshold_window"]) 

        hour = context["hour"]

        return not (start <= hour <= end)
    
    def _in_operator(self, transaction, rule, context):

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
