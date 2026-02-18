from datetime import datetime


class FraudEngine:

    def __init__(self, rules, repository):
        self.rules = rules
        self.repo = repository

        # Operator map (dynamic)
        self.operators = {
            ">": self._greater_than,
            "<": self._less_than,
            "!=": self._not_equal,
            "IN": self._in_operator,
            "count_last_30min": self._count_last_30min,
        }

    # -------------------------
    # MAIN ENTRY
    # -------------------------
    def process(self, transaction):

        # Save transaction first
        self.repo.save_transaction(transaction)

        # Build context once
        context = self._build_context(transaction)

        triggered = []

        for rule in self.rules:
            operator = self.operators.get(rule["condition"])

            if operator and operator(transaction, rule, context):
                triggered.append(rule)

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
            "count_last_30min": self.repo.count_last_30min(transaction["cc_num"]),
            "hour": self._extract_hour(transaction),
            "is_weekend": self._is_weekend(transaction),
        }

    def _extract_hour(self, transaction):
        dt = datetime.strptime(
            transaction["trans_date_trans_time"],
            "%Y-%m-%d %H:%M:%S"
        )
        return dt.hour

    def _is_weekend(self, transaction):
        dt = datetime.strptime(
            transaction["trans_date_trans_time"],
            "%Y-%m-%d %H:%M:%S"
        )
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

    def _in_operator(self, transaction, rule, context):
        allowed = eval(rule["threshold"])
        return transaction[rule["field"]] in allowed

    def _count_last_30min(self, transaction, rule, context):
        return context["count_last_30min"] > int(rule["threshold"])
