"""
Fraud Rule Evaluation Engine.

- Evaluates incoming transactions against a set of configurable fraud rules loaded from the database at startup. 
- Rules are applied using an operator map that maps condition to evaluation methods
- Adding a new rule type requires adding one method and one entry to the operator map, withoutmodifying the core processing logic.
"""

import ast
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class FraudEngine:
    """Fraud engine - evaluates all active rules against each transaction."""

    def __init__(self, rules, repository):
        self.rules = rules
        self.repo = repository

        # Operator map: maps condition to evaluation methods.
        self.operators = {
            ">": self._greater_than,
            "IN": self._in_operator,
            "trans_frequency": self._trans_frequency,
            "avg_last_n": self._avg_last_n,
            "hour_not_between": self._hour_not_between,
        }

    def process(self, transaction):
        """Evaluate all rules against a transaction."""
        context = self._build_context(transaction)
        triggered = []

        for rule in self.rules:
            operator = self.operators.get(rule["condition"])
            if operator and operator(transaction, rule, context):
                triggered.append(rule)

        transaction_id = transaction["trans_num"]

        if triggered:
            rule_ids = [r["id"] for r in triggered]
            reasons = [r["description"] for r in triggered]
            # Atomic: both the transaction record and the flag are committed together. If either fails, both roll back
            self.repo.save_transaction_and_flag(
                transaction, transaction_id, rule_ids, reasons
            )
            logger.warning(
                "Transaction flagged",
                extra={
                    "transaction_id": transaction_id,
                    "rules_triggered": len(triggered),
                    "reasons": reasons,
                },
            )
        else:
            # Save for audit even when not flagged
            self.repo.save_transaction(transaction)

        return triggered

    # Context builder
    def _build_context(self, transaction):
        """Pre-compute reusable facts once before the rule loop."""
        return {
            "hour": self._extract_hour(transaction),
            "is_weekend": self._is_weekend(transaction),
        }

    def _extract_hour(self, transaction):
        dt = datetime.strptime(transaction["trans_date_trans_time"], "%Y-%m-%d %H:%M:%S")
        return dt.hour

    def _is_weekend(self, transaction):
        dt = datetime.strptime(transaction["trans_date_trans_time"], "%Y-%m-%d %H:%M:%S")
        return dt.weekday() >= 5

    # Rule operators
    def _greater_than(self, transaction, rule, context):
        """Flag if the transaction amount exceeds the threshold. Optionally restricted to weekend transactions only."""
        if float(transaction[rule["field"]]) <= float(rule["threshold"]):
            return False
        if rule.get("modifier") == "weekend":
            return context["is_weekend"]
        return True

    def _trans_frequency(self, transaction, rule, context):
        """Flag if this card exceeds N transactions within a rolling time window."""
        trans_time = datetime.strptime(
            transaction["trans_date_trans_time"], "%Y-%m-%d %H:%M:%S"
        )
        count = self.repo.trans_frequency(
            transaction["cc_num"],
            trans_time,
            int(rule["threshold_window"]),
        )
        return (count + 1) > int(rule["threshold"])

    def _avg_last_n(self, transaction, rule, context):
        """Flag if the current amount exceeds a multiplier of the last N average."""
        last_n = self.repo.get_last_n_amounts(
            transaction["cc_num"],
            int(rule["threshold_window"]),
        )
        if len(last_n) < int(rule["threshold_window"]):
            return False
        avg = sum(last_n) / len(last_n)
        return float(transaction[rule["field"]]) > (float(rule["threshold"]) * avg)

    def _hour_not_between(self, transaction, rule, context):
        """Flag if the transaction hour falls outside the allowed range."""
        start = int(rule["threshold"])
        end = int(rule["threshold_window"])
        return not (start <= context["hour"] <= end)

    def _in_operator(self, transaction, rule, context):
        """Flag if the transaction field value is in a known high-risk set."""
        category_sets = {
            "high_risk_categories": [
                "shopping_net",
                "misc_net",
                "entertainment",
            ]
        }
        if rule["modifier"]:
            allowed = category_sets.get(rule["modifier"], [])
        else:
            # Only evaluate against known safe literal lists - not arbitrary input
            allowed = ast.literal_eval(rule["threshold"])
        return transaction[rule["field"]] in allowed