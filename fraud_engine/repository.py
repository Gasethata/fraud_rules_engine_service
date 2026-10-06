"""
Database queries.
    - All queries and their logic.
    - Retry logic on transient failures using tenacity.
    - Atomic transaction boundaries: save_transaction and save_flag
      commit together to prevent data inconsistency.
"""

import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
    retry_if_exception_type,
    before_sleep_log,
)

logger = logging.getLogger(__name__)

# Retry on transient DB errors only - not on programming errors or constraint violations.
# wait_exponential_jitter adds randomness so multiple instances don't all retry at once.
TRANSIENT_ERRORS = (psycopg2.OperationalError, psycopg2.InterfaceError)

db_retry = retry(
    retry=retry_if_exception_type(TRANSIENT_ERRORS),
    stop=stop_after_attempt(3),
    wait=wait_exponential_jitter(initial=1, max=10),
    reraise=True,
    before_sleep=before_sleep_log(logger, logging.WARNING),
)


class Repository:
    """Repository class - all database interactions."""

    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = self._connect()

    @db_retry
    def _connect(self):
        """Open a database connection with retry on transient failures."""
        return psycopg2.connect(**self.db_config)

    @db_retry
    def fetch_active_rules(self):
        """Fetch all active fraud rules from the database."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM fraud_rules
                WHERE is_active = true
            """)
            return cur.fetchall()

    @db_retry
    def save_transaction_and_flag(self, transaction, transaction_id, rule_ids, reasons):
        """
        Atomically save a flagged transaction and its flag record in one commit.
        If save_flag fails after save_transaction succeeds, the whole operation
        rolls back — no orphaned transaction records without a corresponding flag.
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO transactions (
                        transaction_id,
                        trans_date_trans_time,
                        cc_num,
                        merchant,
                        category,
                        amt,
                        first_name,
                        last_name
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    transaction["trans_num"],
                    transaction["trans_date_trans_time"],
                    transaction["cc_num"],
                    transaction["merchant"],
                    transaction["category"],
                    transaction["amt"],
                    transaction.get("first"),
                    transaction.get("last"),
                ))

                cur.execute("""
                    INSERT INTO flagged_transactions (
                        transaction_id,
                        rule_ids,
                        reason
                    )
                    VALUES (%s,%s,%s)
                """, (transaction_id, rule_ids, reasons))

            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    @db_retry
    def save_transaction(self, transaction):
        """
        Save a non-flagged transaction.
        Used when the engine processes a transaction but no rules trigger.
        """
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO transactions (
                        transaction_id,
                        trans_date_trans_time,
                        cc_num,
                        merchant,
                        category,
                        amt,
                        first_name,
                        last_name
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                """, (
                    transaction["trans_num"],
                    transaction["trans_date_trans_time"],
                    transaction["cc_num"],
                    transaction["merchant"],
                    transaction["category"],
                    transaction["amt"],
                    transaction.get("first"),
                    transaction.get("last"),
                ))
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    @db_retry
    def trans_frequency(self, cc_num, trans_time, minutes):
        """Count transactions for a card within a rolling time window."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM transactions
                WHERE cc_num = %s
                AND trans_date_trans_time >= %s - make_interval(mins => %s)
            """, (cc_num, trans_time, minutes))
            return cur.fetchone()[0]

    @db_retry
    def get_last_n_amounts(self, cc_num, n):
        """Return the amounts for the last N transactions for a given card."""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT amt
                FROM transactions
                WHERE cc_num = %s
                ORDER BY trans_date_trans_time DESC
                LIMIT %s
            """, (cc_num, n))
            rows = cur.fetchall()
            return [float(row[0]) for row in rows]