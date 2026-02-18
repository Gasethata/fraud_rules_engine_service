import psycopg2
from psycopg2.extras import RealDictCursor


class Repository:

    def __init__(self, db_config):
        self.conn = psycopg2.connect(**db_config)

    # -------------------------
    # RULES
    # -------------------------
    def fetch_active_rules(self):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM fraud_rules
                WHERE is_active = true
            """)
            return cur.fetchall()

    # -------------------------
    # TRANSACTIONS
    # -------------------------
    def save_transaction(self, transaction):
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
                transaction.get("last")
            ))
        self.conn.commit()

    # -------------------------
    # FLAGS
    # -------------------------
    def save_flag(self, transaction_id, rule_id, reason):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO flagged_transactions (
                    transaction_id,
                    rule_ids,
                    reason
                )
                VALUES (%s,%s, %s)
            """, (transaction_id, rule_id, reason))
        self.conn.commit()

    # -------------------------
    # CONTEXT QUERIES
    # -------------------------
    def count_last_30min(self, cc_num):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM transactions
                WHERE cc_num = %s
                AND trans_date_trans_time >= NOW() - INTERVAL '30 minutes'
            """, (cc_num,))
            return cur.fetchone()[0]
