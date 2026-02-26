"""
Database queries.
    - All the used queries and thier logic.
    - Returns the queries results.
"""

import psycopg2
from psycopg2.extras import RealDictCursor


class Repository:
    """Repository class"""
    def __init__(self, db_config):
        """Initialization"""
        self.conn = psycopg2.connect(**db_config)

    def fetch_active_rules(self):
        """Fetch all rules from the DB"""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT *
                FROM fraud_rules
                WHERE is_active = true
            """)
            return cur.fetchall()

    def save_transaction(self, transaction):
        """Save the transactions from Kafka into the DB"""
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

    def save_flag(self, transaction_id, rule_id, reason):
        """Save the flagged transactions"""
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

    def trans_frequency(self, cc_num, trans_time, minutes):
        """Context needed for the frequency rule"""
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*)
                FROM transactions
                WHERE cc_num = %s
                AND trans_date_trans_time >= %s - make_interval(mins => %s)
            """, (cc_num, trans_time, minutes))
            return cur.fetchone()[0]
        
    def get_last_n_amounts(self, cc_num, n):
        """Get the amounts for n transactions"""
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
        
 
