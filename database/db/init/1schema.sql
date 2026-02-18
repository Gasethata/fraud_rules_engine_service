-- -------------------------------------------
-- 1. Transaction Records (Raw Kafka Events)
-- -------------------------------------------
CREATE TABLE transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    trans_date_trans_time TIMESTAMP NOT NULL,
    cc_num BIGINT NOT NULL,
    merchant VARCHAR(50) NOT NULL,
    category VARCHAR(50) NOT NULL,
    amt NUMERIC NOT NULL,
    first_name VARCHAR(50),
    last_name VARCHAR(50)
);

-- -------------------------------------------
-- 2. Flagged Transactions (After Rules Engine)
-- -------------------------------------------
CREATE TABLE flagged_transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) NOT NULL REFERENCES transactions(transaction_id),
    flagged_at TIMESTAMP DEFAULT NOW(),
    reason tEXT[],
    rule_ids INT[],
    details JSONB
);

-- -------------------------------------------
-- 3. Fraud Rules (Configurable & Dynamic)
-- -------------------------------------------
CREATE TABLE fraud_rules (
    id SERIAL PRIMARY KEY,
    description VARCHAR(100) NOT NULL,
    field VARCHAR(50) NOT NULL,
    condition VARCHAR(50) NOT NULL,
    threshold NUMERIC,              -- numeric values
    threshold_type VARCHAR(50),     -- e.g: static, avg_last_n, count_last_n
    threshold_window INT,           -- N value (e.g: last 5 transactions, last 10 mins)
    modifier VARCHAR(50),           -- weekend, odd_hours, etc.
    action VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- -------------------------------------------
-- 4. (Optional) Audit Log For All Evaluations
-- -------------------------------------------
CREATE TABLE audit_evaluations (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50),
    evaluated_at TIMESTAMP DEFAULT NOW(),
    rule_id INT,
    evaluation_result BOOLEAN,
    details JSONB
);
