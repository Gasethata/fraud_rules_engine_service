
-- -------------------------------------------
-- Connect to frauddb database
-- -------------------------------------------
\connect frauddb

-- -------------------------------------------
-- Transaction Records
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
-- Flagged Transactions
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
-- Fraud Rules
-- -------------------------------------------
CREATE TABLE fraud_rules (
    id SERIAL PRIMARY KEY,
    "description" VARCHAR(100) NOT NULL,
    field VARCHAR(50) NOT NULL,
    condition VARCHAR(50) NOT NULL,
    threshold NUMERIC,              -- numeric values
    threshold_window INT,           -- N value (e.g: last 5 transactions, last 10 mins)
    modifier VARCHAR(50),           -- weekend, odd_hours, etc.
    "action" VARCHAR(50) NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- -------------------------------------------
-- Users
-- -------------------------------------------
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    hashed_password TEXT NOT NULL
);