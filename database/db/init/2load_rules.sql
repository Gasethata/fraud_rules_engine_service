COPY fraud_rules(description,field,condition,threshold,threshold_window,modifier,action,is_active)
FROM '/docker-entrypoint-initdb.d/fraud_rules.csv'
DELIMITER ','
CSV HEADER
NULL 'NULL';