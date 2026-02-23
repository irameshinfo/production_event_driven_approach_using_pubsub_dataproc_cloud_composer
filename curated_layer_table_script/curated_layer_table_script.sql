CREATE TABLE IF NOT EXISTS curated.customer (
    customer_id STRING,
    name STRING,
    email STRING,
    phone STRING,
    account_id STRING,
    gender STRING,
    dob DATE,
    address STRING,
    kyc_status STRING,
    registration_date DATE,
    transaction_id STRING,
    load_time TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
)
PARTITION BY DATE(updated_at)
CLUSTER BY customer_id;

CREATE TABLE IF NOT EXISTS curated.branch (
	branch_id STRING,
	branch_name STRING,
	location STRING,
	manager_name STRING,
	opened_date DATE,
	region STRING,
	branch_type STRING,
	contact_number STRING,
	created_at TIMESTAMP,
    updated_at TIMESTAMP
)
PARTITION BY (opened_date)
CLUSTER BY branch_id;

CREATE TABLE IF NOT EXISTS curated.account (
account_id STRING,
account_name STRING,
phone_number STRING,
address STRING,
created_date date,
created_at TIMESTAMP,
updated_at TIMESTAMP)
PARTITION BY  (created_date)
CLUSTER BY (account_id);

CREATE TABLE IF NOT EXISTS curated.products
(
product_id STRING,
product_name STRING,
product_type STRING,
price FLOAT64,
launch_date date,
is_active STRING,
category STRING,
vendor_name STRING,
created_at TIMESTAMP,
updated_at TIMESTAMP
)
PARTITION BY  (launch_date)
CLUSTER BY (product_id)

CREATE TABLE IF NOT EXISTS curated.transaction
(
transaction_id STRING,
account_id  STRING,
product_id STRING,
branch_id STRING,
date DATE,
timestamp TIMESTAMP,
amount FLOAT64,
payment_method STRING,
status STRING,
currency STRING,
remarks STRING,
is_refund BOOLEAN,
created_at TIMESTAMP,
updated_at TIMESTAMP
)
PARTITION BY  (date)
CLUSTER BY branch_id,product_id,transaction_id
