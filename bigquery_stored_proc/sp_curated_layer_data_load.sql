CREATE OR REPLACE PROCEDURE `ranjanrishi-project.testdataset.sp_curated_layer_data_load`()
BEGIN
/**Merging customer data start**/
MERGE curated.customer T
USING (

    SELECT * FROM (
        SELECT
            customer_id,
            TRIM(customer_name) AS name,            
            CASE 
                WHEN REGEXP_CONTAINS(email, 
                    r'^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$')
                THEN LOWER(email)
                ELSE NULL
            END AS email,
			case when REGEXP_CONTAINS(phone, r'^(\+91)?[6-9][0-9]{9}$') then phone else "" end AS phone,          
            REGEXP_REPLACE(account_id, r'[^A-Za-z0-9]', '') AS account_id,
            gender,
            date(dob) dob,
            address,
            kyc_status,
            date(registration_date) registration_date,
            transaction_id,
            CURRENT_TIMESTAMP() AS load_time,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY registration_date DESC) rn
        FROM `rawlayer.customer`
        WHERE
            customer_name IS NOT NULL
            AND email IS NOT NULL
            AND phone IS NOT NULL
    )
    WHERE rn = 1

) S
ON T.customer_id = S.customer_id

WHEN MATCHED THEN
  UPDATE SET
    T.name = S.name,
    T.email = S.email,
    T.phone = S.phone,
    T.account_id = S.account_id,
    T.gender = S.gender,
    T.dob = S.dob,
    T.address = S.address,
    T.kyc_status = S.kyc_status,
    T.registration_date = S.registration_date,
    T.transaction_id = S.transaction_id,
    T.updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (
    customer_id,
    name,
    email,
    phone,
    account_id,
    gender,
    dob,
    address,
    kyc_status,
    registration_date,
    transaction_id,
    load_time,
    created_at,
    updated_at
  )
  VALUES (
    S.customer_id,
    S.name,
    S.email,
    S.phone,
    S.account_id,
    S.gender,
    S.dob,
    S.address,
    S.kyc_status,
    S.registration_date,
    S.transaction_id,
    S.load_time,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP()
  );
/**Merging customer data end **/

/***Merge branch data start ***/
MERGE curated.branch T
USING (
		select * from (select branch_id,
		branch_name,
		location,
		manager_name,
		date(opened_date) opened_date,
		region,
		branch_type,
		case when REGEXP_CONTAINS(contact_number, r'^(\+91)?[6-9][0-9]{9}$') then contact_number else "" end AS contact_number,		
		ROW_NUMBER() OVER (PARTITION BY branch_id ORDER BY opened_date DESC) rn 
		from `rawlayer.branch` Where branch_id is not null )
		where rn=1
	  ) S	
	  ON T.branch_id=S.branch_id
	  
WHEN MATCHED THEN
  UPDATE SET
T.branch_id=S.branch_id,
T.branch_name= S.branch_name ,
T.location=S.location,
T.manager_name=S.manager_name,
T.opened_date=S.opened_date,	 
T.region= S.region,
T.branch_type=S.branch_type,
T.contact_number=S.contact_number,
T.updated_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (branch_id,
		  branch_name,
		  location,
		  manager_name,
		  opened_date	,
		  region,
		  branch_type,
		  contact_number,		
		  created_at,
		  updated_at	
		)
VALUES (
S.branch_id,
S.branch_name,
S.location,
S.manager_name,
S.opened_date	,
S.region,
S.branch_type,
S.contact_number,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP()
);
/**Merge branch data end**/


/** Merge accounts data start **/

MERGE curated.account T
USING (
select 	account_id,
		account_name,
    phone_number,
		address,
		created_date 
		from (
		select REGEXP_REPLACE(account_id, r'[^A-Za-z0-9]', '') as account_id,
		REGEXP_REPLACE(account_name, r'[^A-Za-z0-9]', '') as account_name,
		case when REGEXP_CONTAINS(phone_number, r'^(\+91)?[6-9][0-9]{9}$') then phone_number else "" end AS phone_number,		
		REGEXP_REPLACE(address, r'[^A-Za-z0-9]', ' ')address,
		case when date(created_date) > CURRENT_DATE() then CURRENT_DATE() else date(created_date) end as created_date,
		ROW_NUMBER() OVER(PARTITION BY account_id order by created_date desc) as rn 
		from rawlayer.account
		Where account_id is not null
 ) where rn=1
  )S
  
 ON T.account_id=S.account_id
 
 WHEN MATCHED THEN
  UPDATE SET 
T.account_id =S.account_id,
T.account_name =S.account_name,
T.phone_number=S.phone_number,
T.address=S.address,
T.created_date=S.created_date,
T.updated_at=CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (account_id,
account_name,
phone_number,
address,
created_date,
created_at,
updated_at)
 VALUES 
 (
S.account_id,
S.account_name,
S.phone_number,
S.address,
S.created_date,
CURRENT_TIMESTAMP(),
CURRENT_TIMESTAMP()
);
/** Merge accounts data end **/

/**Merge product start **/

MERGE curated.products T
USING (
select 
	product_id,
	product_name,
	product_type,
	price,
	launch_date,
	is_active,
	category,
	vendor_name
from
(
  select 
	REGEXP_REPLACE(product_id, r'[^A-Za-z0-9]', '') AS product_id,
	REGEXP_REPLACE(product_name, r'[^A-Za-z0-9_]', '') AS product_name,
	REGEXP_REPLACE(product_type, r'[^A-Za-z0-9]', '') AS product_type,
	price,
	date(launch_date)launch_date,
	is_active,
	REGEXP_REPLACE(category, r'[^A-Za-z0-9]', '') AS category,
	REGEXP_REPLACE(vendor_name, r'[^A-Za-z0-9]', '') AS vendor_name,
	row_number() over(partition by product_id order by launch_date desc) rn
 from rawlayer.products
 where product_id is not null
		and product_name is not null
		and category is not null )
 where rn=1) S
 ON T.product_id=S.product_id
 
  WHEN MATCHED THEN
  UPDATE SET 
T.product_id=S.product_id,
T.product_name=S.product_name,
T.product_type=S.product_type,
T.price=S.price,
T.launch_date=S.launch_date,
T.is_active=CAST(S.is_active AS STRING),
T.category=S.category,
T.vendor_name=S.vendor_name

WHEN NOT MATCHED THEN
  INSERT (
		product_id,
		product_name,
		product_type,
		price,
		launch_date,
		is_active,
		category,
		vendor_name,
		created_at,
		updated_at
		)
	VALUES
    (
		S.product_id,
		S.product_name,
		S.product_type,
		S.price,
		S.launch_date,
		CAST(S.is_active AS STRING),
		S.category,
		S.vendor_name,
		CURRENT_TIMESTAMP(),
		CURRENT_TIMESTAMP()
		);


/**Merge product end **/

/** Merge transaction start ***/
MERGE curated.transaction T
USING (
select transaction_id,
account_id ,
product_id,
branch_id,
date DATE,
timestamp,
amount,
payment_method,
status,
currency,
remarks,
is_refund from(
select 
REGEXP_REPLACE(transaction_id, r'[^A-Za-z0-9]', '') AS transaction_id,
REGEXP_REPLACE(account_id, r'[^A-Za-z0-9]', '') AS account_id,
REGEXP_REPLACE(product_id, r'[^A-Za-z0-9]', '') AS product_id,
REGEXP_REPLACE(branch_id, r'[^A-Za-z0-9]', '') AS branch_id,
date,
timestamp,
amount,
REGEXP_REPLACE(payment_method, r'[^A-Za-z0-9]', '') AS payment_method,
REGEXP_REPLACE(status, r'[^A-Za-z0-9]', '') AS status,
currency,
remarks,
cast(is_refund as boolean) as is_refund,
row_number() over(partition by transaction_id  order by date desc) rn
from rawlayer.transaction
where transaction_id is not null)
where rn=1) S
ON T.transaction_id=S.transaction_id

  WHEN MATCHED THEN
  UPDATE SET 
  T.transaction_id=S.transaction_id,
T.account_id =S.account_id ,
T.product_id=S.product_id,
T.branch_id=S.branch_id,
T.date =date(S.date),
T.timestamp=S.timestamp,
T.amount=S.amount,
T.payment_method=S.payment_method,
T.status=S.status,
T.currency=S.currency,
T.remarks=S.remarks,
T.is_refund=cast(S.is_refund as boolean),
T.updated_at=current_timestamp()
  
  WHEN NOT MATCHED THEN
  INSERT (
transaction_id,
account_id ,
product_id,
branch_id,
date,
timestamp,
amount,
payment_method,
status,
currency,
remarks,
is_refund,
created_at,
updated_at
)
VALUES
(
S.transaction_id,
S.account_id ,
S.product_id,
S.branch_id,
date(S.date),
S.timestamp,
S.amount,
S.payment_method,
S.status,
S.currency,
S.remarks,
cast(S.is_refund as boolean),
current_timestamp(),
current_timestamp()
);
/** Merge transaction end ***/


END;