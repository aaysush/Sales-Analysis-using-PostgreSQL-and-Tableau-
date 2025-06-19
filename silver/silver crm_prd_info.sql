-------------------crm_prd_info------------------------
-----Test 1 : we have cat_id in erp_px_cat_g1v2 which is the intial 5 letter in prd_key but with _ insted of - 
-----the remaining is part of another colummn

-----Test 2 : remove spaces
-----Test 3 : replace null with 0 
-----Test 4 : repalce M with MOuntain ,R with road, T with Touring & S with other sales
-----Test 5 : dates - if end date < start date then we change i.e the start date of the second record 
-----         becomes end date of first and so on



INSERT INTO silver.crm_prd_info (
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT
	prd_id,
	REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id, -- Extract category ID
	SUBSTRING(prd_key FROM 7) AS prd_key,                         -- Extract product key
	prd_nm,
	COALESCE(prd_cost, 0) AS prd_cost,
	CASE 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
		WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
		WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
		WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
		ELSE 'n/a'
	END AS prd_line,                                              -- Map product line codes to descriptive values
	prd_start_dt::DATE AS prd_start_dt,
	(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day')::DATE AS prd_end_dt -- Calculate end date
FROM bronze.crm_prd_info;

    
