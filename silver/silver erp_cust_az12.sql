-----------------------------------------------------------
---test 1: clean up those id as the old data of the table has NAS in front of them
---test 2 : no unmatch data  between crm_cust_info
---test 3: bdate > 2026
------------------------------------------------------------
INSERT INTO
	SILVER.ERP_CUST_AZ12 (CID, BDATE, GEN)
SELECT
	CASE
		WHEN CID LIKE 'NAS%' THEN SUBSTRING(
			CID
			FROM
				4 FOR LENGTH(CID)
		)
		ELSE CID
	END AS CID,
	CASE
		WHEN BDATE > NOW() THEN NULL
		ELSE BDATE
	END AS BDATE,
	CASE
		WHEN UPPER(GEN) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(GEN) IN ('M', 'MALE') THEN 'Male'
		ELSE 'UNKNOWN'
	END AS GEN
FROM
	BRONZE.ERP_CUST_AZ12;