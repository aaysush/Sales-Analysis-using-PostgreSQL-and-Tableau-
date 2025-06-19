-------------------------------------------------------
--------------------Schema creation--------------------
-------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS bronze
    AUTHORIZATION postgres;

CREATE SCHEMA IF NOT EXISTS silver
    AUTHORIZATION postgres;

CREATE SCHEMA IF NOT EXISTS gold
    AUTHORIZATION postgres;



	
-- Drop tables if they exist
DROP TABLE IF EXISTS bronze.crm_cust_info;
DROP TABLE IF EXISTS bronze.crm_prd_info;
DROP TABLE IF EXISTS bronze.crm_sales_details;
DROP TABLE IF EXISTS bronze.erp_loc_a101;
DROP TABLE IF EXISTS bronze.erp_cust_az12;
DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

-- Create tables

CREATE TABLE bronze.crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gndr            VARCHAR(50),
    cst_create_date     DATE
);

CREATE TABLE bronze.crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt TIMESTAMP,
    prd_end_dt   TIMESTAMP
);

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

CREATE TABLE bronze.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);

CREATE TABLE bronze.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50)
);

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50)
);

select * from bronze.crm_prd_info




---========================================
---========================================
---SILVER LAYER MAKING 


-- Drop tables if they exist
DROP TABLE IF EXISTS silver.crm_cust_info;
DROP TABLE IF EXISTS silver.crm_prd_info;
DROP TABLE IF EXISTS silver.crm_sales_details;
DROP TABLE IF EXISTS silver.erp_loc_a101;
DROP TABLE IF EXISTS silver.erp_cust_az12;
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

-- Create tables

CREATE TABLE silver.crm_cust_info (
    cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gndr            VARCHAR(50),
    cst_create_date     DATE
);

CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt TIMESTAMP,
    prd_end_dt   TIMESTAMP
);
ALTER TABLE silver.crm_prd_info
ADD COLUMN cat_id VARCHAR(50) ;
ALTER TABLE silver.crm_prd_info
ALTER COLUMN cat_id TYPE VARCHAR(50);



CREATE TABLE silver.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
); 

ALTER TABLE silver.crm_sales_details
ALTER COLUMN sls_order_dt TYPE DATE USING TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD'),
ALTER COLUMN sls_ship_dt TYPE DATE USING TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD'),
ALTER COLUMN sls_due_dt TYPE DATE USING TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD');


CREATE TABLE silver.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);

CREATE TABLE silver.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50)
);

CREATE TABLE silver.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50)
);




---------------------------------------------------
---------------------------------------------------

truncate silver.erp_loc_a101
ALTER TABLE silver.erp_loc_a101
DROP COLUMN date_creation;

INSERT INTO silver.erp_loc_a101 (cid, cntry)
SELECT 
    replace(cid,'-','') AS cid,
    CASE 
        WHEN trim(cntry) = 'DE' THEN 'Germany'
        WHEN trim(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN trim(cntry) = '' OR cntry IS NULL THEN 'Unknown'
        ELSE trim(cntry)
    END AS cntry
FROM bronze.erp_loc_a101
WHERE replace(cid, '-', '') NOT IN (
    SELECT cid FROM silver.erp_loc_a101
);
-------------------------------------------------------------------
--------------------------------------------------------------------


insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
select 
id,
cat,subcat,maintenance

from bronze.erp_px_cat_g1v2


-------------------------------------------------------------------
--------------------------------------------------------------------
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

 
-----------------------------------------------------------------------
-----------------------------------------------------------------------
INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
)
SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE 
		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL
		ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD')
	END AS sls_order_dt,
	CASE 
		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL
		ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD')
	END AS sls_ship_dt,
	CASE 
		WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL
		ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD')
	END AS sls_due_dt,
	CASE 
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
			THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
	sls_quantity,
	CASE 
		WHEN sls_price IS NULL OR sls_price <= 0 
			THEN sls_sales / NULLIF(sls_quantity, 0)
		ELSE sls_price  -- Derive price if original value is invalid
	END AS sls_price
FROM bronze.crm_sales_details;

select * from silver.crm_sales_details



-----------------------------------------
----------------------------------------

------------------Implement soln for Test 1 & 2 & 3 & 4---------------
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE
        WHEN UPPER(cst_marital_status) = 'S' THEN 'SINGLE'
        WHEN UPPER(cst_marital_status) = 'M' THEN 'Married'
        ELSE 'UNKNOWN'
    END AS cst_marital_status,
    CASE
        WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
        WHEN UPPER(cst_gndr) = 'M' THEN 'Male'
        ELSE 'UNKNOWN'
    END AS cst_gndr,
    cst_create_date
FROM (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) subquery
WHERE flag = 1;

SELECT * FROM silver.crm_cust_info;


------------------------------------------------
-----------------------------------------------



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

select * from silver.crm_prd_info

--------------------------------------------------------------
----alter  table -- Add date_creation column to silver.crm_cust_info
ALTER TABLE silver.crm_cust_info
ADD COLUMN date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add date_creation column to silver.crm_prd_info
ALTER TABLE silver.crm_prd_info
ADD COLUMN date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add date_creation column to silver.crm_sales_details
ALTER TABLE silver.crm_sales_details
ADD COLUMN date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add date_creation column to silver.erp_loc_a101
ALTER TABLE silver.erp_loc_a101
ADD COLUMN date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add date_creation column to silver.erp_cust_az12
ALTER TABLE silver.erp_cust_az12
ADD COLUMN date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- Add date_creation column to silver.erp_px_cat_g1v2
ALTER TABLE silver.erp_px_cat_g1v2
ADD COLUMN date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

---========================================================
---============================================================


/*
===============================================================================
Create Gold Views
===============================================================================
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
Note : 
    - we use star schema where we make facts and dimensions
    - we use suurogate key to uniqelyidentify products and customers as the same products
	  can have multiple records of bieng sold and the same customer can buy multiple 
	  prducts so product keys and customer id are repeated so we use the product key to 
	  uniquely identify each row
	- if any data ragrding gender contadicts we give more preference to crm as it is direct data scouce of customers 
===============================================================================
*/

---=============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
DROP VIEW IF EXISTS gold.dim_customers;

CREATE VIEW gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')              -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
    ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
    ON ci.cst_key = la.cid;

select distinct * from silver.erp_loc_a101 ;
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
DROP VIEW IF EXISTS gold.dim_products;

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
DROP VIEW IF EXISTS gold.fact_sales;

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;

-----------------------------------------------------------
----compiled the entire warehouse project part