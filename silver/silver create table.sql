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
    cst_create_date     DATE,
	date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP --helps to track where the curropt data is coming from
);

CREATE TABLE silver.crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
	cat_id       VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt TIMESTAMP,
    prd_end_dt   TIMESTAMP,
	date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);

CREATE TABLE silver.crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT,
	date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);

CREATE TABLE silver.erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50),
	date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);

CREATE TABLE silver.erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50),
	date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);

CREATE TABLE silver.erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50),
	date_creation TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);

select * from silver.crm_prd_info


