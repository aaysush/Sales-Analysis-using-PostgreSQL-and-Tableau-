/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `COPY` command to load data from CSV files into bronze tables.
    - Logs execution time in seconds for each table and overall.
    - Handles errors and logs them if any occur.
    
Parameters:
    None. 
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    CALL bronze.load_bronze_tables();
===============================================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze_tables()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    total_start_time TIMESTAMP;
    total_end_time TIMESTAMP;
    row_count INT;
    time_taken NUMERIC;
    total_time_taken NUMERIC;
BEGIN
    RAISE NOTICE 'Starting Bronze Layer Loading...';

    -- Start total execution timer
    total_start_time := clock_timestamp();

    -- Truncate tables
    RAISE NOTICE 'Truncating tables...';
    TRUNCATE TABLE bronze.crm_cust_info RESTART IDENTITY;
    TRUNCATE TABLE bronze.crm_prd_info RESTART IDENTITY;
    TRUNCATE TABLE bronze.crm_sales_details RESTART IDENTITY;
    TRUNCATE TABLE bronze.erp_loc_a101 RESTART IDENTITY;
    TRUNCATE TABLE bronze.erp_cust_az12 RESTART IDENTITY;
    TRUNCATE TABLE bronze.erp_px_cat_g1v2 RESTART IDENTITY;

    BEGIN
        -- Load crm_cust_info
        start_time := clock_timestamp();
        COPY bronze.crm_cust_info FROM 'path' 
        DELIMITER ',' CSV HEADER;
        end_time := clock_timestamp();
        time_taken := EXTRACT(EPOCH FROM (end_time - start_time));  -- Convert interval to seconds
        SELECT COUNT(*) INTO row_count FROM bronze.crm_cust_info;
        RAISE NOTICE 'crm_cust_info Loaded: % rows (Time Taken: %s seconds)', row_count, time_taken;

        -- Load crm_prd_info
        start_time := clock_timestamp();
        COPY bronze.crm_prd_info FROM 'path'
        DELIMITER ',' CSV HEADER;
        end_time := clock_timestamp();
        time_taken := EXTRACT(EPOCH FROM (end_time - start_time));
        SELECT COUNT(*) INTO row_count FROM bronze.crm_prd_info;
        RAISE NOTICE 'crm_prd_info Loaded: % rows (Time Taken: %s seconds)', row_count, time_taken;

        -- Load crm_sales_details
        start_time := clock_timestamp();
        COPY bronze.crm_sales_details FROM 'path'
        DELIMITER ',' CSV HEADER;
        end_time := clock_timestamp();
        time_taken := EXTRACT(EPOCH FROM (end_time - start_time));
        SELECT COUNT(*) INTO row_count FROM bronze.crm_sales_details;
        RAISE NOTICE 'crm_sales_details Loaded: % rows (Time Taken: %s seconds)', row_count, time_taken;

        -- Load erp_loc_a101
        start_time := clock_timestamp();
        COPY bronze.erp_loc_a101 FROM 'path'
        DELIMITER ',' CSV HEADER;
        end_time := clock_timestamp();
        time_taken := EXTRACT(EPOCH FROM (end_time - start_time));
        SELECT COUNT(*) INTO row_count FROM bronze.erp_loc_a101;
        RAISE NOTICE 'erp_loc_a101 Loaded: % rows (Time Taken: %s seconds)', row_count, time_taken;

        -- Load erp_cust_az12
        start_time := clock_timestamp();
        COPY bronze.erp_cust_az12 FROM 'path' 
        DELIMITER ',' CSV HEADER;
        end_time := clock_timestamp();
        time_taken := EXTRACT(EPOCH FROM (end_time - start_time));
        SELECT COUNT(*) INTO row_count FROM bronze.erp_cust_az12;
        RAISE NOTICE 'erp_cust_az12 Loaded: % rows (Time Taken: %s seconds)', row_count, time_taken;

        -- Load erp_px_cat_g1v2
        start_time := clock_timestamp();
        COPY bronze.erp_px_cat_g1v2 FROM 'path'
        DELIMITER ',' CSV HEADER;
        end_time := clock_timestamp();
        time_taken := EXTRACT(EPOCH FROM (end_time - start_time));
        SELECT COUNT(*) INTO row_count FROM bronze.erp_px_cat_g1v2;
        RAISE NOTICE 'erp_px_cat_g1v2 Loaded: % rows (Time Taken: %s seconds)', row_count, time_taken;

        -- Capture total execution time
        total_end_time := clock_timestamp();
        total_time_taken := EXTRACT(EPOCH FROM (total_end_time - total_start_time));
        RAISE NOTICE 'Bronze Layer Loading Completed! Total Execution Time: %s seconds', total_time_taken;

    EXCEPTION
        WHEN OTHERS THEN
            RAISE NOTICE 'Error Loading Bronze Tables: %', SQLERRM;
    END;
END;
$$;




CALL bronze.load_bronze_tables();

