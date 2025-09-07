-- ===========================================================================
-- Script: Load Silver Layer (Bronze -> Silver)
-- ===========================================================================
-- Purpose:
--   Perform ETL to populate 'silver' schema tables from 'bronze' schema.
-- 
-- Usage:
--   Either use this script in psql or use stored procedure silver.load_silver();
--   defined in proc_silver_data.sql
-- ===========================================================================


-- Stop on first error
\set ON_ERROR_STOP on

-- Show execution time per command
\timing on


-- Truncate all silver tables

\echo  Truncating Silver tables...

TRUNCATE TABLE
  silver.crm_cust_info,
  silver.crm_prd_info,
  silver.crm_sales_details,
  silver.erp_cust_az12,
  silver.erp_loc_a101,
  silver.erp_px_cat_g1v2;

\echo  All silver tables Truncated 



\echo
\echo  Loading into silver.crm_cust_info tables...
-- crm_cust_info
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
    TRIM(cst_firstname),
    TRIM(cst_lastname),
    CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
    END,
    CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'n/a'
    END,
    cst_create_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
    FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL
) t
WHERE flag_last = 1;

\echo Loaded 
\echo





-- crm_prd_info
\echo
\echo  Loading into silver.crm_prd_info 
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
    REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_'),
    SUBSTRING(prd_key FROM 7),
    prd_nm,
    COALESCE(prd_cost, 0),
    CASE
        WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
    END,
    prd_start_dt::date,
    (LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - INTERVAL '1 day')::date
FROM bronze.crm_prd_info;


\echo Loaded 
\echo




-- crm_sales_details
\echo
\echo  Loading into silver.crm_sales_details
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
        WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::text) != 8 THEN NULL
        ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD')
    END,
    CASE
        WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::text) != 8 THEN NULL
        ELSE TO_DATE(sls_ship_dt::text, 'YYYYMMDD')
    END,
    CASE
        WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::text) != 8 THEN NULL
        ELSE TO_DATE(sls_due_dt::text, 'YYYYMMDD')
    END,
    CASE
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END,
    sls_quantity,
    CASE
        WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
        ELSE sls_price
    END
FROM bronze.crm_sales_details;


\echo Loaded 
\echo




-- erp_cust_az12
\echo
\echo  Loading into
INSERT INTO silver.erp_cust_az12 (
    cid,
    bdate,
    gen
)
SELECT
    CASE
        WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid FROM 4)
        ELSE cid
    END,
    CASE
        WHEN bdate > CURRENT_DATE THEN NULL
        ELSE bdate
    END,
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'n/a'
    END
FROM bronze.erp_cust_az12;


\echo Loaded 
\echo



-- erp_loc_a101
\echo
\echo  Loading into silver.erp_loc_a101
INSERT INTO silver.erp_loc_a101 (
    cid,
    cntry
)
SELECT
    REPLACE(cid, '-', ''),
    CASE
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END
FROM bronze.erp_loc_a101;


\echo Loaded 
\echo



-- erp_px_cat_g1v2
\echo
\echo  Loading into silver.erp_px_cat_g1v2 
INSERT INTO silver.erp_px_cat_g1v2 (
    id,
    cat,
    subcat,
    maintenance
)
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;


\echo Loaded 
\echo




-- Final Status Check
\echo Verifying row counts...

SELECT 'crm_cust_info', COUNT(*) FROM silver.crm_cust_info;
SELECT 'crm_prd_info', COUNT(*) FROM silver.crm_prd_info;
SELECT 'crm_sales_details', COUNT(*) FROM silver.crm_sales_details;
SELECT 'erp_cust_az12', COUNT(*) FROM silver.erp_cust_az12;
SELECT 'erp_loc_a101', COUNT(*) FROM silver.erp_loc_a101;
SELECT 'erp_px_cat_g1v2', COUNT(*) FROM silver.erp_px_cat_g1v2;

\echo  All data loaded successfully!
