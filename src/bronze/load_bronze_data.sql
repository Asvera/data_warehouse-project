-- ==========================================================================
-- Truncate and Load Bronze Tables from CSVs
-- With Error Handling and Timestamps
-- ==========================================================================

-- Stop on first error
\set ON_ERROR_STOP on

-- Show execution time per command
\timing on



-- TRUNCATE ALL TABLES
\echo  Truncating bronze tables...

TRUNCATE TABLE bronze.crm_cust_info;
TRUNCATE TABLE bronze.crm_prd_info;
TRUNCATE TABLE bronze.crm_sales_details;
TRUNCATE TABLE bronze.erp_cust_az12;
TRUNCATE TABLE bronze.erp_loc_a101;
TRUNCATE TABLE bronze.erp_px_cat_g1v2;

\echo All bronze tables truncated.



-- LOAD CRM TABLES

\echo Loading crm_cust_info...
\COPY bronze.crm_cust_info FROM '~/Desktop/Data_warehouse/dataset/source_crm/cust_info.csv' WITH (FORMAT csv, HEADER true);

SELECT 'crm_cust_info loaded at' AS status, now();


\echo Loading crm_prd_info...
\COPY bronze.crm_prd_info FROM '~/Desktop/Data_warehouse/dataset/source_crm/prd_info.csv' WITH (FORMAT csv, HEADER true);

SELECT 'crm_prd_info loaded at' AS status, now();


\echo Loading crm_sales_details...
\COPY bronze.crm_sales_details FROM '~/Desktop/Data_warehouse/dataset/source_crm/sales_details.csv' WITH (FORMAT csv, HEADER true);

SELECT 'crm_sales_details loaded at' AS status, now();




-- LOAD ERP TABLES

\echo Loading erp_cust_az12...
\COPY bronze.erp_cust_az12 FROM '~/Desktop/Data_warehouse/dataset/source_erp/CUST_AZ12.csv' WITH (FORMAT csv, HEADER true);

SELECT 'erp_cust_az12 loaded at' AS status, now();


\echo Loading erp_loc_a101...
\COPY bronze.erp_loc_a101 FROM '~/Desktop/Data_warehouse/dataset/source_erp/LOC_A101.csv' WITH (FORMAT csv, HEADER true);

SELECT 'erp_loc_a101 loaded at' AS status, now();


\echo Loading erp_px_cat_g1v2...
\COPY bronze.erp_px_cat_g1v2  FROM '~/Desktop/Data_warehouse/dataset/source_erp/PX_CAT_G1V2.csv' WITH (FORMAT csv, HEADER true);

SELECT 'erp_px_cat_g1v2 loaded at' AS status, now();




-- Final Status Check
\echo Verifying row counts...

SELECT 'crm_cust_info', COUNT(*) FROM bronze.crm_cust_info;
SELECT 'crm_prd_info', COUNT(*) FROM bronze.crm_prd_info;
SELECT 'crm_sales_details', COUNT(*) FROM bronze.crm_sales_details;
SELECT 'erp_cust_az12', COUNT(*) FROM bronze.erp_cust_az12;
SELECT 'erp_loc_a101', COUNT(*) FROM bronze.erp_loc_a101;
SELECT 'erp_px_cat_g1v2', COUNT(*) FROM bronze.erp_px_cat_g1v2;

\echo  All data loaded successfully!
