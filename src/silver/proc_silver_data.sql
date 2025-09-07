-- ===========================================================================
-- Stored Procedure: Load Silver Layer (Bronze -> Silver)
-- ===========================================================================
-- Purpose:
--   Perform ETL to populate 'silver' schema tables from 'bronze' schema.
--   Actions:
--     - Truncate Silver tables
--     - Insert cleansed and transformed data from Bronze
--
-- Usage:
--   CALL silver.load_silver();
-- ===========================================================================

CREATE OR REPLACE PROCEDURE silver.load_silver()
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    batch_start_time TIMESTAMP;
    batch_end_time TIMESTAMP;
BEGIN
    batch_start_time := clock_timestamp();
    RAISE INFO '';
    RAISE INFO 'Loading Silver Layer';
    RAISE INFO '';

    RAISE INFO '------------------------------------------------';
    RAISE INFO 'Loading CRM Tables';
    RAISE INFO '------------------------------------------------';

    -- Load silver.crm_cust_info
    start_time := clock_timestamp();
    RAISE INFO '>> Truncating Table: silver.crm_cust_info';
    EXECUTE 'TRUNCATE TABLE silver.crm_cust_info';

    RAISE INFO '>> Inserting Data Into: silver.crm_cust_info';
    EXECUTE $insert$
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
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE flag_last = 1;
    $insert$;

    end_time := clock_timestamp();
    RAISE INFO '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE INFO '>> -------------';

    -- Load silver.crm_prd_info
    start_time := clock_timestamp();
    RAISE INFO '>> Truncating Table: silver.crm_prd_info';
    EXECUTE 'TRUNCATE TABLE silver.crm_prd_info';

    RAISE INFO '>> Inserting Data Into: silver.crm_prd_info';
    EXECUTE $insert$
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
        REPLACE(SUBSTRING(prd_key FROM 1 FOR 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key FROM 7) AS prd_key,
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
    $insert$;

    end_time := clock_timestamp();
    RAISE INFO '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE INFO '>> -------------';

    -- Load silver.crm_sales_details
    start_time := clock_timestamp();
    RAISE INFO '>> Truncating Table: silver.crm_sales_details';
    EXECUTE 'TRUNCATE TABLE silver.crm_sales_details';

    RAISE INFO '>> Inserting Data Into: silver.crm_sales_details';
    EXECUTE $insert$
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
    $insert$;

    end_time := clock_timestamp();
    RAISE INFO '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE INFO '>> -------------';

    -- Load silver.erp_cust_az12
    start_time := clock_timestamp();
    RAISE INFO '>> Truncating Table: silver.erp_cust_az12';
    EXECUTE 'TRUNCATE TABLE silver.erp_cust_az12';

    RAISE INFO '>> Inserting Data Into: silver.erp_cust_az12';
    EXECUTE $insert$
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
    $insert$;

    end_time := clock_timestamp();
    RAISE INFO '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE INFO '>> -------------';

    RAISE INFO '------------------------------------------------';
    RAISE INFO 'Loading ERP Tables';
    RAISE INFO '------------------------------------------------';

    -- Load silver.erp_loc_a101
    start_time := clock_timestamp();
    RAISE INFO '>> Truncating Table: silver.erp_loc_a101';
    EXECUTE 'TRUNCATE TABLE silver.erp_loc_a101';

    RAISE INFO '>> Inserting Data Into: silver.erp_loc_a101';
    EXECUTE $insert$
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
    $insert$;

    end_time := clock_timestamp();
    RAISE INFO '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE INFO '>> -------------';

    -- Load silver.erp_px_cat_g1v2
    start_time := clock_timestamp();
    RAISE INFO '>> Truncating Table: silver.erp_px_cat_g1v2';
    EXECUTE 'TRUNCATE TABLE silver.erp_px_cat_g1v2';

    RAISE INFO '>> Inserting Data Into: silver.erp_px_cat_g1v2';
    EXECUTE $insert$
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
    $insert$;

    end_time := clock_timestamp();
    RAISE INFO '>> Load Duration: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));
    RAISE INFO '>> -------------';

    batch_end_time := clock_timestamp();
    RAISE INFO '==========================================';
    RAISE INFO 'Loading Silver Layer is Completed';
    RAISE INFO '   - Total Load Duration: % seconds', EXTRACT(EPOCH FROM (batch_end_time - batch_start_time));
    RAISE INFO '==========================================';

EXCEPTION WHEN OTHERS THEN
    RAISE WARNING '==========================================';
    RAISE WARNING 'ERROR OCCURRED DURING LOADING SILVER LAYER';
    RAISE WARNING 'Error: %', SQLERRM;
    RAISE WARNING '==========================================';
    -- Optionally re-raise the exception if you want the procedure to fail
    -- RAISE;
END;
$$;
