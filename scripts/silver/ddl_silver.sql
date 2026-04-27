/*
===============================================================================
Script: Load Silver Layer
===============================================================================
Description: 
    This script cleanses and transforms data from the 'bronze' layer 
    and populates the 'silver' layer. 
    Transformations include:
    - Deduplication (CRM Customers)
    - Handling invalid dates ('0000-00-00' and future dates)
    - Mapping abbreviations to full text (Gender, Marital Status, Product Line)
    - Calculated fields (Derived sales totals)
    
Author: Zain
Project: Data Warehouse (Baraa Masterclass)
===============================================================================
*/

USE silver;

SELECT '>>> STARTING SILVER LAYER LOAD <<<' AS Info;

-- =============================================================================
-- 1. TRANSFORM CRM DATA
-- =============================================================================

-- [1.1] crm_cust_info: Clean names, standardize gender/status, and deduplicate
SELECT '... Processing silver.crm_cust_info' AS Progress;
DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info AS 
SELECT 
    NULLIF(cust_id, 0) AS cust_id,
    cust_key,
    TRIM(cust_first_name) AS cust_first_name,
    TRIM(cust_last_name) AS cust_last_name,
    CASE 
        WHEN cust_marital_status = 'M' THEN 'Married'
        WHEN cust_marital_status = 'S' THEN 'Single'
        ELSE 'Unknown'
    END AS cust_marital_status,
    CASE 
        WHEN cust_gender = 'M' THEN 'Male'
        WHEN cust_gender = 'F' THEN 'Female'
        ELSE 'Unknown'
    END AS cust_gender,
    CASE 
        WHEN cust_create_date = 0000-00-00 THEN NULL 
        ELSE cust_create_date 
    END AS cust_create_date,
    CURRENT_TIMESTAMP AS dwh_create_date,
    CURRENT_TIMESTAMP AS dwh_update_date
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY cust_create_date DESC) AS rn
    FROM bronze.crm_cust_info
    WHERE cust_id IS NOT NULL AND cust_id != 0
) t 
WHERE rn = 1;

-- [1.2] crm_prd_info: Extract categories and calculate product end dates
SELECT '... Processing silver.crm_prd_info' AS Progress;
DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info AS 
SELECT 
    prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS prd_cat,
    SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
    TRIM(prd_nm) AS prd_nm,
    COALESCE(prd_cost, 0) AS prd_cost,
    CASE TRIM(UPPER(prd_line))
        WHEN 'R' THEN 'Road'
        WHEN 'M' THEN 'Mountain'
        WHEN 'T' THEN 'Touring'
        WHEN 'S' THEN 'Standard'
        ELSE 'n/a'
    END AS prd_line,
    prd_start_dt,
    DATE_SUB(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt), INTERVAL 1 DAY) AS prd_end_dt,
    CURRENT_TIMESTAMP AS dwh_create_date,
    CURRENT_TIMESTAMP AS dwh_update_date
FROM bronze.crm_prd_info;

-- [1.3] crm_sales_details: Fix negative prices and recalculate total sales
SELECT '... Processing silver.crm_sales_details' AS Progress;
DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details AS 
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0000-00-00 THEN NULL ELSE sls_order_dt END AS sls_order_dt, 
    CASE WHEN sls_ship_dt  = 0000-00-00 THEN NULL ELSE sls_ship_dt END AS sls_ship_dt, 
    CASE WHEN sls_due_dt   = 0000-00-00 THEN NULL ELSE sls_due_dt END AS sls_due_dt, 
    ABS(sls_quantity * sls_price) AS sls_sales,
    sls_quantity,
    sls_price,
    CURRENT_TIMESTAMP AS dwh_create_date,
    CURRENT_TIMESTAMP AS dwh_update_date
FROM (
    SELECT *,
        CASE 
            WHEN sls_price <= 0 OR sls_price IS NULL THEN ROUND(sls_sales / NULLIF(sls_quantity, 0))
            ELSE sls_price 
        END AS sls_price
    FROM bronze.crm_sales_details
) t;

-- =============================================================================
-- 2. TRANSFORM ERP DATA
-- =============================================================================

-- [2.1] erp_cust_az12: Normalize gender and strip prefix from CID
SELECT '... Processing silver.erp_cust_az12' AS Progress;
DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12 AS 
SELECT 
    SUBSTRING(cid, 4, LENGTH(cid)) AS cid,
    CASE WHEN bdate > CURDATE() THEN NULL ELSE bdate END AS bdate,
    CASE 
        WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
        ELSE 'Unknown'
    END AS gen 
FROM bronze.erp_cust_az12;

-- [2.2] erp_loc_a101: Standardize country names and remove dashes from CID
SELECT '... Processing silver.erp_loc_a101' AS Progress;
DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101 AS 
SELECT 
    REPLACE(cid, '-', '') AS cid, 
    CASE 
        WHEN TRIM(cntry) = 'DE' THEN 'Germany'
        WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
        WHEN NULLIF(TRIM(cntry), '') IS NULL THEN 'n/a'
        ELSE TRIM(cntry)
    END AS cntry
FROM bronze.erp_loc_a101;

-- [2.3] erp_px_cat_g1v2: Clean category mapping
SELECT '... Processing silver.erp_px_cat_g1v2' AS Progress;
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
CREATE TABLE silver.erp_px_cat_g1v2 AS 
SELECT 
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2;

SELECT '>>> SILVER LAYER LOAD COMPLETED <<<' AS Info;
