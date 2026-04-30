/*
===============================================================================
Script: Load Gold Layer (Star Schema)
===============================================================================
Description: 
    This script finalizes the Data Warehouse by creating the 'Gold' layer.
    It follows a Star Schema design consisting of Dimensions and Facts.
    
    Transformations:
    - Generates Surrogate Keys using ROW_NUMBER().
    - Integrates CRM and ERP data for a unified view of Customers and Products.
    - Resolves data gaps (e.g., filling 'Unknown' gender from ERP records).
    - Maps transactional sales data to the master Dimension keys.

Author: Zain
Project: Medallion Architecture - Data Warehouse Project
===============================================================================
*/

USE gold;

-- =============================================================================
-- 1. DIMENSION TABLES
-- =============================================================================

-- [1.1] gold.dim_customers: Integrated view of Customer records
-- Merges demographic data from CRM and ERP sources for a single source of truth.
SELECT '... Creating gold.dim_customers' AS Progress;
DROP VIEW IF EXISTS gold.dim_customers;
CREATE VIEW gold.dim_customers AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY ci.cust_id) AS id, -- Surrogate Key
        ci.cust_id AS customer_id,
        ci.cust_key AS customer_key,
        ci.cust_first_name AS first_name,
        ci.cust_last_name AS last_name,
        ci.cust_marital_status AS marital_status,
        -- Priority logic: If CRM gender is unknown, check ERP data
        CASE 
            WHEN ci.cust_gender != 'Unknown' THEN ci.cust_gender  
            ELSE COALESCE(ca.gen, 'Unknown')
        END AS gender,
        ca.bdate AS birth_date,
        cl.cntry AS country,
        ci.cust_create_date AS create_date
    FROM silver.crm_cust_info ci
    LEFT JOIN silver.erp_cust_az12 ca
        ON ca.cid = ci.cust_key
    LEFT JOIN silver.erp_loc_a101 cl
        ON cl.cid = ci.cust_key
);

-- [1.2] gold.dim_products: Master record for Product details
-- Filters only 'active' products based on current system records.
SELECT '... Creating gold.dim_products' AS Progress;
DROP VIEW IF EXISTS gold.dim_products;
CREATE VIEW gold.dim_products AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY pi.prd_start_dt, pi.prd_key) AS id, -- Surrogate Key
        pi.prd_id AS product_id,
        pi.prd_key AS product_number,
        pi.prd_nm AS product_name,
        pi.prd_cost AS cost,
        pc.maintenance,
        pi.prd_cat AS category_id,
        pc.cat AS category,
        pc.subcat AS subcategory,
        pi.prd_line AS product_line,
        pi.prd_start_dt AS start_date
    FROM silver.crm_prd_info AS pi
    LEFT JOIN silver.erp_px_cat_g1v2 AS pc
        ON pc.id = pi.prd_cat
    WHERE pi.prd_end_dt IS NULL -- Filters only current version of product
);

-- =============================================================================
-- 2. FACT TABLES
-- =============================================================================

-- [2.1] gold.fact_sales: Central record of all Sales transactions
-- Joins Sales details with Dimension keys for optimized analytical performance.
SELECT '... Creating gold.fact_sales' AS Progress;
DROP VIEW IF EXISTS gold.fact_sales;
CREATE VIEW gold.fact_sales AS (
    SELECT 
        sl.sls_ord_num AS order_number,
        dp.id AS product_key,  -- Reference to dim_products surrogate key
        dc.id AS customer_key, -- Reference to dim_customers surrogate key
        sl.sls_order_dt AS order_date,
        sl.sls_ship_dt AS ship_date,
        sl.sls_due_dt AS due_date,
        sl.sls_sales AS sales_amount,
        sl.sls_quantity AS quantity,
        sl.sls_price AS price
    FROM silver.crm_sales_details sl
    LEFT JOIN gold.dim_customers dc
        ON dc.customer_id = sl.sls_cust_id
    LEFT JOIN gold.dim_products dp
        ON dp.product_number = sl.sls_prd_key
);

-- Verification Log
SELECT '>>> GOLD LAYER LOAD COMPLETED <<<' AS Info;
