/*
===============================================================================
Script: Initialize Bronze Layer
===============================================================================
Description: 
    This script creates the raw staging tables for the Bronze layer. 
    It drops existing tables to ensure a clean slate and loads data from 
    CSV files using the LOAD DATA LOCAL INFILE command.
    
Author: Zain
Project: Data Warehouse (Baraa Masterclass)
===============================================================================
*/

USE bronze;

-- =============================================================================
-- 1. CREATE TABLES (CRM Source)
-- =============================================================================

DROP TABLE IF EXISTS crm_cust_info;
CREATE TABLE crm_cust_info (
    cust_id             INT,
    cust_key            VARCHAR(30),
    cust_first_name     VARCHAR(30),
    cust_last_name      VARCHAR(30),
    cust_marital_status VARCHAR(1),
    cust_gender         VARCHAR(1),
    cust_create_date    DATE
);

DROP TABLE IF EXISTS crm_prd_info;
CREATE TABLE crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(30),
    prd_nm       VARCHAR(30),
    prd_cost     INT,
    prd_line     VARCHAR(3),
    prd_start_dt DATE,
    prd_end_dt   DATE
);

DROP TABLE IF EXISTS crm_sales_details;
CREATE TABLE crm_sales_details (
    sls_ord_num  VARCHAR(30),
    sls_prd_key  VARCHAR(30),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

-- =============================================================================
-- 2. CREATE TABLES (ERP Source)
-- =============================================================================

DROP TABLE IF EXISTS erp_cust_az12;
CREATE TABLE erp_cust_az12 (
    cid   VARCHAR(50),
    bdate DATE,
    gen   VARCHAR(10)
);

DROP TABLE IF EXISTS erp_loc_a101;
CREATE TABLE erp_loc_a101 (
    cid   VARCHAR(50),
    cntry VARCHAR(50)
);

DROP TABLE IF EXISTS erp_px_cat_g1v2;
CREATE TABLE erp_px_cat_g1v2 (
    id          VARCHAR(50),
    cat         VARCHAR(50),
    subcat      VARCHAR(50),
    maintenance VARCHAR(10)
);

-- =============================================================================
-- 3. DATA LOADING (Truncate and Load)
-- =============================================================================

-- CRM Data
TRUNCATE TABLE crm_cust_info;
LOAD DATA LOCAL INFILE 'C:/Users/zaint/OneDrive/Desktop/SQL_Project_Baraa/datasets/source_crm/cust_info.csv'
INTO TABLE crm_cust_info FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;

TRUNCATE TABLE crm_prd_info;
LOAD DATA LOCAL INFILE 'C:/Users/zaint/OneDrive/Desktop/SQL_Project_Baraa/datasets/source_crm/prd_info.csv'
INTO TABLE crm_prd_info FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;

TRUNCATE TABLE crm_sales_details;
LOAD DATA LOCAL INFILE 'C:/Users/zaint/OneDrive/Desktop/SQL_Project_Baraa/datasets/source_crm/sales_details.csv'
INTO TABLE crm_sales_details FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;

-- ERP Data
TRUNCATE TABLE erp_cust_az12;
LOAD DATA LOCAL INFILE 'C:/Users/zaint/OneDrive/Desktop/SQL_Project_Baraa/datasets/source_erp/CUST_AZ12.csv'
INTO TABLE erp_cust_az12 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;

TRUNCATE TABLE erp_loc_a101;
LOAD DATA LOCAL INFILE 'C:/Users/zaint/OneDrive/Desktop/SQL_Project_Baraa/datasets/source_erp/LOC_A101.csv'
INTO TABLE erp_loc_a101 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;

TRUNCATE TABLE erp_px_cat_g1v2;
LOAD DATA LOCAL INFILE 'C:/Users/zaint/OneDrive/Desktop/SQL_Project_Baraa/datasets/source_erp/PX_CAT_G1V2.csv'
INTO TABLE erp_px_cat_g1v2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;
