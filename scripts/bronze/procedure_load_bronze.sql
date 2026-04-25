/*
===============================================================================
Stored Procedure: load_bronze
===============================================================================
Description: 
    Full ETL process for the Bronze Layer.
    Includes time tracking and status messaging for monitoring.
    
Author: Zain
Project: Data Warehouse (Baraa Masterclass)
===============================================================================
*/

DELIMITER //

CREATE PROCEDURE load_bronze()
BEGIN
    -- Declare variables for time tracking
    DECLARE v_start_time DATETIME;
    DECLARE v_end_time DATETIME;
    
    -- Capture starting time
    SET v_start_time = NOW();

    SELECT '>>> STARTING BRONZE LAYER ETL PROCESS <<<' AS Status;
    SELECT CONCAT('Process started at: ', v_start_time) AS Start_Time;

    -- -------------------------------------------------------------------------
    -- LOADING CRM DATA
    -- -------------------------------------------------------------------------
    SELECT '... Loading CRM Tables' AS Progress;

    TRUNCATE TABLE crm_cust_info;
    LOAD DATA LOCAL INFILE 'C:/Users/zaint/OneDrive/Desktop/SQL_Project_Baraa/datasets/source_crm/cust_info.csv'
    INTO TABLE crm_cust_info FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;
    SELECT '    -> Table: crm_cust_info Loaded' AS Status;

    TRUNCATE TABLE crm_prd_info;
    LOAD DATA LOCAL INFILE 'C:/Users/zaint/OneDrive/Desktop/SQL_Project_Baraa/datasets/source_crm/prd_info.csv'
    INTO TABLE crm_prd_info FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;
    SELECT '    -> Table: crm_prd_info Loaded' AS Status;

    TRUNCATE TABLE crm_sales_details;
    LOAD DATA LOCAL INFILE 'C:/Users/zaint/OneDrive/Desktop/SQL_Project_Baraa/datasets/source_crm/sales_details.csv'
    INTO TABLE crm_sales_details FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;
    SELECT '    -> Table: crm_sales_details Loaded' AS Status;

    -- -------------------------------------------------------------------------
    -- LOADING ERP DATA
    -- -------------------------------------------------------------------------
    SELECT '... Loading ERP Tables' AS Progress;

    TRUNCATE TABLE erp_cust_az12;
    LOAD DATA LOCAL INFILE 'C:/Users/zaint/OneDrive/Desktop/SQL_Project_Baraa/datasets/source_erp/CUST_AZ12.csv'
    INTO TABLE erp_cust_az12 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;
    SELECT '    -> Table: erp_cust_az12 Loaded' AS Status;

    TRUNCATE TABLE erp_loc_a101;
    LOAD DATA LOCAL INFILE 'C:/Users/zaint/OneDrive/Desktop/SQL_Project_Baraa/datasets/source_erp/LOC_A101.csv'
    INTO TABLE erp_loc_a101 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;
    SELECT '    -> Table: erp_loc_a101 Loaded' AS Status;

    TRUNCATE TABLE erp_px_cat_g1v2;
    LOAD DATA LOCAL INFILE 'C:/Users/zaint/OneDrive/Desktop/SQL_Project_Baraa/datasets/source_erp/PX_CAT_G1V2.csv'
    INTO TABLE erp_px_cat_g1v2 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\r\n' IGNORE 1 ROWS;
    SELECT '    -> Table: erp_px_cat_g1v2 Loaded' AS Status;

    -- -------------------------------------------------------------------------
    -- FINALIZE TIME TRACKING
    -- -------------------------------------------------------------------------
    SET v_end_time = NOW();
    
    SELECT '>>> BRONZE LAYER ETL COMPLETED <<<' AS Status;
    SELECT CONCAT('Total Duration: ', TIMESTAMPDIFF(SECOND, v_start_time, v_end_time), ' seconds') AS Runtime_Stats;

END //

DELIMITER ;
