/*
===============================================================================
Database Initialization Script
===============================================================================
Scope: This script initializes the Data Warehouse environment. 
It creates the main project database and the three architectural layers.

WARNING: Running this script will DROP the existing databases, 
resulting in PERMANENT DATA LOSS. Use with caution.
===============================================================================
*/

-- 1. Create the Main Project Database
-- In MySQL, we check if it exists before dropping to avoid errors.
DROP DATABASE IF EXISTS DataWarehouse;
CREATE DATABASE DataWarehouse;

-- 2. Create the Architectural Layers (Schemas)
-- As discussed, in MySQL these will appear as separate databases in your sidebar.
-- We create them to keep the Raw, Cleaned, and Final data separated.

-- BRONZE: The landing zone for raw data (Directly from CSVs)
DROP DATABASE IF EXISTS bronze;
CREATE DATABASE bronze;

-- SILVER: The cleaning zone (Standardized and validated data)
DROP DATABASE IF EXISTS silver;
CREATE DATABASE silver;

-- GOLD: The reporting zone (Optimized Star Schema for Business Intelligence)
DROP DATABASE IF EXISTS gold;
CREATE DATABASE gold;

/*
Note for GitHub: 
In MySQL, 'SCHEMA' and 'DATABASE' are synonyms. 
Executing 'CREATE SCHEMA bronze' is the same as 'CREATE DATABASE bronze'.
*/
