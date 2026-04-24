# Modern SQL Data Warehouse Project

## 📌 Project Overview
This project demonstrates the design and implementation of a professional-grade Data Warehouse using **MySQL**. Following a modern Medallion Architecture (Bronze, Silver, Gold), I have transformed raw, messy data into a clean, optimized Star Schema ready for business intelligence and reporting.

## 🏗️ Data Architecture
The warehouse is organized into three distinct layers to ensure data quality and performance:

- **Bronze Layer (Raw):** Direct ingestion of source data (CRM and ERP CSV files). Data is kept in its original format.
- **Silver Layer (Cleaned):** Data is standardized and cleaned. This includes handling nulls, correcting date formats, and removing duplicates.
- **Gold Layer (Reporting):** The final layer organized into a **Star Schema** (Fact and Dimension tables) for high-performance querying.



## 🛠️ Tech Stack
- **Database:** MySQL 8.0+
- **Tooling:** Visual Studio Code (with Database Client extension)
- **Language:** SQL (DDL, DML, Stored Procedures)

## 📂 Folder Structure
- `/datasets`: Raw CSV source files from ERP and CRM systems.
- `/scripts`: SQL scripts for database initialization and layer-specific logic.
    - `bronze/`: Scripts for raw data loading.
    - `silver/`: Data transformation and cleaning logic.
    - `gold/`: View and table creation for the Star Schema.
- `/docs`: Architecture diagrams and data dictionary.
- `/tests`: Quality checks to ensure data consistency between layers.

## 🚀 How to Run the Project
1. **Clone the repo:** `git clone <your-repo-link>`
2. **Initialize Database:** Run `scripts/init_database.sql` to create the schemas.
3. **Load Bronze Layer:** Execute the stored procedures in `scripts/bronze/` to import the CSV data.
4. **Transform to Silver:** Run the cleaning scripts in `scripts/silver/`.
5. **Finalize Gold Layer:** Execute the DDL in `scripts/gold/` to create the reporting model.

## 📊 Key Concepts Applied
- **Normalization vs De-normalization:** Utilizing 3NF in the cleaning stage and Star Schema for the reporting stage.
- **Stored Procedures:** Automating the ETL (Extract, Transform, Load) process.
- **Data Integrity:** Implementing Primary/Foreign Key relationships and data validation checks.

---
*Created by Zain as part of the BSCS (Computer Science) Data Engineering curriculum.*
