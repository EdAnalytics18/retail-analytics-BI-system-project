#  SQL Scripts — ELT Pipeline & Data Modeling

This folder contains the **SQL-based ELT pipeline** used to build the Retail
Analytics BI System.

The scripts implement a **layered warehouse architecture** (Bronze → Silver →
Gold) and are designed to be **idempotent**, **auditable**, and **production-style**.

Scripts are intended to be executed **sequentially** in numeric order.

---

##  Execution Order & Purpose

### `00_database_and_schemas.sql`
Creates the data warehouse and core schemas used throughout the project.

**Responsibilities**
- Create database (if not exists)
- Define schemas:
  - `staging` (raw and cleaned staging)
  - `core` (fact and dimension tables)
  - `analytics` (business-ready views)

---

### `01_staging_raw_tables.sql`
Defines **raw staging (Bronze) tables** that mirror source data structures.

**Responsibilities**
- Create raw tables for all source datasets
- Preserve source data exactly as received (schema-on-read)
- Avoid transformations or data loss at this stage

---

### `02_bulk_ingestion.sql`
Handles **data ingestion** from CSV files into raw staging tables.

**Responsibilities**
- Load CSV extracts using `BULK INSERT`
- Capture ingestion metadata for traceability
- Ensure repeatable, batch-oriented data loading

---

### `03_data_validation.sql`
Implements **data quality checks** on raw and cleaned staging data.

**Responsibilities**
- Validate data types and required fields
- Identify nulls, duplicates, and invalid values
- Flag data quality issues without dropping records

---

### `04_staging_cleaning.sql`
Transforms raw data into **clean, standardized staging tables (Silver)**.

**Responsibilities**
- Standardize data types using `TRY_CONVERT`
- Normalize categorical fields
- Deduplicate deterministically using window functions
- Reconcile financial metrics where required

---

### `05_dimension_tables.sql`
Creates and populates **conformed dimension tables**.

**Responsibilities**
- Build dimensions such as:
  - `dim_date`
  - `dim_product`
  - `dim_store`
- Assign surrogate keys
- Ensure consistent attributes across all star schemas

---

### `06_fact_tables.sql`
Creates and populates **fact tables** at clearly defined grains.

**Responsibilities**
- Build fact tables including:
  - `fact_pos_transactions`
  - `fact_ecom_orders`
  - `fact_sales_items`
  - `fact_inventory_snapshots`
  - `fact_returns`
- Enforce correct granularity and metric integrity
- Support analytical and BI workloads

---

### `07_constraints_and_indexes.sql`
Applies **data integrity and performance optimizations**.

**Responsibilities**
- Define primary keys and foreign key constraints
- Enforce referential integrity
- Create indexes optimized for analytical queries

---

### `08_analytics_views.sql`
Defines **analytics-layer (Gold) SQL views** used for reporting and dashboards.

**Responsibilities**
- Encapsulate complex joins and business logic
- Define consistent KPI calculations
- Provide BI-ready, read-optimized views

---

##  Design Principles

- **ELT-first architecture**: Transform data after loading
- **Dimensional modeling**: Star schemas optimized for analytics
- **Idempotent scripts**: Safe to re-run without side effects
- **Data quality over deletion**: Flag issues instead of dropping data
- **Separation of concerns**: Clear boundaries between layers

---

##  Relationship to Other Folders

- Business questions and KPI definitions are documented in:
  - `analytics/business_questions.md`
  - `analytics/metrics_definitions.md`

- Example analytical queries are available in:
  - `analytics/sample_queries.sql`

- BI dashboards consume views defined in:
  - `08_analytics_views.sql`

---

##  Notes

- Scripts are written for **SQL Server**
- Designed for batch processing using CSV extracts
- Can be adapted to other data warehouses with minimal changes
