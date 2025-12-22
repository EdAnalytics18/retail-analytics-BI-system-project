/* =============================================================================
   RETAIL ANALYTICS BI SYSTEM
   BRONZE / RAW DATA INGESTION PIPELINE (CSV-BASED)
   =============================================================================
   Executive Summary:
   This script implements the raw (bronze) data ingestion layer of a modern
   retail analytics platform. It simulates a real-world enterprise scenario
   where operational systems deliver data as scheduled CSV extracts.

   The goal of this layer is not analytics, it is reliability.
   Data is ingested exactly as received so the organization always retains
   a complete, auditable system of record.

   -----------------------------------------------------------------------------
   Business Value:
   - Guarantees a permanent copy of original source data
   - Enables safe reprocessing when business rules or KPIs change
   - Reduces downstream reporting risk caused by silent data corruption
   - Mirrors how real analytics teams ingest data from vendors and legacy systems

   -----------------------------------------------------------------------------
   Design Philosophy:
   - Bronze-layer tables mirror source files 1:1
   - No transformations or assumptions during ingestion
   - Repeatable, automated loads (safe to re-run)
   - Centralized ingestion logic to reduce operational complexity
   - Clear separation between ingestion, transformation, and analytics
============================================================================= */


/* =============================================================================
   1. GENERIC RAW CSV LOADER (REUSABLE INGESTION PROCEDURE)
   -----------------------------------------------------------------------------
   Purpose:
   This stored procedure standardizes how CSV files are loaded into raw
   staging tables across the entire data warehouse.

   Rather than writing one-off BULK INSERT statements, ingestion is handled
   through a single, reusable component similar to production ETL frameworks.

   -----------------------------------------------------------------------------
   What This Procedure Does:
   - Clears the target raw table (idempotent behavior)
   - Loads data from a CSV file using BULK INSERT
   - Skips header rows automatically
   - Preserves NULL values from the source
   - Supports parameterized ingestion for multiple datasets

   -----------------------------------------------------------------------------
   Why This Matters:
   - Reduces copy/paste errors
   - Makes ingestion easy to extend to new data sources
   - Encourages consistency and operational discipline
============================================================================= */

CREATE OR ALTER PROCEDURE staging.usp_load_raw_csv
(
    @schema_name SYSNAME,          -- Target schema (e.g. staging)
    @table_name  SYSNAME,          -- Target raw table
    @base_path   NVARCHAR(4000),   -- Directory containing CSV files
    @file_name   NVARCHAR(255)     -- CSV file name
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql NVARCHAR(MAX);

    BEGIN TRY
        -- Step 1: Clear existing data to support repeatable loads
        SET @sql = '
            TRUNCATE TABLE '
            + QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name) + ';
        ';
        EXEC sp_executesql @sql;

        -- Step 2: Load raw CSV data without transformation
        SET @sql = '
            BULK INSERT '
            + QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name) + '
            FROM ''' + @base_path + @file_name + '''
            WITH (
                FIRSTROW = 2,         -- Skip header row
                FIELDTERMINATOR = '','',
                ROWTERMINATOR = ''0x0A'',
                TABLOCK,              -- Improves bulk load performance
                KEEPNULLS             -- Preserve original NULL values
            );
        ';
        EXEC sp_executesql @sql;

    END TRY
    BEGIN CATCH
        -- Surface ingestion failures clearly for operational visibility
        THROW;
    END CATCH
END;
GO


/* =============================================================================
   2. INGESTION CONFIGURATION
   -----------------------------------------------------------------------------
   Purpose:
   Centralizes the file system location for all raw CSV extracts.

   In production, this path would typically point to:
   - A secure file share
   - An SFTP landing zone
   - Cloud storage (via external tables or pipelines)

   -----------------------------------------------------------------------------
   Operational Note:
   The SQL Server service account must have READ access to this directory.
============================================================================= */

DECLARE @base_path NVARCHAR(4000)
    = 'C:\Retail Analytics BI System Project\raw_data\';


/* =============================================================================
   3. RAW DATA INGESTION (PIPELINE ORCHESTRATION)
   -----------------------------------------------------------------------------
   Purpose:
   This section orchestrates ingestion by invoking the generic loader
   once per dataset.

   Each EXEC statement represents a single, traceable ingestion step
   similar to tasks in tools like Airflow, SSIS, or Azure Data Factory.

   -----------------------------------------------------------------------------
   Business Impact:
   - Makes ingestion transparent and easy to audit
   - Enables quick onboarding of new data sources
   - Keeps ingestion logic readable and maintainable
============================================================================= */

-- POS Transactions (Transaction Header Level)
EXEC staging.usp_load_raw_csv
    @schema_name = 'staging',
    @table_name  = 'pos_transactions_raw',
    @base_path   = @base_path,
    @file_name   = 'pos_transactions_raw.csv';

-- POS Order Items (Transaction Line Level)
EXEC staging.usp_load_raw_csv
    @schema_name = 'staging',
    @table_name  = 'pos_items_raw',
    @base_path   = @base_path,
    @file_name   = 'pos_order_items_raw.csv';

-- E-Commerce Orders
EXEC staging.usp_load_raw_csv
    @schema_name = 'staging',
    @table_name  = 'ecom_orders_raw',
    @base_path   = @base_path,
    @file_name   = 'ecom_orders_raw.csv';

-- E-Commerce Order Items
EXEC staging.usp_load_raw_csv
    @schema_name = 'staging',
    @table_name  = 'ecom_items_raw',
    @base_path   = @base_path,
    @file_name   = 'ecom_order_items_raw.csv';

-- Inventory Snapshots
EXEC staging.usp_load_raw_csv
    @schema_name = 'staging',
    @table_name  = 'inventory_snapshots_raw',
    @base_path   = @base_path,
    @file_name   = 'inventory_raw.csv';

-- Returns
EXEC staging.usp_load_raw_csv
    @schema_name = 'staging',
    @table_name  = 'returns_raw',
    @base_path   = @base_path,
    @file_name   = 'returns_raw.csv';

-- Products Master Data
EXEC staging.usp_load_raw_csv
    @schema_name = 'staging',
    @table_name  = 'products_raw',
    @base_path   = @base_path,
    @file_name   = 'products_raw.csv';

-- Stores Master Data
EXEC staging.usp_load_raw_csv
    @schema_name = 'staging',
    @table_name  = 'stores_raw',
    @base_path   = @base_path,
    @file_name   = 'stores_raw.csv';


/* =============================================================================
   4. METADATA ENRICHMENT - DATA LINEAGE & TRACEABILITY
   -----------------------------------------------------------------------------
   Purpose:
   After raw ingestion completes successfully, ingestion metadata is added
   to each table.

   This enables:
   - Data lineage tracking
   - Auditability for compliance and debugging
   - Visibility into when and from where data was loaded

   -----------------------------------------------------------------------------
   Why This Matters to the Business:
   When numbers are questioned, the team can quickly answer:
   - Which file did this data come from?
   - When was it loaded?
   - Can it be reprocessed safely?
============================================================================= */

-- POS TRANSACTIONS
ALTER TABLE staging.pos_transactions_raw
ADD
    load_timestamp DATETIME,
    source_file    VARCHAR(255);

UPDATE staging.pos_transactions_raw
SET
    load_timestamp = GETDATE(),
    source_file    = 'pos_transactions_raw.csv';

-- POS ORDER ITEMS
ALTER TABLE staging.pos_items_raw
ADD
    load_timestamp DATETIME,
    source_file    VARCHAR(255);

UPDATE staging.pos_items_raw
SET
    load_timestamp = GETDATE(),
    source_file    = 'pos_order_items_raw.csv';

-- E-COMMERCE ORDERS
ALTER TABLE staging.ecom_orders_raw
ADD
    load_timestamp DATETIME,
    source_file    VARCHAR(255);

UPDATE staging.ecom_orders_raw
SET
    load_timestamp = GETDATE(),
    source_file    = 'ecom_orders_raw.csv';

-- E-COMMERCE ORDER ITEMS
ALTER TABLE staging.ecom_items_raw
ADD
    load_timestamp DATETIME,
    source_file    VARCHAR(255);

UPDATE staging.ecom_items_raw
SET
    load_timestamp = GETDATE(),
    source_file    = 'ecom_order_items_raw.csv';

-- INVENTORY SNAPSHOTS
ALTER TABLE staging.inventory_snapshots_raw
ADD
    load_timestamp DATETIME,
    source_file    VARCHAR(255);

UPDATE staging.inventory_snapshots_raw
SET
    load_timestamp = GETDATE(),
    source_file    = 'inventory_raw.csv';

-- RETURNS
ALTER TABLE staging.returns_raw
ADD
    load_timestamp DATETIME,
    source_file    VARCHAR(255);

UPDATE staging.returns_raw
SET
    load_timestamp = GETDATE(),
    source_file    = 'returns_raw.csv';

-- PRODUCTS
ALTER TABLE staging.products_raw
ADD
    load_timestamp DATETIME,
    source_file    VARCHAR(255);

UPDATE staging.products_raw
SET
    load_timestamp = GETDATE(),
    source_file    = 'products_raw.csv';

-- STORES
ALTER TABLE staging.stores_raw
ADD
    load_timestamp DATETIME,
    source_file    VARCHAR(255);

UPDATE staging.stores_raw
SET
    load_timestamp = GETDATE(),
    source_file    = 'stores_raw.csv';
