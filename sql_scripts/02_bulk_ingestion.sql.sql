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
    @schema_name SYSNAME,
    @table_name  SYSNAME,
    @base_path   NVARCHAR(4000),
    @file_name   NVARCHAR(255)
)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql        NVARCHAR(MAX);
    DECLARE @full_table SYSNAME =
        QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name);
    DECLARE @full_path  NVARCHAR(4000) =
        @base_path + @file_name;

    BEGIN TRY
        RAISERROR (
            '[START] Loading %s from %s',
            0, 1, @full_table, @full_path
        ) WITH NOWAIT;

        /* Step 1: Truncate */
        RAISERROR (
            '[STEP] Truncating table %s',
            0, 1, @full_table
        ) WITH NOWAIT;

        SET @sql = N'TRUNCATE TABLE ' + @full_table;
        EXEC sp_executesql @sql;

        /* Step 2: Bulk Insert */
        RAISERROR (
            '[STEP] Bulk inserting from %s',
            0, 1, @full_path
        ) WITH NOWAIT;

        SET @sql = N'
            BULK INSERT ' + @full_table + '
            FROM ''' + @full_path + '''
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = '','',
                ROWTERMINATOR = ''0x0A'',
                TABLOCK,
                KEEPNULLS
            );
        ';
        EXEC sp_executesql @sql;

        RAISERROR (
            '[SUCCESS] Finished loading %s',
            0, 1, @full_table
        ) WITH NOWAIT;
    END TRY
    BEGIN CATCH
        RAISERROR (
            '[ERROR] Load failed for %s | %s',
            16, 1, @full_table, ERROR_MESSAGE()
        );
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

RAISERROR ('[CONFIG] Initializing ingestion configuration', 0, 1) WITH NOWAIT;

DECLARE @base_path NVARCHAR(4000)
    = 'C:\Retail Analytics BI System Project\raw_data\';

RAISERROR (
    '[CONFIG] Base path set to %s',
    0, 1, @base_path
) WITH NOWAIT;


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

RAISERROR ('[PIPELINE] Starting raw data ingestion pipeline', 0, 1) WITH NOWAIT;

-- POS Transactions
EXEC staging.usp_load_raw_csv 'staging','pos_transactions_raw',@base_path,'pos_transactions_raw.csv';

-- POS Order Items
EXEC staging.usp_load_raw_csv 'staging','pos_items_raw',@base_path,'pos_order_items_raw.csv';

-- E-Commerce Orders
EXEC staging.usp_load_raw_csv 'staging','ecom_orders_raw',@base_path,'ecom_orders_raw.csv';

-- E-Commerce Order Items
EXEC staging.usp_load_raw_csv 'staging','ecom_items_raw',@base_path,'ecom_order_items_raw.csv';

-- Inventory Snapshots
EXEC staging.usp_load_raw_csv 'staging','inventory_snapshots_raw',@base_path,'inventory_raw.csv';

-- Returns
EXEC staging.usp_load_raw_csv 'staging','returns_raw',@base_path,'returns_raw.csv';

-- Products
EXEC staging.usp_load_raw_csv 'staging','products_raw',@base_path,'products_raw.csv';

-- Stores
EXEC staging.usp_load_raw_csv 'staging','stores_raw',@base_path,'stores_raw.csv';

RAISERROR ('[PIPELINE] Raw data ingestion completed', 0, 1) WITH NOWAIT;

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

RAISERROR (
    '[METADATA] Starting metadata enrichment',
    0, 1
) WITH NOWAIT;

-- POS TRANSACTIONS
RAISERROR ('[METADATA] Enriching pos_transactions_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.pos_transactions_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.pos_transactions_raw
SET load_timestamp = GETDATE(),
    source_file    = 'pos_transactions_raw.csv';

RAISERROR ('[METADATA] Completed pos_transactions_raw', 0, 1) WITH NOWAIT;

-- POS ORDER ITEMS
RAISERROR ('[METADATA] Enriching pos_items_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.pos_items_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.pos_items_raw
SET load_timestamp = GETDATE(),
    source_file    = 'pos_order_items_raw.csv';

RAISERROR ('[METADATA] Completed pos_items_raw', 0, 1) WITH NOWAIT;

-- E-COMMERCE ORDERS
RAISERROR ('[METADATA] Enriching ecom_orders_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.ecom_orders_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.ecom_orders_raw
SET load_timestamp = GETDATE(),
    source_file    = 'ecom_orders_raw.csv';

RAISERROR ('[METADATA] Completed ecom_orders_raw', 0, 1) WITH NOWAIT;

-- E-COMMERCE ORDER ITEMS
RAISERROR ('[METADATA] Enriching ecom_items_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.ecom_items_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.ecom_items_raw
SET load_timestamp = GETDATE(),
    source_file    = 'ecom_order_items_raw.csv';

RAISERROR ('[METADATA] Completed ecom_items_raw', 0, 1) WITH NOWAIT;

-- INVENTORY SNAPSHOTS
RAISERROR ('[METADATA] Enriching inventory_snapshots_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.inventory_snapshots_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.inventory_snapshots_raw
SET load_timestamp = GETDATE(),
    source_file    = 'inventory_raw.csv';

RAISERROR ('[METADATA] Completed inventory_snapshots_raw', 0, 1) WITH NOWAIT;

-- RETURNS
RAISERROR ('[METADATA] Enriching returns_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.returns_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.returns_raw
SET load_timestamp = GETDATE(),
    source_file    = 'returns_raw.csv';

RAISERROR ('[METADATA] Completed returns_raw', 0, 1) WITH NOWAIT;

-- PRODUCTS
RAISERROR ('[METADATA] Enriching products_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.products_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.products_raw
SET load_timestamp = GETDATE(),
    source_file    = 'products_raw.csv';

RAISERROR ('[METADATA] Completed products_raw', 0, 1) WITH NOWAIT;

-- STORES
RAISERROR ('[METADATA] Enriching stores_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.stores_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.stores_raw
SET load_timestamp = GETDATE(),
    source_file    = 'stores_raw.csv';

RAISERROR ('[METADATA] Completed stores_raw', 0, 1) WITH NOWAIT;

