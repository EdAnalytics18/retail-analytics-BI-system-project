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
   Standardized ingestion utility for loading CSV files into raw (bronze)
   staging tables within the data warehouse.

   Instead of maintaining one-off BULK INSERT scripts per dataset, this
   procedure centralizes ingestion logic into a single, reusable component
   aligned with production ETL / ELT design patterns.

   -----------------------------------------------------------------------------
   What This Procedure Does:
   - Fully reloads the target raw table (idempotent behavior)
   - Loads CSV data using BULK INSERT without applying transformations
   - Skips header rows automatically
   - Preserves source NULL values
   - Supports parameterized ingestion across multiple datasets

   -----------------------------------------------------------------------------
   Operational Guarantees:
   - Consistent ingestion behavior across all raw tables
   - Real-time logging for pipeline observability
   - Fail-fast error propagation to upstream orchestration layers

   -----------------------------------------------------------------------------
   Why This Matters:
   - Reduces copy/paste ingestion logic and operational risk
   - Makes onboarding new raw datasets trivial
   - Enforces discipline and consistency at the warehouse entry point
============================================================================= */

CREATE OR ALTER PROCEDURE staging.usp_load_raw_csv
(
    @schema_name SYSNAME,        -- Target schema
    @table_name  SYSNAME,        -- Target table
    @base_path   NVARCHAR(4000), -- Directory containing CSV files
    @file_name   NVARCHAR(255)   -- CSV file name
)
AS
BEGIN
    SET NOCOUNT ON;

    -- Dynamic SQL container
    DECLARE @sql NVARCHAR(MAX);

    -- Fully-qualified, safely quoted table name
    DECLARE @full_table SYSNAME =
        QUOTENAME(@schema_name) + '.' + QUOTENAME(@table_name);

    -- Full CSV file path
    DECLARE @full_path NVARCHAR(4000) =
        @base_path + @file_name;

    BEGIN TRY
        -- Pipeline start marker
        RAISERROR (
            '[START] Loading %s from %s',
            0, 1, @full_table, @full_path
        ) WITH NOWAIT;

        /* Step 1: Truncate target table
           Ensures a clean, repeatable raw load */
        RAISERROR (
            '[STEP] Truncating table %s',
            0, 1, @full_table
        ) WITH NOWAIT;

        SET @sql = N'TRUNCATE TABLE ' + @full_table;
        EXEC sp_executesql @sql;

        /* Step 2: Bulk insert CSV data
           No transformations applied (raw system-of-record) */
        RAISERROR (
            '[STEP] Bulk inserting from %s',
            0, 1, @full_path
        ) WITH NOWAIT;

        SET @sql = N'
            BULK INSERT ' + @full_table + '
            FROM ''' + @full_path + '''
            WITH (
                FIRSTROW = 2,          -- Skip header row
                FIELDTERMINATOR = '','',
                ROWTERMINATOR = ''0x0A'',
                TABLOCK,               -- Improve bulk load performance
                KEEPNULLS              -- Preserve source NULLs
            );
        ';
        EXEC sp_executesql @sql;

        -- Successful completion marker
        RAISERROR (
            '[SUCCESS] Finished loading %s',
            0, 1, @full_table
        ) WITH NOWAIT;
    END TRY
    BEGIN CATCH
        -- Surface contextual error and propagate failure
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
   Centralized configuration for the file system location containing all
   raw CSV extracts consumed by the ingestion pipeline.

   In production environments, this path would typically reference:
   - A secure network file share
   - An SFTP landing zone
   - Cloud object storage (accessed via external tables or orchestration tools)

   -----------------------------------------------------------------------------
   Operational Notes:
   - The SQL Server service account must have READ access to this directory
   - Path structure is assumed to be stable across ingestion runs
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
   Orchestrates raw data ingestion by invoking the generic CSV loader
   once per dataset.

   Each EXEC statement represents an explicit, traceable ingestion step,
   conceptually similar to tasks in orchestration tools such as Airflow,
   SSIS, or Azure Data Factory.

   -----------------------------------------------------------------------------
   Business Impact:
   - Makes ingestion flow transparent and easy to audit
   - Simplifies onboarding of new raw data sources
   - Keeps pipeline logic readable, maintainable, and deterministic
============================================================================= */

RAISERROR ('[PIPELINE] Starting raw data ingestion pipeline', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- POS Transactions
----------------------------------------------------------------
RAISERROR ('[INGEST] Starting POS transactions ingestion', 0, 1) WITH NOWAIT;

EXEC staging.usp_load_raw_csv
    'staging',
    'pos_transactions_raw',
    @base_path,
    'pos_transactions_raw.csv';

RAISERROR ('[INGEST] Completed POS transactions ingestion', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- POS Order Items
----------------------------------------------------------------
RAISERROR ('[INGEST] Starting POS order items ingestion', 0, 1) WITH NOWAIT;

EXEC staging.usp_load_raw_csv
    'staging',
    'pos_items_raw',
    @base_path,
    'pos_order_items_raw.csv';

RAISERROR ('[INGEST] Completed POS order items ingestion', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- E-Commerce Orders
----------------------------------------------------------------
RAISERROR ('[INGEST] Starting E-Commerce orders ingestion', 0, 1) WITH NOWAIT;

EXEC staging.usp_load_raw_csv
    'staging',
    'ecom_orders_raw',
    @base_path,
    'ecom_orders_raw.csv';

RAISERROR ('[INGEST] Completed E-Commerce orders ingestion', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- E-Commerce Order Items
----------------------------------------------------------------
RAISERROR ('[INGEST] Starting E-Commerce order items ingestion', 0, 1) WITH NOWAIT;

EXEC staging.usp_load_raw_csv
    'staging',
    'ecom_items_raw',
    @base_path,
    'ecom_order_items_raw.csv';

RAISERROR ('[INGEST] Completed E-Commerce order items ingestion', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- Inventory Snapshots
----------------------------------------------------------------
RAISERROR ('[INGEST] Starting inventory snapshots ingestion', 0, 1) WITH NOWAIT;

EXEC staging.usp_load_raw_csv
    'staging',
    'inventory_snapshots_raw',
    @base_path,
    'inventory_raw.csv';

RAISERROR ('[INGEST] Completed inventory snapshots ingestion', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- Returns
----------------------------------------------------------------
RAISERROR ('[INGEST] Starting returns ingestion', 0, 1) WITH NOWAIT;

EXEC staging.usp_load_raw_csv
    'staging',
    'returns_raw',
    @base_path,
    'returns_raw.csv';

RAISERROR ('[INGEST] Completed returns ingestion', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- Products
----------------------------------------------------------------
RAISERROR ('[INGEST] Starting products ingestion', 0, 1) WITH NOWAIT;

EXEC staging.usp_load_raw_csv
    'staging',
    'products_raw',
    @base_path,
    'products_raw.csv';

RAISERROR ('[INGEST] Completed products ingestion', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- Stores
----------------------------------------------------------------
RAISERROR ('[INGEST] Starting stores ingestion', 0, 1) WITH NOWAIT;

EXEC staging.usp_load_raw_csv
    'staging',
    'stores_raw',
    @base_path,
    'stores_raw.csv';

RAISERROR ('[INGEST] Completed stores ingestion', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
RAISERROR ('[PIPELINE] Raw data ingestion completed', 0, 1) WITH NOWAIT;


/* =============================================================================
   4. METADATA ENRICHMENT – DATA LINEAGE & TRACEABILITY
   -----------------------------------------------------------------------------
   Purpose:
   Enriches raw tables with ingestion metadata after successful load
   completion.

   This enables:
   - End-to-end data lineage tracking
   - Auditability for compliance, debugging, and root-cause analysis
   - Visibility into when, how, and from where data was ingested

   -----------------------------------------------------------------------------
   Why This Matters to the Business:
   When metrics are questioned, teams can quickly answer:
   - Which source file produced this data?
   - When was it loaded?
   - Can it be safely reprocessed?
============================================================================= */

RAISERROR (
    '[METADATA] Starting metadata enrichment',
    0, 1
) WITH NOWAIT;

----------------------------------------------------------------
-- POS Transactions
----------------------------------------------------------------
RAISERROR ('[METADATA] Enriching pos_transactions_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.pos_transactions_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.pos_transactions_raw
SET load_timestamp = GETDATE(),
    source_file    = 'pos_transactions_raw.csv';

RAISERROR ('[METADATA] Completed pos_transactions_raw', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- POS Order Items
----------------------------------------------------------------
RAISERROR ('[METADATA] Enriching pos_items_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.pos_items_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.pos_items_raw
SET load_timestamp = GETDATE(),
    source_file    = 'pos_order_items_raw.csv';

RAISERROR ('[METADATA] Completed pos_items_raw', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- E-Commerce Orders
----------------------------------------------------------------
RAISERROR ('[METADATA] Enriching ecom_orders_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.ecom_orders_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.ecom_orders_raw
SET load_timestamp = GETDATE(),
    source_file    = 'ecom_orders_raw.csv';

RAISERROR ('[METADATA] Completed ecom_orders_raw', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- E-Commerce Order Items
----------------------------------------------------------------
RAISERROR ('[METADATA] Enriching ecom_items_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.ecom_items_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.ecom_items_raw
SET load_timestamp = GETDATE(),
    source_file    = 'ecom_order_items_raw.csv';

RAISERROR ('[METADATA] Completed ecom_items_raw', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- Inventory Snapshots
----------------------------------------------------------------
RAISERROR ('[METADATA] Enriching inventory_snapshots_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.inventory_snapshots_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.inventory_snapshots_raw
SET load_timestamp = GETDATE(),
    source_file    = 'inventory_raw.csv';

RAISERROR ('[METADATA] Completed inventory_snapshots_raw', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- Returns
----------------------------------------------------------------
RAISERROR ('[METADATA] Enriching returns_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.returns_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.returns_raw
SET load_timestamp = GETDATE(),
    source_file    = 'returns_raw.csv';

RAISERROR ('[METADATA] Completed returns_raw', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- Products
----------------------------------------------------------------
RAISERROR ('[METADATA] Enriching products_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.products_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.products_raw
SET load_timestamp = GETDATE(),
    source_file    = 'products_raw.csv';

RAISERROR ('[METADATA] Completed products_raw', 0, 1) WITH NOWAIT;

----------------------------------------------------------------
-- Stores
----------------------------------------------------------------
RAISERROR ('[METADATA] Enriching stores_raw', 0, 1) WITH NOWAIT;

ALTER TABLE staging.stores_raw
ADD load_timestamp DATETIME, source_file VARCHAR(255);

UPDATE staging.stores_raw
SET load_timestamp = GETDATE(),
    source_file    = 'stores_raw.csv';

RAISERROR ('[METADATA] Completed stores_raw', 0, 1) WITH NOWAIT;
