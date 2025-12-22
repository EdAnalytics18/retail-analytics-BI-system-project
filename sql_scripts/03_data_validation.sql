/* =============================================================================
   DATA PROFILING & INGESTION VALIDATION
   -----------------------------------------------------------------------------
   Executive Purpose:
   This section performs initial validation checks immediately after raw
   data ingestion. Its goal is to confirm that source data landed correctly
   and is structurally sound before any transformations or business logic
   are applied.

   This step mirrors how analytics teams protect dashboards and KPIs from
   upstream data issues.

   -----------------------------------------------------------------------------
   Business Value:
   - Ensures dashboards are not built on missing or corrupted data
   - Detects ingestion failures early (before reports are affected)
   - Builds confidence in data reliability for stakeholders
   - Creates a clear quality checkpoint in the data pipeline

   -----------------------------------------------------------------------------
   Validation Strategy:
   1) Sample inspection (TOP records)
      - Confirms column alignment and delimiter correctness
      - Helps spot obvious anomalies (shifted columns, null floods)

   2) Record count validation
      - Confirms expected data volume per source
      - Enables quick comparison against source system row counts
============================================================================= */


-- ============================================================================
-- 1. SAMPLE DATA INSPECTION (VISUAL SANITY CHECK)
-- ----------------------------------------------------------------------------
-- Purpose:
-- Review a small subset of records from each raw table to ensure:
-- - Data loaded into correct columns
-- - No unexpected truncation or formatting issues
-- - Timestamps, identifiers, and amounts look reasonable
--
-- This step is especially useful during initial onboarding of new data sources
-- or when file formats change.
-- ============================================================================

SELECT TOP 10 * FROM staging.pos_transactions_raw;
SELECT TOP 10 * FROM staging.pos_items_raw;

SELECT TOP 10 * FROM staging.ecom_orders_raw;
SELECT TOP 10 * FROM staging.ecom_items_raw;

SELECT TOP 10 * FROM staging.inventory_snapshots_raw;
SELECT TOP 10 * FROM staging.returns_raw;

SELECT TOP 10 * FROM staging.products_raw;
SELECT TOP 10 * FROM staging.stores_raw;
GO


-- ============================================================================
-- 2. RECORD COUNT VALIDATION (COMPLETENESS CHECK)
-- ----------------------------------------------------------------------------
-- Purpose:
-- Validate that each dataset loaded the expected number of records.
-- This is one of the fastest and most effective checks for:
-- - Empty files
-- - Partial loads
-- - Incorrect file mappings
--
-- In production, these counts would typically be compared against:
-- - Source system exports
-- - Control totals from upstream teams
-- - Historical load patterns
-- ============================================================================

SELECT 'POS Transactions'   AS table_name, COUNT(*) AS record_count FROM staging.pos_transactions_raw
UNION ALL
SELECT 'POS Items',                        COUNT(*) FROM staging.pos_items_raw
UNION ALL
SELECT 'E-Commerce Orders',                COUNT(*) FROM staging.ecom_orders_raw
UNION ALL
SELECT 'E-Commerce Items',                 COUNT(*) FROM staging.ecom_items_raw
UNION ALL
SELECT 'Inventory Snapshots',              COUNT(*) FROM staging.inventory_snapshots_raw
UNION ALL 
SELECT 'Returns',                          COUNT(*) FROM staging.returns_raw
UNION ALL
SELECT 'Products',                         COUNT(*) FROM staging.products_raw
UNION ALL
SELECT 'Stores',                           COUNT(*) FROM staging.stores_raw;
GO
