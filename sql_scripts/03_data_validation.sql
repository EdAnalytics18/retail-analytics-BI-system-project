/* =============================================================================
   DATA PROFILING & INGESTION VALIDATION
   -----------------------------------------------------------------------------
   Purpose:
   - Confirm data successfully loaded
   - Inspect samples for anomalies
   - Validate record completeness per source
============================================================================= */
SELECT TOP 10 * FROM staging.pos_transactions_raw;
SELECT TOP 10 * FROM staging.pos_items_raw;

SELECT TOP 10 * FROM staging.ecom_orders_raw;
SELECT TOP 10 * FROM staging.ecom_items_raw;

SELECT TOP 10 * FROM staging.inventory_snapshots_raw;
SELECT TOP 10 * FROM staging.returns_raw;

SELECT TOP 10 * FROM staging.products_raw;
SELECT TOP 10 * FROM staging.stores_raw;
GO


SELECT 'POS Transactions' AS table_name, COUNT(*) AS record_count FROM staging.pos_transactions_raw
UNION ALL
SELECT 'POS Items',                      COUNT(*) FROM staging.pos_items_raw
UNION ALL
SELECT 'E-Commerce Orders',              COUNT(*) FROM staging.ecom_orders_raw
UNION ALL
SELECT 'E-Commerce Items',               COUNT(*) FROM staging.ecom_items_raw
UNION ALL
SELECT 'Inventory Snapshots',            COUNT(*) FROM staging.inventory_snapshots_raw
UNION ALL 
SELECT 'Returns',                        COUNT(*) FROM staging.returns_raw
UNION ALL
SELECT 'Products',                       COUNT(*) FROM staging.products_raw
UNION ALL
SELECT 'Stores',                         COUNT(*) FROM staging.stores_raw
GO
