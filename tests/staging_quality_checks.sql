/*
===============================================================================
STAGING (SILVER) LAYER - DATA QUALITY CHECKS
===============================================================================
Script Purpose:
    This script validates data quality, consistency, and standardization
    across the cleaned staging (Silver Prep) layer.

    These checks verify that:
    - Primary keys are present and not duplicated unexpectedly
    - Data quality flags correctly identify bad records
    - Business-rule standardization is applied consistently
    - Financial reconciliation logic behaves as expected
    - Dates, quantities, and monetary values are sane

Usage Notes:
    - Run AFTER staging_cleaning.sql
    - Any returned rows indicate data quality issues
    - Expected Result for most checks: NO ROWS
===============================================================================
*/

---------------------------------------------------------------
-- POS TRANSACTIONS - staging.pos_transactions_clean
---------------------------------------------------------------

-- Check for NULL or duplicated transaction IDs
-- Expectation: No Results
SELECT
    transaction_id,
    COUNT(*) AS record_count
FROM staging.pos_transactions_clean
GROUP BY transaction_id
HAVING transaction_id IS NULL
    OR COUNT(*) > 1;

-- Check for untrimmed string fields
-- Expectation: No Results
SELECT transaction_id
FROM staging.pos_transactions_clean
WHERE transaction_id != LTRIM(RTRIM(transaction_id))
   OR cashier_id    != LTRIM(RTRIM(cashier_id));

-- Validate payment method standardization
SELECT DISTINCT payment_method
FROM staging.pos_transactions_clean;

-- Validate flagged bad dates
-- Expectation: is_bad_date = 1 only when timestamp is NULL
SELECT *
FROM staging.pos_transactions_clean
WHERE is_bad_date = 1
  AND transaction_timestamp IS NOT NULL;

-- Validate non-negative monetary fields
-- Expectation: No Results
SELECT *
FROM staging.pos_transactions_clean
WHERE total_amount < 0
   OR discount_amount < 0
   OR tax_amount < 0;

---------------------------------------------------------------
-- POS ITEMS - staging.pos_items_clean
---------------------------------------------------------------

-- Check for invalid quantities or prices not flagged
-- Expectation: No Results
SELECT *
FROM staging.pos_items_clean
WHERE (quantity < 0 OR quantity IS NULL)
  AND is_bad_qty = 0;

SELECT *
FROM staging.pos_items_clean
WHERE (unit_price < 0 OR unit_price IS NULL)
  AND is_bad_price = 0;

-- Check line total reconciliation
-- Expectation: No Results
SELECT *
FROM staging.pos_items_clean
WHERE ABS(
    COALESCE(line_total, 0)
  - COALESCE(calculated_line_total, 0)
) > 0.05
AND is_line_total_mismatch = 0;

---------------------------------------------------------------
-- E-COMMERCE ORDERS - staging.ecom_orders_clean
---------------------------------------------------------------

-- Check for NULL or duplicate order IDs
-- Expectation: No Results
SELECT
    order_id,
    COUNT(*) AS record_count
FROM staging.ecom_orders_clean
GROUP BY order_id
HAVING order_id IS NULL
    OR COUNT(*) > 1;

-- Validate standardized categorical fields
SELECT DISTINCT order_status FROM staging.ecom_orders_clean;
SELECT DISTINCT channel FROM staging.ecom_orders_clean;
SELECT DISTINCT device_type FROM staging.ecom_orders_clean;

-- Validate net revenue calculation
-- Expectation: No Results
SELECT *
FROM staging.ecom_orders_clean
WHERE ABS(
    net_revenue
  - (COALESCE(total_amount,0)
     - COALESCE(discount_amount,0)
     + COALESCE(shipping_cost,0))
) > 0.05;

---------------------------------------------------------------
-- E-COMMERCE ITEMS - staging.ecom_items_clean
---------------------------------------------------------------

-- Check for invalid quantities not flagged
-- Expectation: No Results
SELECT *
FROM staging.ecom_items_clean
WHERE (quantity < 0 OR quantity IS NULL)
  AND is_bad_qty = 0;

-- Check for pricing reconciliation mismatches
-- Expectation: No Results
SELECT *
FROM staging.ecom_items_clean
WHERE ABS(
    COALESCE(line_total,0)
  - COALESCE(calculated_line_total,0)
) > 0.05
AND is_line_total_mismatch = 0;

---------------------------------------------------------------
-- INVENTORY SNAPSHOTS - staging.inventory_snapshots_clean
---------------------------------------------------------------

-- Validate snapshot dates
-- Expectation: No Results
SELECT *
FROM staging.inventory_snapshots_clean
WHERE snapshot_date IS NULL
  AND is_bad_date = 0;

-- Validate store references
-- Expectation: No Results
SELECT *
FROM staging.inventory_snapshots_clean
WHERE store_id IS NULL
  AND is_bad_store = 0;

-- Validate safety stock flagging
-- Expectation: No Results
SELECT *
FROM staging.inventory_snapshots_clean
WHERE ending_inventory < safety_stock
  AND is_below_safety_stock = 0;

-- Validate inventory delta logic
SELECT *
FROM staging.inventory_snapshots_clean
WHERE calculated_inventory_delta
    != (ending_inventory - beginning_inventory);

---------------------------------------------------------------
-- RETURNS - staging.returns_clean
---------------------------------------------------------------

-- Check for invalid return dates not flagged
-- Expectation: No Results
SELECT *
FROM staging.returns_clean
WHERE return_date IS NULL
  AND is_bad_return_date = 0;

-- Check invalid quantities not flagged
-- Expectation: No Results
SELECT *
FROM staging.returns_clean
WHERE quantity_returned <= 0
  AND is_bad_qty = 0;

-- Validate refund amounts
-- Expectation: No Results
SELECT *
FROM staging.returns_clean
WHERE refund_amount < 0
  AND is_bad_refund = 0;

-- Validate standardized return reasons and channels
SELECT DISTINCT return_reason FROM staging.returns_clean;
SELECT DISTINCT return_channel FROM staging.returns_clean;

---------------------------------------------------------------
-- PRODUCTS - staging.products_clean
---------------------------------------------------------------

-- Validate primary key presence
-- Expectation: No Results
SELECT *
FROM staging.products_clean
WHERE product_id IS NULL;

-- Validate margin logic
-- Expectation: No Results
SELECT *
FROM staging.products_clean
WHERE price < cost
  AND is_negative_margin = 0;

-- Validate standardized categories
SELECT DISTINCT category FROM staging.products_clean;
SELECT DISTINCT brand FROM staging.products_clean;
SELECT DISTINCT status FROM staging.products_clean;

---------------------------------------------------------------
-- STORES - staging.stores_clean
---------------------------------------------------------------

-- Validate store IDs
-- Expectation: No Results
SELECT *
FROM staging.stores_clean
WHERE store_id IS NULL;

-- Validate standardized regions and store types
SELECT DISTINCT region FROM staging.stores_clean;
SELECT DISTINCT store_type FROM staging.stores_clean;

-- Validate duplicate flagging
-- Expectation: No Results
SELECT *
FROM staging.stores_clean
WHERE is_duplicate = 1
AND store_id IS NULL;

---------------------------------------------------------------
-- END OF SILVER LAYER QUALITY CHECKS
---------------------------------------------------------------
