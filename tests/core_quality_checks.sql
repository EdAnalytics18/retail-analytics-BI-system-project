/*
===============================================================================
CORE (GOLD) LAYER - DATA QUALITY & INTEGRITY CHECKS
===============================================================================
Script Purpose:
    This script validates the correctness, integrity, and analytical safety
    of the Core (Gold) layer fact and dimension tables.

    These checks ensure:
    - Fact table grains are respected (no unintended duplicates)
    - Foreign key relationships to conformed dimensions are valid
    - No NULLs exist in mandatory surrogate keys
    - Revenue, quantity, and inventory metrics are sane
    - Silver-layer filtering rules were correctly enforced

Usage Notes:
    - Run AFTER core dimensions and fact tables are built
    - Any returned rows indicate data quality issues
    - Expected Result for most checks: NO ROWS
===============================================================================
*/

---------------------------------------------------------------
-- DIMENSION CHECKS
---------------------------------------------------------------

---------------------------------------------------------------
-- DIM_DATE
---------------------------------------------------------------

-- Validate unique date grain
-- Expectation: No Results
SELECT full_date, COUNT(*)
FROM core.dim_date
GROUP BY full_date
HAVING COUNT(*) > 1;

-- Validate surrogate key format (YYYYMMDD)
-- Expectation: No Results
SELECT *
FROM core.dim_date
WHERE date_sk < 19000101
   OR date_sk > 21000101;

---------------------------------------------------------------
-- DIM_PRODUCT
---------------------------------------------------------------

-- Validate primary key presence
-- Expectation: No Results
SELECT *
FROM core.dim_product
WHERE product_sk IS NULL
   OR product_id IS NULL;

-- Validate duplicate natural keys
-- Expectation: No Results
SELECT product_id, COUNT(*)
FROM core.dim_product
GROUP BY product_id
HAVING COUNT(*) > 1;

-- Validate margin logic
-- Expectation: No Results
SELECT *
FROM core.dim_product
WHERE margin != (price - cost)
  AND price IS NOT NULL
  AND cost IS NOT NULL;

---------------------------------------------------------------
-- DIM_STORE
---------------------------------------------------------------

-- Validate primary key presence
-- Expectation: No Results
SELECT *
FROM core.dim_store
WHERE store_sk IS NULL
   OR store_id IS NULL;

-- Validate duplicate natural keys
-- Expectation: No Results
SELECT store_id, COUNT(*)
FROM core.dim_store
GROUP BY store_id
HAVING COUNT(*) > 1;

---------------------------------------------------------------
-- FACT TABLE CHECKS
---------------------------------------------------------------

---------------------------------------------------------------
-- FACT_POS_TRANSACTIONS
---------------------------------------------------------------

-- Validate grain (one row per transaction_id)
-- Expectation: No Results
SELECT transaction_id, COUNT(*)
FROM core.fact_pos_transactions
GROUP BY transaction_id
HAVING COUNT(*) > 1;

-- Validate foreign key integrity
-- Expectation: No Results
SELECT *
FROM core.fact_pos_transactions f
LEFT JOIN core.dim_store s ON f.store_sk = s.store_sk
LEFT JOIN core.dim_date d  ON f.date_sk  = d.date_sk
WHERE s.store_sk IS NULL
   OR d.date_sk IS NULL;

-- Validate net revenue calculation
-- Expectation: No Results
SELECT *
FROM core.fact_pos_transactions
WHERE ABS(
    net_revenue
  - (COALESCE(total_amount,0)
     - COALESCE(discount_amount,0)
     + COALESCE(tax_amount,0))
) > 0.05;

---------------------------------------------------------------
-- FACT_ECOM_ORDERS
---------------------------------------------------------------

-- Validate grain (one row per order_id)
-- Expectation: No Results
SELECT order_id, COUNT(*)
FROM core.fact_ecom_orders
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Validate date foreign key
-- Expectation: No Results
SELECT *
FROM core.fact_ecom_orders f
LEFT JOIN core.dim_date d ON f.date_sk = d.date_sk
WHERE d.date_sk IS NULL;

-- Validate net revenue calculation
-- Expectation: No Results
SELECT *
FROM core.fact_ecom_orders
WHERE ABS(
    net_revenue
  - (COALESCE(total_amount,0)
     - COALESCE(discount_amount,0)
     - COALESCE(shipping_cost,0))
) > 0.05;

---------------------------------------------------------------
-- FACT_SALES_ITEMS
---------------------------------------------------------------

-- Validate grain (no duplicate line items)
-- Expectation: No Results
SELECT
    source_system,
    transaction_id,
    product_sk,
    date_sk,
    COUNT(*)
FROM core.fact_sales_items
GROUP BY source_system, transaction_id, product_sk, date_sk
HAVING COUNT(*) > 1;

-- Validate foreign keys
-- Expectation: No Results
SELECT *
FROM core.fact_sales_items f
LEFT JOIN core.dim_product p ON f.product_sk = p.product_sk
LEFT JOIN core.dim_date d    ON f.date_sk    = d.date_sk
LEFT JOIN core.dim_store s  ON f.store_sk   = s.store_sk
WHERE p.product_sk IS NULL
   OR d.date_sk IS NULL
   OR (f.source_system = 'POS' AND s.store_sk IS NULL);

-- Validate non-negative quantities and revenue
-- Expectation: No Results
SELECT *
FROM core.fact_sales_items
WHERE quantity < 0
   OR line_revenue < 0;

---------------------------------------------------------------
-- FACT_RETURNS
---------------------------------------------------------------

-- Validate grain (one row per return_id)
-- Expectation: No Results
SELECT return_id, COUNT(*)
FROM core.fact_returns
GROUP BY return_id
HAVING COUNT(*) > 1;

-- Validate foreign keys
-- Expectation: No Results
SELECT *
FROM core.fact_returns f
LEFT JOIN core.dim_product p ON f.product_sk = p.product_sk
LEFT JOIN core.dim_date d    ON f.date_sk    = d.date_sk
LEFT JOIN core.dim_store s  ON f.store_sk   = s.store_sk
WHERE p.product_sk IS NULL
   OR d.date_sk IS NULL;

-- Validate non-negative refunds and quantities
-- Expectation: No Results
SELECT *
FROM core.fact_returns
WHERE refund_amount < 0
   OR quantity_returned <= 0;

---------------------------------------------------------------
-- FACT_INVENTORY_SNAPSHOTS
---------------------------------------------------------------

-- Validate grain (snapshot_date + store + product)
-- Expectation: No Results
SELECT
    product_sk,
    store_sk,
    date_sk,
    COUNT(*)
FROM core.fact_inventory_snapshots
GROUP BY product_sk, store_sk, date_sk
HAVING COUNT(*) > 1;

-- Validate foreign keys
-- Expectation: No Results
SELECT *
FROM core.fact_inventory_snapshots f
LEFT JOIN core.dim_product p ON f.product_sk = p.product_sk
LEFT JOIN core.dim_store s   ON f.store_sk   = s.store_sk
LEFT JOIN core.dim_date d    ON f.date_sk    = d.date_sk
WHERE p.product_sk IS NULL
   OR s.store_sk IS NULL
   OR d.date_sk IS NULL;

-- Validate inventory sanity
-- Expectation: No Results
SELECT *
FROM core.fact_inventory_snapshots
WHERE beginning_inventory < 0
   OR ending_inventory < 0
   OR inventory_value < 0;

---------------------------------------------------------------
-- END OF CORE LAYER QUALITY CHECKS
---------------------------------------------------------------
