/* =============================================================================
   ANALYTICS LAYER ? BUSINESS-READY ANALYTICAL VIEWS
   -----------------------------------------------------------------------------
   Purpose:
   - Provide simplified, trusted datasets for BI dashboards and ad-hoc analysis
   - Abstract complex joins and business logic away from analysts
   -----------------------------------------------------------------------------
   Design Principles:
   - Consistent grains per view
   - Business-friendly metric definitions
   - Optimized for read performance
============================================================================= */

---------------------------------------------------------------------------
-- 1?. EXECUTIVE REVENUE SUMMARY
-- Grain: One row per source system (POS / ECOM)
---------------------------------------------------------------------------
CREATE OR ALTER VIEW analytics.v_revenue_summary AS
SELECT
    f.source_system,
    COUNT(DISTINCT f.transaction_id)          AS total_transactions,
    SUM(f.quantity)                           AS total_units_sold,
    ROUND(SUM(f.line_revenue), 2)             AS total_revenue,
    ROUND(AVG(f.line_revenue), 2)             AS avg_line_revenue
FROM 
    core.fact_sales_items f
GROUP BY 
    f.source_system;
GO


---------------------------------------------------------------------------
-- 2?. DAILY REVENUE TREND
-- Grain: One row per day
---------------------------------------------------------------------------
CREATE OR ALTER VIEW analytics.v_daily_revenue AS
SELECT
    d.full_date,
    d.year_num,
    d.month_num,
    d.month_name,
    COUNT(DISTINCT f.transaction_id)           AS total_transactions,
    SUM(f.quantity)                            AS units_sold,
    ROUND(SUM(f.line_revenue), 2)              AS daily_revenue
FROM 
    core.fact_sales_items f
JOIN 
    core.dim_date d
    ON f.date_sk = d.date_sk
GROUP BY
    d.full_date,
    d.year_num,
    d.month_num,
    d.month_name;
GO


---------------------------------------------------------------------------
-- 3?. PRODUCT PERFORMANCE
-- Grain: One row per product
---------------------------------------------------------------------------
CREATE OR ALTER VIEW analytics.v_product_performance AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    SUM(f.quantity)                            AS units_sold,
    ROUND(SUM(f.line_revenue), 2)              AS total_revenue,
    ROUND(AVG(f.unit_price), 2)                AS avg_selling_price,
    ROUND(SUM(p.margin * f.quantity), 2)       AS total_margin
FROM 
    core.fact_sales_items f
JOIN 
    core.dim_product p
    ON f.product_sk = p.product_sk
GROUP BY
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand;
GO


---------------------------------------------------------------------------
-- 4?. CATEGORY PERFORMANCE
-- Grain: One row per product category
---------------------------------------------------------------------------
CREATE OR ALTER VIEW analytics.v_category_performance AS
SELECT
    p.category,
    p.subcategory,
    SUM(f.quantity)                            AS units_sold,
    ROUND(SUM(f.line_revenue), 2)              AS category_revenue,
    ROUND(AVG(p.margin), 2)                    AS avg_margin_per_product
FROM 
    core.fact_sales_items f
JOIN 
    core.dim_product p
    ON f.product_sk = p.product_sk
GROUP BY
    p.category,
    p.subcategory;
GO


---------------------------------------------------------------------------
-- 5?. STORE PERFORMANCE (IN-STORE ONLY)
-- Grain: One row per store
---------------------------------------------------------------------------
CREATE OR ALTER VIEW analytics.v_store_performance AS
SELECT
    s.store_id,
    s.store_name,
    s.region,
    COUNT(DISTINCT p.transaction_id)           AS total_transactions,
    ROUND(SUM(p.net_revenue), 2)               AS total_revenue,
    ROUND(AVG(p.net_revenue), 2)               AS avg_transaction_value
FROM 
    core.fact_pos_transactions p
JOIN 
    core.dim_store s
    ON p.store_sk = s.store_sk
GROUP BY
    s.store_id,
    s.store_name,
    s.region;
GO


---------------------------------------------------------------------------
-- 6?. INVENTORY SNAPSHOT
-- Grain: One row per product-store-date
---------------------------------------------------------------------------
CREATE OR ALTER VIEW analytics.v_inventory_snapshot AS
SELECT
    d.full_date,
    s.store_id,
    p.product_id,
    i.beginning_inventory,
    i.ending_inventory,
    i.safety_stock,
    i.stock_status,
    i.inventory_value
FROM 
    core.fact_inventory_snapshots i
JOIN 
    core.dim_product p
    ON i.product_sk = p.product_sk
JOIN 
    core.dim_store s
    ON i.store_sk = s.store_sk
JOIN 
    core.dim_date d
    ON i.date_sk = d.date_sk;
GO


---------------------------------------------------------------------------
-- 7?. INVENTORY TURNOVER (SIMPLIFIED)
-- Grain: One row per product
---------------------------------------------------------------------------
CREATE OR ALTER VIEW analytics.v_inventory_turnover AS
SELECT
    p.product_id,
    p.product_name,
    SUM(f.quantity)                               AS total_units_sold,
    AVG(i.inventory_value)                        AS avg_inventory_value,
    ROUND(
        CAST(SUM(f.quantity) AS FLOAT)
        / NULLIF(AVG(i.inventory_value), 0),
        2
    ) AS inventory_turnover_ratio
FROM 
    core.fact_sales_items f
JOIN 
    core.dim_product p
    ON f.product_sk = p.product_sk
JOIN 
    core.fact_inventory_snapshots i
    ON f.product_sk = i.product_sk
GROUP BY
    p.product_id,
    p.product_name;
GO


---------------------------------------------------------------------------
-- 8?. RETURNS SUMMARY
-- Grain: One row per product
---------------------------------------------------------------------------
CREATE OR ALTER VIEW analytics.v_returns_summary AS
SELECT
    p.product_id,
    p.product_name,
    COUNT(r.return_id)                       AS total_returns,
    SUM(r.quantity_returned)                 AS units_returned,
    ROUND(SUM(r.refund_amount), 2)           AS total_refund_amount
FROM 
    core.fact_returns r
JOIN 
    core.dim_product p
    ON r.product_sk = p.product_sk
GROUP BY
    p.product_id,
    p.product_name;
GO
