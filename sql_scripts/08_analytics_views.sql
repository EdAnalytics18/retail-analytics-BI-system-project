/* =============================================================================
   ANALYTICS LAYER - BUSINESS-READY ANALYTICAL VIEWS
   -----------------------------------------------------------------------------
   Project: Retail Analytics BI System
   Layer: Analytics (Gold)
   Platform: SQL Server
   -----------------------------------------------------------------------------
   Purpose:
   - Provide simplified, trusted datasets for BI dashboards and ad-hoc analysis
   - Abstract complex joins and business logic away from analysts
   -----------------------------------------------------------------------------
   Design Principles:
   - Consistent, clearly defined grain per view
   - Business-friendly metric definitions
   - Read-optimized for Power BI / Tableau / Looker
   - Stable contracts for downstream reporting
============================================================================= */

/* =============================================================================
   1) EXECUTIVE REVENUE SUMMARY
   -----------------------------------------------------------------------------
   Grain: One row per source system (POS / ECOM)
   Use Cases:
   - Executive overview
   - Channel mix analysis
============================================================================= */
CREATE OR ALTER VIEW analytics.view_revenue_summary AS
SELECT
    f.source_system,                              
    COUNT(DISTINCT f.transaction_id) AS total_transactions,
    SUM(f.quantity)                  AS total_units_sold,
    ROUND(SUM(f.line_revenue), 2)    AS total_revenue,
    ROUND(AVG(f.line_revenue), 2)    AS avg_line_item_revenue
FROM 
    core.fact_sales_items f
GROUP BY 
    f.source_system;
GO


/* =============================================================================
   2) EXECUTIVE KPI OVERVIEW
   -----------------------------------------------------------------------------
   Grain: One row for the entire business (enterprise-level snapshot)
   -----------------------------------------------------------------------------
   KPIs Included:
   - Total Revenue
   - Average Order Value (AOV)
   - Units Sold
   - Return Rate (Units Returned / Units Sold)
   - Average Inventory Turnover
   -----------------------------------------------------------------------------
   Use Cases:
   - Executive dashboards (C-suite, VP, Director)
   - High-level business health monitoring
   - Performance benchmarking and trend summaries
   -----------------------------------------------------------------------------
   Design Notes:
   - Aggregates across all channels (POS + E-Commerce)
   - Business definitions align with retail finance standards
   - Returns exactly ONE ROW (ideal for KPI tiles)
   - Intended as a stable Gold-layer contract for BI tools
============================================================================= */
CREATE OR ALTER VIEW analytics.view_kpi_overview AS
WITH sales AS (
    SELECT
        COUNT(DISTINCT transaction_id) AS total_transactions,
        SUM(line_revenue)              AS total_revenue,
        SUM(quantity)                  AS units_sold
    FROM core.fact_sales_items
),
returns AS (
    SELECT
        SUM(quantity_returned) AS units_returned
    FROM core.fact_returns
),
inventory_turnover AS (
    SELECT
        AVG(
            CAST(s.total_units_sold AS FLOAT)
            / NULLIF(i.avg_inventory_units, 0)
        ) AS avg_inventory_turnover
    FROM (
        SELECT
            product_sk,
            SUM(quantity) AS total_units_sold
        FROM core.fact_sales_items
        GROUP BY product_sk
    ) s
    JOIN (
        SELECT
            product_sk,
            AVG(ending_inventory) AS avg_inventory_units
        FROM core.fact_inventory_snapshots
        GROUP BY product_sk
    ) i
        ON s.product_sk = i.product_sk
)

SELECT
    ROUND(s.total_revenue, 2) AS total_revenue,

    ROUND(
        s.total_revenue / NULLIF(s.total_transactions, 0),
        2
    ) AS avg_order_value,

    s.units_sold AS units_sold,

    ROUND(
        r.units_returned * 1.0 / NULLIF(s.units_sold, 0),
        4
    ) AS return_rate,

    ROUND(it.avg_inventory_turnover, 2) AS avg_inventory_turnover
FROM 
    sales s
CROSS JOIN 
    returns r
CROSS JOIN 
    inventory_turnover it;
GO


/* =============================================================================
   3) DAILY REVENUE TREND
   -----------------------------------------------------------------------------
   Grain: One row per calendar day
   Use Cases:
   - Time-series trend analysis
   - YoY / MoM comparisons
============================================================================= */
CREATE OR ALTER VIEW analytics.view_daily_revenue AS
SELECT
    d.full_date,
    d.year_num,
    d.month_num,
    d.month_name,
    COUNT(DISTINCT f.transaction_id) AS total_transactions,
    SUM(f.quantity)                  AS units_sold,
    ROUND(SUM(f.line_revenue), 2)    AS daily_revenue
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


/* =============================================================================
   4) PRODUCT PERFORMANCE
   -----------------------------------------------------------------------------
   Grain: One row per product
   Use Cases:
   - Top / bottom product analysis
   - Margin and pricing evaluation
============================================================================= */
CREATE OR ALTER VIEW analytics.view_product_performance AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,
    SUM(f.quantity)                       AS units_sold,
    ROUND(SUM(f.line_revenue), 2)         AS total_revenue,
    ROUND(AVG(f.unit_price), 2)           AS avg_selling_price,
    ROUND(SUM(p.margin * f.quantity), 2)  AS total_margin
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


/* =============================================================================
   5) CATEGORY PERFORMANCE
   -----------------------------------------------------------------------------
   Grain: One row per category / subcategory
   Use Cases:
   - Category contribution analysis
   - Margin mix optimization
============================================================================= */
CREATE OR ALTER VIEW analytics.view_category_performance AS
SELECT
    p.category,
    p.subcategory,
    SUM(f.quantity)                AS units_sold,
    ROUND(SUM(f.line_revenue), 2)  AS category_revenue,
    ROUND(
        SUM(p.margin * f.quantity)
        / NULLIF(SUM(f.line_revenue), 0),
        4
    ) AS category_gross_margin_pct
FROM 
    core.fact_sales_items f
JOIN 
    core.dim_product p
    ON f.product_sk = p.product_sk
GROUP BY
    p.category,
    p.subcategory;
GO


/* =============================================================================
   6) STORE PERFORMANCE (IN-STORE ONLY)
   -----------------------------------------------------------------------------
   Grain: One row per store
   Use Cases:
   - Store ranking
   - Regional performance analysis
============================================================================= */
CREATE OR ALTER VIEW analytics.view_store_performance AS
SELECT
    s.store_id,
    s.store_name,
    s.region,
    COUNT(DISTINCT p.transaction_id) AS total_transactions,
    ROUND(SUM(p.net_revenue), 2)     AS total_revenue,
    ROUND(
        SUM(p.net_revenue)
        / NULLIF(COUNT(DISTINCT p.transaction_id), 0),
        2
    ) AS avg_transaction_value
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


/* =============================================================================
   7) INVENTORY SNAPSHOT
   -----------------------------------------------------------------------------
   Grain: One row per product per store per date
   Use Cases:
   - Stock monitoring
   - Safety stock analysis
============================================================================= */
CREATE OR ALTER VIEW analytics.view_inventory_snapshot AS
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


/* =============================================================================
   8) INVENTORY TURNOVER (SIMPLIFIED)
   -----------------------------------------------------------------------------
   Grain: One row per product
   Use Cases:
   - Supply chain efficiency
   - Slow-moving inventory detection
============================================================================= */
CREATE OR ALTER VIEW analytics.view_inventory_turnover AS
WITH sales_per_product AS (
    SELECT
        product_sk,
        SUM(quantity) AS total_units_sold
    FROM core.fact_sales_items
    GROUP BY product_sk
),
inventory_per_product AS (
    SELECT
        product_sk,
        AVG(ending_inventory) AS avg_inventory_units
    FROM core.fact_inventory_snapshots
    GROUP BY product_sk
)
SELECT
    p.product_id,
    p.product_name,
    s.total_units_sold,
    i.avg_inventory_units,
    ROUND(
        CAST(s.total_units_sold AS FLOAT)
        / NULLIF(i.avg_inventory_units, 0),
        2
    ) AS inventory_turnover_ratio
FROM 
    sales_per_product s
JOIN 
    inventory_per_product i
    ON s.product_sk = i.product_sk
JOIN 
    core.dim_product p
    ON p.product_sk = s.product_sk;
GO


/* =============================================================================
   9) RETURNS SUMMARY
   -----------------------------------------------------------------------------
   Grain: One row per product
   Use Cases:
   - Return rate analysis
   - Quality and customer satisfaction signals
============================================================================= */
CREATE OR ALTER VIEW analytics.view_returns_summary AS
SELECT
    p.product_id,
    p.product_name,
    COUNT(r.return_id)               AS total_returns,
    SUM(r.quantity_returned)         AS units_returned,
    ROUND(SUM(r.refund_amount), 2)   AS total_refund_amount
FROM 
    core.fact_returns r
JOIN 
    core.dim_product p
    ON r.product_sk = p.product_sk
GROUP BY
    p.product_id,
    p.product_name;
GO
