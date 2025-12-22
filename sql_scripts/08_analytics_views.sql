/* =============================================================================
   ANALYTICS LAYER - BUSINESS-READY ANALYTICAL VIEWS (GOLD)
   =============================================================================
   Layer: Analytics / Gold
   -----------------------------------------------------------------------------
   Executive Summary:
   This layer exposes trusted, business-ready analytical views designed for
   dashboards, reporting, and ad-hoc analysis.

   All complex joins, calculations, and business rules are encapsulated here
   so analysts and BI tools can focus on insights, not SQL logic.

   -----------------------------------------------------------------------------
   Business Value:
   - Provides a stable, trusted data contract for BI tools
   - Reduces duplication of logic across dashboards
   - Ensures consistent KPI definitions across teams
   - Accelerates time-to-insight for executives and analysts

   -----------------------------------------------------------------------------
   Design Principles:
   - Clearly defined grain per view
   - Business-friendly metric naming
   - Read-optimized for Power BI / Tableau / Looker
   - Safe to use directly for executive dashboards
============================================================================= */


/* =============================================================================
   1) EXECUTIVE REVENUE SUMMARY
   =============================================================================
   Grain:
   One row per source system (POS vs E-Commerce)

   Use Cases:
   - Executive revenue overview
   - Channel mix analysis
   - Revenue contribution benchmarking
============================================================================= */
CREATE OR ALTER VIEW analytics.view_revenue_summary AS
SELECT
    f.source_system,

    COUNT(DISTINCT CONCAT(f.source_system, '-', f.transaction_id))
        AS total_transactions,

    SUM(f.quantity) AS total_units_sold,

    ROUND(SUM(f.line_revenue), 2) AS total_revenue,

    ROUND(
        SUM(p.margin * f.quantity), 2
    ) AS total_margin,

    ROUND(
        SUM(f.line_revenue)
        / NULLIF(SUM(SUM(f.line_revenue)) OVER (), 0),
        4
    ) AS revenue_share_pct
FROM core.fact_sales_items f
JOIN core.dim_product p
  ON f.product_sk = p.product_sk
GROUP BY f.source_system;
GO


/* =============================================================================
   2) EXECUTIVE KPI OVERVIEW (ENTERPRISE SNAPSHOT)
   =============================================================================
   Grain:
   One row for the entire business

   KPIs Included:
   - Total Revenue
   - Average Order Value (AOV)
   - Units Sold
   - Return Rate
   - Inventory Turnover

   Design Notes:
   - Aggregates across all sales channels
   - Always returns exactly ONE row
   - Ideal for KPI tiles and executive scorecards
============================================================================= */
CREATE OR ALTER VIEW analytics.view_kpi_overview AS
WITH sales AS (
    SELECT
        COUNT(DISTINCT CONCAT(source_system, '-', transaction_id))
            AS total_transactions,
        SUM(line_revenue) AS total_revenue,
        SUM(quantity)     AS units_sold
    FROM core.fact_sales_items
),
returns AS (
    SELECT
        SUM(quantity_returned) AS units_returned
    FROM core.fact_returns
),
inventory AS (
    SELECT
        AVG(ending_inventory) AS avg_inventory_units
    FROM core.fact_inventory_snapshots
)
SELECT
    ROUND(s.total_revenue, 2) AS total_revenue,

    ROUND(
        s.total_revenue / NULLIF(s.total_transactions, 0),
        2
    ) AS avg_order_value,

    s.units_sold,

    ROUND(
        r.units_returned * 1.0 / NULLIF(s.units_sold, 0),
        4
    ) AS return_rate,

    ROUND(
        s.units_sold / NULLIF(i.avg_inventory_units, 0),
        2
    ) AS inventory_turnover
FROM sales s
CROSS JOIN returns r
CROSS JOIN inventory i;
GO


/* =============================================================================
   3) DAILY REVENUE TREND
   =============================================================================
   Grain:
   One row per calendar day per source system

   Use Cases:
   - Time-series revenue analysis
   - MoM / YoY trend reporting
   - Channel performance over time
============================================================================= */
CREATE OR ALTER VIEW analytics.view_daily_revenue AS
SELECT
    d.full_date,
    d.year_num,
    d.month_num,
    d.month_name,
    f.source_system,

    COUNT(DISTINCT CONCAT(f.source_system, '-', f.transaction_id))
        AS total_transactions,

    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.line_revenue), 2) AS daily_revenue
FROM core.fact_sales_items f
JOIN core.dim_date d
  ON f.date_sk = d.date_sk
GROUP BY
    d.full_date,
    d.year_num,
    d.month_num,
    d.month_name,
    f.source_system;
GO


/* =============================================================================
   4) PRODUCT PERFORMANCE
   =============================================================================
   Grain:
   One row per product

   Use Cases:
   - Top / bottom product analysis
   - Pricing and margin evaluation
   - Product portfolio optimization
============================================================================= */
CREATE OR ALTER VIEW analytics.view_product_performance AS
SELECT
    p.product_id,
    p.product_name,
    p.category,
    p.subcategory,
    p.brand,

    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.line_revenue), 2) AS total_revenue,

    ROUND(AVG(f.unit_price), 2) AS avg_selling_price,

    ROUND(SUM(p.margin * f.quantity), 2) AS total_margin,

    ROUND(
        SUM(p.margin * f.quantity)
        / NULLIF(SUM(f.line_revenue), 0),
        4
    ) AS gross_margin_pct
FROM core.fact_sales_items f
JOIN core.dim_product p
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
   =============================================================================
   Grain:
   One row per category / subcategory

   Use Cases:
   - Category contribution analysis
   - Revenue and margin mix optimization
============================================================================= */
CREATE OR ALTER VIEW analytics.view_category_performance AS
SELECT
    p.category,
    p.subcategory,

    SUM(f.quantity) AS units_sold,
    ROUND(SUM(f.line_revenue), 2) AS category_revenue,

    ROUND(
        SUM(p.margin * f.quantity)
        / NULLIF(SUM(f.line_revenue), 0),
        4
    ) AS category_gross_margin_pct,

    ROUND(
        SUM(f.line_revenue)
        / NULLIF(SUM(SUM(f.line_revenue)) OVER (), 0),
        4
    ) AS category_revenue_share
FROM core.fact_sales_items f
JOIN core.dim_product p
  ON f.product_sk = p.product_sk
GROUP BY
    p.category,
    p.subcategory;
GO


/* =============================================================================
   6) STORE PERFORMANCE (IN-STORE ONLY)
   =============================================================================
   Grain:
   One row per store

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

    ROUND(SUM(p.net_revenue), 2) AS total_revenue,

    ROUND(
        SUM(p.net_revenue)
        / NULLIF(COUNT(DISTINCT p.transaction_id), 0),
        2
    ) AS avg_transaction_value
FROM core.fact_pos_transactions p
JOIN core.dim_store s
  ON p.store_sk = s.store_sk
GROUP BY
    s.store_id,
    s.store_name,
    s.region;
GO


/* =============================================================================
   7) INVENTORY SNAPSHOT
   =============================================================================
   Grain:
   One row per product per store per date

   Use Cases:
   - Stock availability monitoring
   - Safety stock breach detection
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
    i.inventory_value,

    CASE WHEN i.ending_inventory = 0 THEN 1 ELSE 0 END
        AS is_stock_out,

    CASE WHEN i.ending_inventory < i.safety_stock THEN 1 ELSE 0 END
        AS is_below_safety_stock
FROM core.fact_inventory_snapshots i
JOIN core.dim_product p ON i.product_sk = p.product_sk
JOIN core.dim_store   s ON i.store_sk   = s.store_sk
JOIN core.dim_date    d ON i.date_sk    = d.date_sk;
GO


/* =============================================================================
   8) INVENTORY TURNOVER (SIMPLIFIED)
   =============================================================================
   Grain:
   One row per product

   Use Cases:
   - Supply chain efficiency analysis
   - Slow-moving inventory identification
============================================================================= */
CREATE OR ALTER VIEW analytics.view_inventory_turnover AS
WITH sales AS (
    SELECT
        product_sk,
        SUM(quantity) AS total_units_sold
    FROM core.fact_sales_items
    GROUP BY product_sk
),
inventory AS (
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
        s.total_units_sold * 1.0
        / NULLIF(i.avg_inventory_units, 0),
        2
    ) AS inventory_turnover_ratio
FROM sales s
JOIN inventory i
  ON s.product_sk = i.product_sk
JOIN core.dim_product p
  ON p.product_sk = s.product_sk;
GO


/* =============================================================================
   9) RETURNS SUMMARY
   =============================================================================
   Grain:
   One row per product

   Use Cases:
   - Return rate analysis
   - Product quality and CX insights
============================================================================= */
CREATE OR ALTER VIEW analytics.view_returns_summary AS
SELECT
    p.product_id,
    p.product_name,

    COUNT(r.return_id) AS total_returns,
    SUM(r.quantity_returned) AS units_returned,
    ROUND(SUM(r.refund_amount), 2) AS total_refund_amount,

    ROUND(
        SUM(r.quantity_returned)
        / NULLIF(SUM(f.quantity), 0),
        4
    ) AS return_rate
FROM core.fact_returns r
JOIN core.dim_product p
  ON r.product_sk = p.product_sk
LEFT JOIN core.fact_sales_items f
  ON f.product_sk = p.product_sk
GROUP BY
    p.product_id,
    p.product_name;
GO
