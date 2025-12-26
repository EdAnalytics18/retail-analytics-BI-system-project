/* ============================================================================
   SAMPLE ANALYTICAL QUERIES
   ----------------------------------------------------------------------------
   Purpose:
   These example queries demonstrate how key business questions are answered
   using the Retail Analytics BI System’s dimensional model.

   Notes:
   - Queries are written directly against Core fact and dimension tables
     for educational clarity.
   - In production, equivalent logic is typically exposed via analytics-layer
     (Gold) views to ensure consistent KPI definitions across BI tools.
============================================================================ */


/* ============================================================================
   1. Monthly Net Revenue Trend (POS + E-Commerce)
   ----------------------------------------------------------------------------
   Business Goal:
   - Understand revenue performance over time
   - Identify seasonality, growth trends, and revenue dips
   - Used by Finance, FP&A, and Executive teams

   Logic:
   - Combine POS and E-Commerce net revenue
   - Aggregate revenue by calendar month using dim_date
============================================================================ */
SELECT
    d.year_num,
    d.month_num,
    d.month_name,
    SUM(f.net_revenue) AS monthly_net_revenue
FROM (
    SELECT date_sk, net_revenue
    FROM core.fact_pos_transactions

    UNION ALL

    SELECT date_sk, net_revenue
    FROM core.fact_ecom_orders
) f
JOIN core.dim_date d
    ON f.date_sk = d.date_sk
GROUP BY
    d.year_num,
    d.month_num,
    d.month_name
ORDER BY
    d.year_num,
    d.month_num;


/* ============================================================================
   2. Revenue by Channel (POS vs E-Commerce)
   ----------------------------------------------------------------------------
   Business Goal:
   - Compare revenue contribution by sales channel
   - Supports channel strategy and investment decisions

   Logic:
   - Use unified fact_sales_items
   - Uses line_revenue to support product-level comparisons
============================================================================ */
SELECT
    source_system,
    SUM(line_revenue) AS total_revenue
FROM core.fact_sales_items
GROUP BY
    source_system;


/* ============================================================================
   3. Top 10 Products by Revenue
   ----------------------------------------------------------------------------
   Business Goal:
   - Identify best-selling products
   - Supports assortment planning, pricing, promotions, and inventory allocation

   Logic:
   - Join fact_sales_items to dim_product
   - Rank products by total revenue
============================================================================ */
SELECT TOP 10
    p.product_name,
    SUM(f.line_revenue) AS total_revenue
FROM core.fact_sales_items f
JOIN core.dim_product p
    ON f.product_sk = p.product_sk
GROUP BY
    p.product_name
ORDER BY
    total_revenue DESC;


/* ============================================================================
   4. Products Below Safety Stock
   ----------------------------------------------------------------------------
   Business Goal:
   - Detect potential stock-out risks
   - Supports replenishment planning and inventory availability

   Logic:
   - Compare ending inventory to defined safety stock thresholds
   - Snapshot-based (point-in-time) inventory analysis
============================================================================ */
SELECT
    p.product_name,
    s.store_name,
    i.ending_inventory,
    i.safety_stock
FROM core.fact_inventory_snapshots i
JOIN core.dim_product p
    ON i.product_sk = p.product_sk
JOIN core.dim_store s
    ON i.store_sk = s.store_sk
WHERE
    i.ending_inventory < i.safety_stock;


/* ============================================================================
   5. Return Rate by Product and Channel (SAFE AGGREGATION)
   ----------------------------------------------------------------------------
   Business Goal:
   - Identify products with high return rates by channel
   - Highlights potential quality issues, sizing problems, or CX friction

   Logic:
   - Aggregate sold units and returned units separately
   - Prevents metric inflation caused by row-level joins
   - Segment results by POS vs E-Commerce
============================================================================ */
WITH sales AS (
    SELECT
        product_sk,
        source_system,
        SUM(quantity) AS sold_units
    FROM core.fact_sales_items
    GROUP BY
        product_sk,
        source_system
),
returns AS (
    SELECT
        product_sk,
        SUM(quantity_returned) AS returned_units
    FROM core.fact_returns
    GROUP BY
        product_sk
)
SELECT
    p.product_name,
    s.source_system,
    r.returned_units,
    s.sold_units,
    CAST(
        r.returned_units * 1.0
        / NULLIF(s.sold_units, 0)
        AS DECIMAL(5,2)
    ) AS return_rate
FROM sales s
JOIN returns r
    ON s.product_sk = r.product_sk
JOIN core.dim_product p
    ON p.product_sk = s.product_sk
ORDER BY
    return_rate DESC;
