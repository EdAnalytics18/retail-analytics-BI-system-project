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
   2. Revenue by Channel Over Time (POS vs E-Commerce)
----------------------------------------------------------------------------
Business Goal:
- Compare revenue contribution by sales channel over time
- Identify channel mix shifts and growth trends
- Support channel strategy and investment decisions

Logic:
- Uses unified core.fact_sales_items
- Aggregates line_revenue at the desired time grain
- Joins to dim_date for temporal analysis
============================================================================ */
WITH channel_revenue AS (
    SELECT
        d.year_num,
        d.month_num,
        f.source_system,
        SUM(f.line_revenue) AS channel_revenue
    FROM core.fact_sales_items f
    JOIN core.dim_date d
        ON f.date_sk = d.date_sk
    GROUP BY
        d.year_num,
        d.month_num,
        f.source_system
),
total_revenue AS (
    SELECT
        year_num,
        month_num,
        SUM(channel_revenue) AS total_revenue
    FROM channel_revenue
    GROUP BY
        year_num,
        month_num
)
SELECT
    c.year_num,
    c.month_num,
    c.source_system,
    c.channel_revenue,
    ROUND(
        c.channel_revenue / t.total_revenue * 100, 2
    ) AS channel_revenue_pct
FROM channel_revenue c
JOIN total_revenue t
    ON c.year_num = t.year_num
   AND c.month_num = t.month_num
ORDER BY
    c.year_num,
    c.month_num,
    c.source_system;


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
   4. Slow-Moving Inventory (Low Turnover Risk)
   ----------------------------------------------------------------------------
   Business Goal:
   - Identify products with excess inventory relative to sales velocity
   - Support markdown planning, assortment optimization, and cash efficiency

   Logic:
   - Compare average ending inventory to total units sold over a time window
   - Uses inventory snapshots and sales line items
   - Aggregated at product and store level
============================================================================ */

WITH inventory_avg AS (
    SELECT
        i.product_sk,
        i.store_sk,
        AVG(i.ending_inventory) AS avg_inventory
    FROM core.fact_inventory_snapshots i
    GROUP BY
        i.product_sk,
        i.store_sk
),
sales_volume AS (
    SELECT
        f.product_sk,
        f.store_sk,
        SUM(f.quantity) AS total_units_sold
    FROM core.fact_sales_items f
    GROUP BY
        f.product_sk,
        f.store_sk
)
SELECT
    p.product_name,
    s.store_name,
    ia.avg_inventory,
    sv.total_units_sold,
    CASE
        WHEN sv.total_units_sold = 0 THEN 'No Sales'
        WHEN ia.avg_inventory / sv.total_units_sold > 2 THEN 'Slow-Moving'
        ELSE 'Healthy'
    END AS inventory_status
FROM inventory_avg ia
LEFT JOIN sales_volume sv
    ON ia.product_sk = sv.product_sk
   AND ia.store_sk = sv.store_sk
JOIN core.dim_product p
    ON ia.product_sk = p.product_sk
JOIN core.dim_store s
    ON ia.store_sk = s.store_sk
ORDER BY
    inventory_status DESC,
    ia.avg_inventory DESC;


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
WHERE
    r.returned_units > 0
ORDER BY
    return_rate DESC;

