/* ============================================================================
   SAMPLE ANALYTICAL QUERIES
   ----------------------------------------------------------------------------
   These examples demonstrate how key business questions are answered using 
   the Gold-layer dimensional model. Each query shows how facts and dimensions 
   come together to support real-world retail analytics use cases.
============================================================================ */


/* ============================================================================
   1. Monthly Net Revenue Trend (POS + E-Commerce)
   ----------------------------------------------------------------------------
   Business Goal:
   - Understand revenue performance over time (seasonality, growth, dips)
   - Used by Finance, FP&A, and Executive teams for forecasting

   Logic:
   - Combine POS and E-Commerce net revenue
   - Aggregate net revenue per month using the Date dimension
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
JOIN 
    core.dim_date d
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
   - Compare performance across sales channels
   - Helps marketing, retail ops, and e-commerce teams assess where revenue 
     is coming from and how customer behavior differs

   Logic:
   - Use unified sales_items fact
   - Aggregate revenue by source system (POS vs ECOM)
============================================================================ */
SELECT
    source_system,     
    SUM(line_revenue) AS total_revenue
FROM 
   core.fact_sales_items
GROUP BY
    source_system;


/* ============================================================================
   3. Top 10 Products by Revenue
   ----------------------------------------------------------------------------
   Business Goal:
   - Identify best-selling products to support assortment planning, pricing,
     promotions, and inventory allocation

   Logic:
   - Join fact_sales_items to dim_product
   - Rank products by total revenue
============================================================================ */
SELECT TOP 10
    p.product_name,
    SUM(f.line_revenue) AS total_revenue
FROM 
   core.fact_sales_items f
JOIN 
   core.dim_product p
    ON f.product_sk = p.product_sk
GROUP BY
    p.product_name
ORDER BY
    total_revenue DESC;


/* ============================================================================
   4. Products Below Safety Stock
   ----------------------------------------------------------------------------
   Business Goal:
   - Detect potential stockouts before they occur
   - Supports replenishment planning and improves customer availability

   Logic:
   - Compare ending inventory to defined safety stock thresholds
============================================================================ */
SELECT
    p.product_name,
    s.store_name,
    i.ending_inventory,
    i.safety_stock
FROM 
   core.fact_inventory_snapshots i
JOIN 
   core.dim_product p
    ON i.product_sk = p.product_sk
JOIN 
   core.dim_store s
    ON i.store_sk = s.store_sk
WHERE
    i.ending_inventory < i.safety_stock;


/* ============================================================================
   5. Return Rate by Product and Channel
   ----------------------------------------------------------------------------
   Business Goal:
   - Identify products with high return rates by sales channel
   - Highlights potential quality issues, sizing problems, or customer friction

   Logic:
   - Compare returned units to sold units
   - Segment return behavior by POS vs E-Commerce
============================================================================ */
SELECT
    p.product_name,
    f.source_system,
    SUM(r.quantity_returned) AS returned_units,
    SUM(f.quantity) AS sold_units,
    CAST(
        SUM(r.quantity_returned) * 1.0
        / NULLIF(SUM(f.quantity), 0)
        AS DECIMAL(5,2)
    ) AS return_rate
FROM 
   core.fact_returns r
JOIN 
   core.dim_product p
    ON r.product_sk = p.product_sk
JOIN 
   core.fact_sales_items f
    ON r.product_sk = f.product_sk
GROUP BY
    p.product_name,
    f.source_system
ORDER BY
    return_rate DESC;
