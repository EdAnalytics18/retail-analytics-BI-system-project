#  Metrics Definitions

This document defines the **core business KPIs** supported by the Retail Analytics BI System.  
All metrics are calculated from **conformed fact and dimension tables** in the Core layer and
exposed through **analytics-layer (Gold) views**, ensuring consistency, accuracy, and reuse
across dashboards and ad-hoc analyses.

---

##  Revenue Metrics

### Total Revenue
Total sales revenue generated at the product line-item level, before refunds.

**Formula**  
`SUM(line_revenue)`

**Source**  
`core.fact_sales_items`

---

### Net Revenue
Revenue realized after discounts, shipping, and adjustments.  
Calculated separately for in-store (POS) and e-commerce channels.

**Formula**  
`POS Net Revenue  = SUM(net_revenue)`  
`ECOM Net Revenue = SUM(net_revenue)`

**Source**  
- `core.fact_pos_transactions`  
- `core.fact_ecom_orders`

---

### Average Order Value (AOV)
Average net revenue generated per completed transaction, calculated **per channel**.

**Formula**  
`POS AOV  = SUM(net_revenue) / COUNT(DISTINCT transaction_id)`  
`ECOM AOV = SUM(net_revenue) / COUNT(DISTINCT order_id)`

**Source**  
- `core.fact_pos_transactions`  
- `core.fact_ecom_orders`

---

##  Sales & Volume Metrics

### Units Sold
Total number of product units sold across all channels.

**Formula**  
`SUM(quantity)`

**Source**  
`core.fact_sales_items`

---

### Order Count
Total number of completed sales transactions, calculated by channel.

**Formula**  
`POS Orders  = COUNT(DISTINCT transaction_id)`  
`ECOM Orders = COUNT(DISTINCT order_id)`

**Source**  
- `core.fact_pos_transactions`  
- `core.fact_ecom_orders`

> **Note:** Unified order counts across channels are derived at the analytics layer when required.

---

##  Profitability Metrics

### Unit Gross Margin
Per-unit profit after cost of goods sold.

**Formula**  
`price − cost`

**Source**  
`core.dim_product`

---

### Total Contribution Margin
Total margin generated from product sales across all channels.

**Formula**  
`SUM(quantity × margin)`

**Source**  
- `core.fact_sales_items`  
- `core.dim_product`

---

##  Inventory Metrics

### Inventory Value
Total monetary value of on-hand inventory at the time of each snapshot.

**Formula**  
`SUM(inventory_value)`

**Source**  
`core.fact_inventory_snapshots`

---

### Inventory Delta
Change in inventory levels within a single snapshot, used as a sanity and operational check.

**Formula**  
`ending_inventory − beginning_inventory`

**Source**  
`core.fact_inventory_snapshots`

> **Note:** Period-over-period inventory change is derived analytically using multiple snapshots
and the date dimension.

---

##  Returns Metrics

### Return Count
Total number of return events recorded.

**Formula**  
`COUNT(DISTINCT return_id)`

**Source**  
`core.fact_returns`

---

### Unit-Based Return Rate
Percentage of sold units that were returned.

**Formula**  
`SUM(quantity_returned) / SUM(quantity_sold)`

**Source**  
- `core.fact_returns`  
- `core.fact_sales_items`

---

### Revenue Return Rate (Advanced KPI)
Percentage of gross revenue lost due to refunds.

**Formula**  
`SUM(refund_amount) / SUM(line_revenue)`

**Source**  
- `core.fact_returns`  
- `core.fact_sales_items`

---

> **Metric Governance Note**  
> All metrics defined above are derived from **conformed fact tables** and exposed through
> analytics-layer views, ensuring that KPIs remain **consistent, explainable, and reproducible**
> across BI tools and analytical workflows.
