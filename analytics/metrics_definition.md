# Metrics Definitions

This document defines the **core business KPIs** supported by the Retail Analytics BI System.
All metrics are calculated from **Gold-layer (core) tables**, ensuring consistency,
accuracy, and reuse across dashboards and analyses.

---

## Revenue Metrics

### Total Revenue
Total sales generated before discounts and refunds, calculated at the line-item level.

**Formula**  
`SUM(line_revenue)`

**Source**  
`core.fact_sales_items`

---

### Net Revenue
Revenue earned after discounts and shipping adjustments, representing realized revenue.

**Formula**  
`SUM(net_revenue)`

**Source**  
- `core.fact_pos_transactions`  
- `core.fact_ecom_orders`

---

### Average Order Value (AOV)
Average revenue generated per transaction.

**Formula**  
`SUM(net_revenue) / COUNT(DISTINCT transaction_id)`

**Source**  
`core.fact_sales_items`

---

## Sales & Volume Metrics

### Units Sold
Total number of product units sold.

**Formula**  
`SUM(quantity)`

**Source**  
`core.fact_sales_items`

---

### Order Count
Total number of distinct transactions across all sales channels.

**Formula**  
`COUNT(DISTINCT transaction_id)`

**Source**  
`core.fact_sales_items`

---

## Profitability Metrics

### Gross Margin
Per-unit profit after cost of goods sold.

**Formula**  
`price − cost`

**Source**  
`core.dim_product`

---

### Total Margin
Total contribution margin generated from product sales.

**Formula**  
`SUM(quantity × margin)`

**Source**  
`core.fact_sales_items` + `core.dim_product`

---

## Inventory Metrics

### Inventory Value
Total value of on-hand inventory at the time of snapshot.

**Formula**  
`SUM(inventory_value)`

**Source**  
`core.fact_inventory_snapshots`

---

### Inventory Delta
Change in inventory levels between the beginning and end of a given period.

**Formula**  
`ending_inventory − beginning_inventory`

**Source**  
`core.fact_inventory_snapshots`

---

## Returns Metrics

### Return Count
Total number of return transactions.

**Formula**  
`COUNT(DISTINCT return_id)`

**Source**  
`core.fact_returns`

---

### Return Rate
Percentage of sold units that were returned.

**Formula**  
`SUM(returned_quantity) / SUM(sold_quantity)`

**Source**  
`core.fact_returns` + `core.fact_sales_items`
