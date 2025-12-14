#  Retail Analytics BI System  
**End-to-End ELT Data Warehouse for Retail Analytics** 

---

##  Table of Contents

- [General Info](#-general-info)
- [Project Objectives](#-project-objectives)
- [Project Description](#-project-description)
- [Definition of Key Project Deliverables](#-definition-of-key-project-deliverables)
- [Database Schema & Dimensional Model](#-database-schema--dimensional-model)
- [Project Processes](#-project-processes)
- [Business Impact](#-business-impact)
- [Business Impact Summary](#-business-impact-summary)
- [Key Skills Demonstrated](#-key-skills-demonstrated)

---

##  General Info

Retail organizations typically operate across multiple disconnected operational systems, such as in-store POS platforms, e-commerce systems, inventory management tools, and returns processing systems. While these systems generate large volumes of data, they often lack a centralized analytics foundation that enables consistent, reliable reporting.

As a result, analytics teams struggle to answer fundamental business questions around **revenue performance, product profitability, inventory efficiency, and return behavior**.

This project addresses that gap by designing and implementing a **production-style Retail Analytics Data Warehouse**, applying **modern ELT and dimensional modeling best practices** to transform raw operational data into a **trusted, analytics-ready source of truth** for business intelligence and decision-making.

---

##  Project Objectives

The objectives of this project are to:

- Design and implement a **production-style Retail Analytics Data Warehouse** using SQL Server  
- Apply **modern ELT architecture (Bronze → Silver → Gold)** aligned with analytics engineering best practices  
- Perform **dimensional data modeling (star schema)** optimized for BI and analytical queries  
- Integrate **POS, E-commerce, Inventory, Products, Stores, and Returns** data into a single source of truth  
- Enforce **data quality, consistency, and auditability** through standardized cleaning and validation logic  
- Deliver **business-ready analytical views** that support executive reporting and operational decision-making  
- Enable stakeholders to analyze **revenue, profitability, inventory health, and returns** at scale  

---

##  Project Description

A retail organization operates across multiple sales channels, including physical stores (POS) and e-commerce platforms. Each system generates its own datasets for sales, inventory, products, and returns, resulting in fragmented data and inconsistent reporting.

The analytics team lacks a centralized, reliable way to answer core business questions such as:

- How is net revenue trending across channels?  
- Which products and categories drive profitability?  
- Where are inventory risks and inefficiencies occurring?  
- Which products experience high return rates and revenue leakage?  

To solve this, a **Retail Analytics BI System** was designed to consolidate raw operational data into a centralized analytics data warehouse. The system transforms raw CSV extracts into a **clean, conformed dimensional model**, optimized for BI tools and ad-hoc SQL analysis.

This approach mirrors **real-world analytics engineering workflows** used by retail, e-commerce, and consumer brands.

---

##  Definition of Key Project Deliverables

###  Datasets

The project integrates data extracted from multiple operational systems. All source data is ingested as **CSV files** into the staging (Bronze) layer.

Primary datasets include:

- `pos_transactions_raw`  
- `pos_items_raw`  
- `ecom_orders_raw`  
- `ecom_items_raw`  
- `inventory_snapshots_raw`  
- `returns_raw`  
- `products_raw`  
- `stores_raw`  

---

### 1️. POS Transactions Dataset

**Grain:** One row per in-store transaction  
**Purpose:** Capture in-store revenue, payment methods, discounts, and taxes  

**Key attributes**
- Transaction ID  
- Store ID  
- Transaction timestamp  
- Payment method  
- Total, discount, tax, and net revenue  

**Business value**
- Enables **store-level revenue analysis**  
- Supports **AOV and transaction-based KPIs**  

---

### 2️. E-Commerce Orders Dataset

**Grain:** One row per e-commerce order  
**Purpose:** Capture online order revenue and digital attributes  

**Key attributes**
- Order ID  
- Order timestamp  
- Channel (Web / App)  
- Device type  
- Traffic source  
- Discounts, shipping, and net revenue  

**Business value**
- Enables **digital channel performance analysis**  
- Supports **marketing and attribution insights**  

---

### 3️. Sales Line Items Dataset (POS + E-Commerce)

**Grain:** One row per product per transaction  
**Purpose:** Unified view of all product-level sales activity  

**Key attributes**
- Product ID  
- Quantity sold  
- Unit price  
- Line revenue  
- Source system (POS / ECOM)  

**Business value**
- Supports **product and category performance analysis**  
- Enables **margin and product-mix optimization**  

---

### 4️. Inventory Snapshots Dataset

**Grain:** One row per product per store per date  
**Purpose:** Point-in-time inventory tracking  

**Key attributes**
- Beginning and ending inventory  
- Inventory value  
- Safety stock  
- Stock status  

**Business value**
- Detects **stock-out risk**  
- Supports **inventory turnover and capital efficiency analysis**  

---

### 5️. Returns Dataset

**Grain:** One row per return event  
**Purpose:** Track refunds and customer dissatisfaction signals  

**Key attributes**
- Return quantity  
- Refund amount  
- Return reason  
- Return channel  

**Business value**
- Enables **return rate and refund impact analysis**  
- Identifies **quality and customer experience issues**  

---

##  Database Schema & Dimensional Model

The data warehouse uses a **star schema** to optimize analytical performance and simplify BI queries.

###  Fact Tables
- `fact_pos_transactions` – In-store transaction revenue  
- `fact_ecom_orders` – Online order-level revenue  
- `fact_sales_items` – Unified POS + ECOM line-item sales  
- `fact_inventory_snapshots` – Point-in-time inventory positions  
- `fact_returns` – Refunds and return behavior  

###  Dimension Tables
- `dim_date` – Calendar and time attributes  
- `dim_product` – Product pricing, cost, margin, lifecycle  
- `dim_store` – Store attributes and regional hierarchy  

### Why Star Schema?
- Fast aggregations for BI dashboards  
- Simple joins for analysts  
- Clear grain definition prevents metric distortion  
- Industry-standard design used in production data warehouses  

---

##  Project Processes

### 1️. Database & Schema Setup
- Create database and schemas (`staging`, `core`, `analytics`)  
- Define raw staging tables (Bronze)  
- Define cleaned staging tables (Silver)  
- Define dimension and fact tables (Gold)  
- Apply primary keys, foreign keys, and unique constraints  

---

### 2️. ELT Pipeline (SQL-Based)

**Extract**
- Load CSV files into raw staging tables using `BULK INSERT`

**Load**
- Preserve raw data exactly as received  
- Capture ingestion metadata for auditability  

**Transform**
- Standardize data using `TRY_CONVERT`  
- Normalize categorical fields  
- Recalculate and reconcile financial metrics  
- Flag data quality issues instead of dropping records  
- Deduplicate deterministically using window functions  

---

### 3️. Analytics Layer
- Build **BI-ready views** that abstract complex joins  
- Define consistent business logic for KPIs  
- Optimize for **Power BI, Tableau, Looker, and SQL analysis**  

---

##  Business Impact

This warehouse is designed to **drive decisions, not just store data**.

### Executive & Finance Teams
- Track **net revenue trends** across POS and E-commerce  
- Understand **channel mix shifts and growth drivers**  
- Monitor **AOV, unit volume, and contribution margin**  
- Support **forecasting, budgeting, and performance reviews**  

### Merchandising & Product Teams
- Identify **top and bottom-performing products**  
- Evaluate **product-level margins and pricing effectiveness**  
- Optimize **category and assortment mix**  
- Detect **products with high return rates or negative margins**  

### Inventory & Operations
- Monitor **inventory levels and safety stock breaches**  
- Detect **slow-moving or overstocked products**  
- Improve **inventory turnover and capital efficiency**  
- Align stock availability with sales demand  

### Retail & E-Commerce Teams
- Compare **in-store vs online performance**  
- Analyze **device and traffic source behavior**  
- Understand **returns by channel and reason**  
- Reduce **revenue leakage and customer friction**  

---

##  Business Impact Summary

This system enables the organization to:

- Trust revenue and profitability metrics  
- Identify growth drivers and operational risks  
- Improve inventory efficiency and cash flow  
- Reduce return-related revenue leakage  
- Empower analysts with **self-service, analytics-ready data**  

---
