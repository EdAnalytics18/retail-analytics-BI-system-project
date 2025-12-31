# Data Catalog — Core Layer (Gold)

## Overview
The Core Layer represents the trusted analytical foundation of the Retail Analytics BI System.
Data in this layer is modeled using conformed star schemas, optimized for analytics, BI dashboards, and ad-hoc SQL queries.

It consists of:
Dimension tables providing descriptive business context
Fact tables capturing measurable business events at clearly defined grains

All Core-layer tables:
Use surrogate keys for performance and stability
Retain natural keys for traceability
Are sourced exclusively from validated Silver (clean staging) tables
Serve as the single source of truth for KPIs across the organization

---

### 1. **core.dim_date**
- **Purpose:** Centralized calendar dimension used for all date-based reporting.
- **Grain:** One row per calendar date.
- **Columns:**

| Column Name      | Data Type     | Description                                                                                   |
|------------------|---------------|-----------------------------------------------------------------------------------------------|
| date_sk          | INT           | Surrogate key representing the date in YYYYMMDD format.                                       |
| full_date        | DATE          | Calendar date (unique)                                                                        |
| year_num         | INT           | Calendar year                                                                                 |
| quarter_num      | INT           | Calendar quanrter (1-4)                                                                       |
| month_num        | INT           | Calendar month number (1-12)                                                                  |
| month_name       | VARCHAR(20)   | Month name (e.g., January)                                                                    |
| day_num          | INT           | Day of month                                                                                  |
| day_name         | VARCHAR(20)   | Day of week (e.g., Monday)                                                                    |
| is_weekend       | BIT           | Indicates whether the date falls on a weekend                                                 |
| load_timestamp   | DATETIME2     | Timestamp when the record was generated                                                       |

---

### 2. **core.dim_product**
- **Purpose:** Provides trusted product master data used across sales, inventory, returns, and profitability analysis.
- **Grain:** One row per product (natural key: product_id).
- **Columns:**

| Column Name      | Data Type     | Description                                                                                   |
|------------------|---------------|-----------------------------------------------------------------------------------------------|
| product_sk       | INT           | Surrogate key identifying the product                                                         |
| product_id       | INT           | Natural product identifier                                                                    |
| sku              | VARCHAR(100)  | Stock keeping unit                                                                            |
| product_name     | VARCHAR(255)  | Descriptive product name                                                                      |
| category         | VARCHAR(50)   | High-level product category                                                                   |
| subcategory      | VARCHAR(50)   | Product subcategory                                                                           |
| brand            | VARCHAR(50)   | Brand name                                                                                    |
| cost             | DECIMAL(12,2) | Unit cost                                                                                     |
| price            | DECIMAL(12,2) | Unit selling price                                                                            |
| margin           | DECIMAL(12,2) | Unit margin (price -cost)                                                                     |
| season           | VARCHAR(50)   | Seasonal classification                                                                       |
| launch_date      | DATE          | Product launch date                                                                           |
| status           | VARCHAR(50)   | Product lifecycle status                                                                      |
| load_timestamp   | DATETIME2     | Ingestion timestamp                                                                           |
| source_file      | VARCHAR(255)  | Source file name                                                                              |

---
