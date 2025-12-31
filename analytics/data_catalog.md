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

### 3. **core.dim_store**
- **Purpose:** Represents physical retail locations and their organizational attributes.
- **Grain:** One row per store (natural key: store_id).
- **Columns:**

| Column Name      | Data Type     | Description                                                                                   |
|------------------|---------------|-----------------------------------------------------------------------------------------------|
| store_sk         | INT           | Surrogate key identifying the store                                                           |
| store_id         | INT           | Natural product identifier                                                                    |
| store_name       | VARCHAR(255)  | Store name                                                                                    |
| store_type       | VARCHAR(50)   | Store type                                                                                    |
| region           | VARCHAR(50)   | Geographic region                                                                             |
| address          | VARCHAR(255)  | Store address                                                                                 |
| opening_date     | DATE          | Store opening date                                                                            |
| manager_id       | VARCHAR(100)  | Store manager identifier                                                                      |
| load_timestamp   | DATETIME2     | Ingestion timestamp                                                                           |
| source_file      | VARCHAR(255)  | Source file name                                                                              |

---

### 4. **core.fact_pos_transactions**
- **Purpose:** Captures completed in-store point-of-sale transactions at the transaction (header) level.
- **Grain:** One row per POS transaction
- **Columns:**

| Column Name          | Data Type        | Description                                       |
|----------------------|------------------|---------------------------------------------------|
| pos_transaction_sk   | INT              | Surrogate key                                     |
| transaction_id       | VARCHAR(100)     | POS transaction identifier                        |
| store_sk             | INT              | Foreign key to `dim_store`                        |
| date_sk              | INT              | Foreign key to `dim_date`                         |
| cashier_id           | VARCHAR(100)     | Cashier identifier                                |
| payment_method       | VARCHAR(50)      | Payment method                                    |
| total_amount         | DECIMAL(12,2)    | Gross transaction amount                          |
| discount_amount      | DECIMAL(12,2)    | Discount applied                                  |
| tax_amount           | DECIMAL(12,2)    | Tax amount                                        |
| net_revenue          | DECIMAL(12,2)    | Net revenue after discounts and tax               |
| load_timestamp       | DATETIME2        | Ingestion timestamp                               |
| source_file          | VARCHAR(255)     | Source file name                                  |

---

### 5. **core.fact_ecom_orders**
- **Purpose:** Stores completed e-commerce orders with digital channel attributes.
- **Grain:** One row per e-commerce order
- **Columns:**

| Column Name       | Data Type        | Description                               |
|-------------------|------------------|-------------------------------------------|
| ecom_order_sk     | INT              | Surrogate key                             |
| order_id          | VARCHAR(100)     | E-commerce order identifier               |
| date_sk           | INT              | Foreign key to `dim_date`                 |
| order_status      | VARCHAR(50)      | Order status                              |
| channel           | VARCHAR(50)      | Sales channel                             |
| device_type       | VARCHAR(50)      | Device used                               |
| traffic_source    | VARCHAR(50)      | Marketing traffic source                  |
| total_amount      | DECIMAL(12,2)    | Gross order amount                        |
| discount_amount   | DECIMAL(12,2)    | Discount applied                          |
| shipping_cost     | DECIMAL(12,2)    | Shipping cost                             |
| net_revenue       | DECIMAL(12,2)    | Net revenue                               |
| load_timestamp    | DATETIME2        | Ingestion timestamp                       |
| source_file       | VARCHAR(255)     | Source file name                          |

---

### 6. **core.fact_sales_items**
- **Purpose:** Unified product-level sales fact across POS and E-commerce.
- **Grain:** One row per product per transaction per channel per date
- **Columns:**

| Column Name        | Data Type        | Description                                        |
|--------------------|------------------|----------------------------------------------------|
| sales_item_sk      | INT              | Surrogate key                                      |
| source_system      | VARCHAR(10)      | POS or ECOM                                        |
| transaction_id     | VARCHAR(100)     | Transaction or order identifier                    |
| product_sk         | INT              | Foreign key to `dim_product`                       |
| store_sk           | INT              | Foreign key to `dim_store` (nullable for ECOM)     |
| date_sk            | INT              | Foreign key to `dim_date`                          |
| quantity           | INT              | Units sold                                         |
| unit_price         | DECIMAL(12,2)    | Unit price                                         |
| line_revenue       | DECIMAL(12,2)    | Revenue for the line item                          |
| load_timestamp     | DATETIME2        | Ingestion timestamp                                |
| source_file        | VARCHAR(255)     | Source file name                                   |

---

### 7. **core.fact_returns**
- **Purpose:** Captures customer return events across sales channels.
- **Grain:** One row per return event
- **Columns:**

| Column Name          | Data Type        | Description                                      |
|----------------------|------------------|--------------------------------------------------|
| return_sk            | INT              | Surrogate key                                    |
| return_id            | VARCHAR(100)     | Return identifier                                |
| product_sk           | INT              | Foreign key to `dim_product`                     |
| store_sk             | INT              | Foreign key to `dim_store` (nullable)            |
| date_sk              | INT              | Foreign key to `dim_date`                        |
| quantity_returned    | INT              | Units returned                                   |
| refund_amount        | DECIMAL(12,2)    | Refund value                                     |
| return_reason        | VARCHAR(100)     | Reason for return                                |
| return_channel       | VARCHAR(50)      | POS or ECOM                                      |
| load_timestamp       | DATETIME2        | Ingestion timestamp                              |
| source_file          | VARCHAR(255)     | Source file name                                 |

