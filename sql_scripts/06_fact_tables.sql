/* =============================================================================
   CORE LAYER - FACT TABLES (ANALYTICAL EVENTS)
   =============================================================================
   Executive Summary:
   Fact tables capture measurable business events at clearly defined grains.
   They are the primary source for KPIs, dashboards, and trend analysis.

   Each fact table is designed to:
   - Represent a single type of business activity
   - Join cleanly to shared (conformed) dimensions
   - Support fast, consistent analytics across BI tools

   -----------------------------------------------------------------------------
   Business Value:
   - Ensures revenue, margin, and inventory metrics are trustworthy
   - Prevents double-counting through explicit grain definitions
   - Enables unified reporting across POS and e-commerce channels
   - Provides a scalable foundation for executive dashboards

   -----------------------------------------------------------------------------
   Input Layer:
   - staging.*_clean tables (Silver / Clean)
   - These inputs already:
     - Enforce correct data types
     - Standardize categorical values
     - Flag bad records (instead of deleting them)
     - Flag duplicates deterministically using is_duplicate

   The fact layer focuses on modeling business events, not data cleanup.
============================================================================= */


/* =============================================================================
   FACT TABLES
============================================================================= */


/* -----------------------------------------------------------------------------
   FACT_POS_TRANSACTIONS - IN-STORE SALES (HEADER LEVEL)
   -----------------------------------------------------------------------------
   Description:
   Captures completed point-of-sale transactions at retail locations.

   Grain:
   One row per POS transaction_id.

   Source:
   staging.pos_transactions_clean

   Business Rules:
   - Only current records (is_duplicate = 0)
   - Exclude records with invalid dates, stores, or totals
   - Net revenue calculated consistently at the transaction level

   Why This Matters:
   - Forms the foundation for in-store revenue reporting
   - Supports cashier, store, and payment-method analysis
   - Protects financial KPIs from invalid or duplicated transactions
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.fact_pos_transactions;
GO

CREATE TABLE core.fact_pos_transactions (
    pos_transaction_sk INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    transaction_id     VARCHAR(100) NOT NULL,
    store_sk           INT NOT NULL,
    date_sk            INT NOT NULL,
    cashier_id         VARCHAR(100),
    payment_method     VARCHAR(50),
    total_amount       DECIMAL(12,2),
    discount_amount    DECIMAL(12,2),
    tax_amount         DECIMAL(12,2),
    net_revenue        DECIMAL(12,2),
    load_timestamp     DATETIME2,
    source_file        VARCHAR(255),

    CONSTRAINT fk_fact_pos_store FOREIGN KEY (store_sk)
        REFERENCES core.dim_store(store_sk),

    CONSTRAINT fk_fact_pos_date FOREIGN KEY (date_sk)
        REFERENCES core.dim_date(date_sk)
);
GO

INSERT INTO core.fact_pos_transactions (
    transaction_id,
    store_sk,
    date_sk,
    cashier_id,
    payment_method,
    total_amount,
    discount_amount,
    tax_amount,
    net_revenue,
    load_timestamp,
    source_file
)
SELECT
    pt.transaction_id,
    ds.store_sk,
    dd.date_sk,
    pt.cashier_id,
    pt.payment_method,
    pt.total_amount,
    pt.discount_amount,
    pt.tax_amount,
    ROUND(
        COALESCE(pt.total_amount, 0)
      - COALESCE(pt.discount_amount, 0)
      + COALESCE(pt.tax_amount, 0), 2
    ) AS net_revenue,
    pt.load_timestamp,
    pt.source_file
FROM staging.pos_transactions_clean pt
JOIN core.dim_store ds
  ON pt.store_id = ds.store_id
JOIN core.dim_date dd
  ON CONVERT(DATE, pt.transaction_timestamp) = dd.full_date
WHERE pt.is_duplicate = 0
  AND pt.is_bad_date = 0
  AND pt.is_bad_store = 0
  AND pt.is_bad_total = 0;
GO


/* -----------------------------------------------------------------------------
   FACT_ECOM_ORDERS - ONLINE SALES (ORDER LEVEL)
   -----------------------------------------------------------------------------
   Description:
   Captures completed e-commerce orders with channel and device attributes.

   Grain:
   One row per e-commerce order_id.

   Source:
   staging.ecom_orders_clean

   Business Rules:
   - Only current records (is_duplicate = 0)
   - Exclude invalid dates and totals
   - Net revenue derived consistently across orders

   Why This Matters:
   - Supports digital revenue and funnel analysis
   - Enables channel, device, and traffic-source reporting
   - Ensures consistent online revenue calculations
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.fact_ecom_orders;
GO

CREATE TABLE core.fact_ecom_orders (
    ecom_order_sk   INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    order_id        VARCHAR(100) NOT NULL,
    date_sk         INT NOT NULL,
    order_status    VARCHAR(50),
    channel         VARCHAR(50),
    device_type     VARCHAR(50),
    traffic_source  VARCHAR(50),
    total_amount    DECIMAL(12,2),
    discount_amount DECIMAL(12,2),
    shipping_cost   DECIMAL(12,2),
    net_revenue     DECIMAL(12,2),
    load_timestamp  DATETIME2,
    source_file     VARCHAR(255),

    CONSTRAINT fk_fact_ecom_date FOREIGN KEY (date_sk)
        REFERENCES core.dim_date(date_sk)
);
GO

INSERT INTO core.fact_ecom_orders (
    order_id,
    date_sk,
    order_status,
    channel,
    device_type,
    traffic_source,
    total_amount,
    discount_amount,
    shipping_cost,
    net_revenue,
    load_timestamp,
    source_file
)
SELECT
    eo.order_id,
    dd.date_sk,
    eo.order_status,
    eo.channel,
    eo.device_type,
    eo.traffic_source,
    eo.total_amount,
    eo.discount_amount,
    eo.shipping_cost,
    ROUND(
        COALESCE(eo.total_amount, 0)
      - COALESCE(eo.discount_amount, 0)
      - COALESCE(eo.shipping_cost, 0), 2
    ) AS net_revenue,
    eo.load_timestamp,
    eo.source_file
FROM staging.ecom_orders_clean eo
JOIN core.dim_date dd
  ON CONVERT(DATE, eo.order_timestamp) = dd.full_date
WHERE eo.is_duplicate = 0
  AND eo.is_bad_date = 0
  AND eo.is_bad_total = 0;
GO


/* -----------------------------------------------------------------------------
   FACT_SALES_ITEMS - UNIFIED LINE ITEMS (POS + E-COMMERCE)
   -----------------------------------------------------------------------------
   Description:
   Unified fact table capturing product-level sales across all channels.

   Grain:
   One row per:
     source_system + transaction_id + product + date

   Sources:
   - POS: pos_items_clean + pos_transactions_clean
   - ECOM: ecom_items_clean + ecom_orders_clean

   Business Rules:
   - Only current (non-duplicate) header and line records
   - Exclude invalid dates, totals, and pricing mismatches
   - Revenue calculated consistently at the line-item level

   Why This Matters:
   - Enables cross-channel product performance analysis
   - Supports SKU-level revenue, quantity, and margin metrics
   - Provides a single, unified view of sales activity
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.fact_sales_items;
GO

CREATE TABLE core.fact_sales_items (
    sales_item_sk  INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    source_system  VARCHAR(10) NOT NULL,
    transaction_id VARCHAR(100) NOT NULL,
    product_sk     INT NOT NULL,
    store_sk       INT NULL,
    date_sk        INT NOT NULL,
    quantity       INT,
    unit_price     DECIMAL(12,2),
    line_revenue   DECIMAL(12,2),
    load_timestamp DATETIME2,
    source_file    VARCHAR(255),

    CONSTRAINT fk_fact_sales_product FOREIGN KEY (product_sk)
        REFERENCES core.dim_product(product_sk),

    CONSTRAINT fk_fact_sales_store FOREIGN KEY (store_sk)
        REFERENCES core.dim_store(store_sk),

    CONSTRAINT fk_fact_sales_date FOREIGN KEY (date_sk)
        REFERENCES core.dim_date(date_sk)
);
GO

;WITH pos_sales AS (
    SELECT
        'POS' AS source_system,
        pi.transaction_id,
        dp.product_sk,
        ds.store_sk,
        dd.date_sk,
        pi.quantity,
        pi.unit_price,
        pi.calculated_line_total AS line_revenue,
        pi.load_timestamp,
        pi.source_file
    FROM staging.pos_items_clean pi
    JOIN staging.pos_transactions_clean pt
      ON pi.transaction_id = pt.transaction_id
    JOIN core.dim_product dp
      ON pi.product_id = dp.product_id
    JOIN core.dim_store ds
      ON pt.store_id = ds.store_id
    JOIN core.dim_date dd
      ON CONVERT(DATE, pt.transaction_timestamp) = dd.full_date
    WHERE pi.is_duplicate = 0
      AND pt.is_duplicate = 0
      AND pt.is_bad_date = 0
      AND pt.is_bad_total = 0
      AND pi.is_bad_qty = 0
      AND pi.is_bad_price = 0
      AND pi.is_line_total_mismatch = 0
),
ecom_sales AS (
    SELECT
        'ECOM' AS source_system,
        ei.order_id AS transaction_id,
        dp.product_sk,
        NULL AS store_sk,
        dd.date_sk,
        ei.quantity,
        ei.unit_price,
        ei.calculated_line_total AS line_revenue,
        ei.load_timestamp,
        ei.source_file
    FROM staging.ecom_items_clean ei
    JOIN staging.ecom_orders_clean eo
      ON ei.order_id = eo.order_id
    JOIN core.dim_product dp
      ON ei.product_id = dp.product_id
    JOIN core.dim_date dd
      ON CONVERT(DATE, eo.order_timestamp) = dd.full_date
    WHERE ei.is_duplicate = 0
      AND eo.is_duplicate = 0
      AND eo.is_bad_date = 0
      AND eo.is_bad_total = 0
      AND ei.is_bad_qty = 0
      AND ei.is_bad_price = 0
      AND ei.is_line_total_mismatch = 0
),
combined AS (
    SELECT * FROM pos_sales
    UNION ALL
    SELECT * FROM ecom_sales
)
INSERT INTO core.fact_sales_items (
    source_system,
    transaction_id,
    product_sk,
    store_sk,
    date_sk,
    quantity,
    unit_price,
    line_revenue,
    load_timestamp,
    source_file
)
SELECT
    source_system,
    transaction_id,
    product_sk,
    store_sk,
    date_sk,
    quantity,
    unit_price,
    line_revenue,
    load_timestamp,
    source_file
FROM combined;
GO


/* -----------------------------------------------------------------------------
   FACT_RETURNS - CUSTOMER RETURN EVENTS
   -----------------------------------------------------------------------------
   Description:
   Captures customer return events across channels.

   Grain:
   One row per return_id.

   Notes:
   - store_sk derived from POS transaction when available
   - E-commerce returns may not have a physical store

   Why This Matters:
   - Enables return rate and refund analysis
   - Supports loss prevention and CX insights
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.fact_returns;
GO

CREATE TABLE core.fact_returns (
    return_sk         INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    return_id         VARCHAR(100) NOT NULL,
    product_sk        INT NOT NULL,
    store_sk          INT NULL,
    date_sk           INT NOT NULL,
    quantity_returned INT,
    refund_amount     DECIMAL(12,2),
    return_reason     VARCHAR(100),
    return_channel    VARCHAR(50),
    load_timestamp    DATETIME2,
    source_file       VARCHAR(255),

    CONSTRAINT fk_fact_returns_product FOREIGN KEY (product_sk)
        REFERENCES core.dim_product(product_sk),

    CONSTRAINT fk_fact_returns_store FOREIGN KEY (store_sk)
        REFERENCES core.dim_store(store_sk),

    CONSTRAINT fk_fact_returns_date FOREIGN KEY (date_sk)
        REFERENCES core.dim_date(date_sk)
);
GO

INSERT INTO core.fact_returns (
    return_id,
    product_sk,
    store_sk,
    date_sk,
    quantity_returned,
    refund_amount,
    return_reason,
    return_channel,
    load_timestamp,
    source_file
)
SELECT
    r.return_id,
    dp.product_sk,
    ds.store_sk,
    dd.date_sk,
    r.quantity_returned,
    r.refund_amount,
    r.return_reason,
    r.return_channel,
    r.load_timestamp,
    r.source_file
FROM staging.returns_clean r
JOIN core.dim_product dp
  ON r.product_id = dp.product_id
LEFT JOIN staging.pos_transactions_clean pt
  ON r.transaction_id = pt.transaction_id
LEFT JOIN core.dim_store ds
  ON pt.store_id = ds.store_id
JOIN core.dim_date dd
  ON CONVERT(DATE, r.return_date) = dd.full_date
WHERE r.is_duplicate = 0
  AND r.is_bad_return_date = 0
  AND r.is_bad_qty = 0
  AND r.is_bad_refund = 0;
GO


/* -----------------------------------------------------------------------------
   FACT_INVENTORY_SNAPSHOTS - POINT-IN-TIME INVENTORY
   -----------------------------------------------------------------------------
   Description:
   Captures inventory levels by product and store at specific points in time.

   Grain:
   One row per snapshot_date + store + product.

   Why This Matters:
   - Supports stock availability and replenishment analysis
   - Enables inventory valuation reporting
   - Detects operational risks (low stock, overstock)
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.fact_inventory_snapshots;
GO

CREATE TABLE core.fact_inventory_snapshots (
    inventory_sk        INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    product_sk          INT NOT NULL,
    store_sk            INT NOT NULL,
    date_sk             INT NOT NULL,
    beginning_inventory INT,
    ending_inventory    INT,
    inventory_value     DECIMAL(14,2),
    safety_stock        INT,
    stock_status        VARCHAR(50),
    load_timestamp      DATETIME2,
    source_file         VARCHAR(255),

    CONSTRAINT fk_fact_inv_product FOREIGN KEY (product_sk)
        REFERENCES core.dim_product(product_sk),

    CONSTRAINT fk_fact_inv_store FOREIGN KEY (store_sk)
        REFERENCES core.dim_store(store_sk),

    CONSTRAINT fk_fact_inv_date FOREIGN KEY (date_sk)
        REFERENCES core.dim_date(date_sk)
);
GO

INSERT INTO core.fact_inventory_snapshots (
    product_sk,
    store_sk,
    date_sk,
    beginning_inventory,
    ending_inventory,
    inventory_value,
    safety_stock,
    stock_status,
    load_timestamp,
    source_file
)
SELECT
    dp.product_sk,
    ds.store_sk,
    dd.date_sk,
    i.beginning_inventory,
    i.ending_inventory,
    i.inventory_value,
    i.safety_stock,
    i.stock_status,
    i.load_timestamp,
    i.source_file
FROM staging.inventory_snapshots_clean i
JOIN core.dim_product dp
  ON i.product_id = dp.product_id
JOIN core.dim_store ds
  ON i.store_id = ds.store_id
JOIN core.dim_date dd
  ON CONVERT(DATE, i.snapshot_date) = dd.full_date
WHERE i.is_duplicate = 0
  AND i.is_bad_date = 0
  AND i.is_bad_store = 0;
GO
