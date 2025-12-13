/* =============================================================================
   CORE LAYER - FACT TABLES
   -----------------------------------------------------------------------------
   Purpose:
   - Store measurable business events at explicitly defined grains
   - Serve as the analytical backbone for KPIs, dashboards, and trend analysis
   -----------------------------------------------------------------------------
   Facts Included:
   - POS Transactions
   - E-Commerce Orders
   - Unified Sales Line Items (POS + ECOM)
   - Returns
   - Inventory Snapshots
============================================================================= */


/* -----------------------------------------------------------------------------
   FACT_POS_TRANSACTIONS
   Grain: One row per POS transaction
   Represents:
   - In-store revenue events
   - Payment and cashier-level attributes
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.fact_pos_transactions;
GO

CREATE TABLE core.fact_pos_transactions (
    pos_transaction_sk INT IDENTITY(1,1) PRIMARY KEY,
    transaction_id     VARCHAR(100) NOT NULL,
    store_sk           INT NOT NULL,
    date_sk            INT NOT NULL,
    cashier_id         VARCHAR(100),
    payment_method     VARCHAR(50),
    total_amount       DECIMAL(12,2),
    discount_amount    DECIMAL(12,2),
    tax_amount         DECIMAL(12,2),
    net_revenue        DECIMAL(12,2),
    load_timestamp     DATETIME,
    source_file        VARCHAR(255)
);
GO

;WITH deduplicate AS (
    SELECT
        pt.transaction_id,
        ds.store_sk,
        dd.date_sk,
        pt.cashier_id,
        pt.payment_method,
        pt.total_amount,
        pt.discount_amount,
        pt.tax_amount,
        pt.total_amount - pt.discount_amount AS net_revenue,
        pt.load_timestamp,
        pt.source_file,
        ROW_NUMBER() OVER (
            PARTITION BY pt.transaction_id
            ORDER BY pt.load_timestamp DESC
        ) AS rn
    FROM 
        staging.pos_transactions_clean pt
    JOIN 
        core.dim_store ds
        ON pt.store_id = ds.store_id
    JOIN 
        core.dim_date dd
        ON CAST(pt.transaction_timestamp AS DATE) = dd.full_date
)
INSERT INTO core.fact_pos_transactions
SELECT
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
FROM deduplicate
WHERE rn = 1;
GO


/* -----------------------------------------------------------------------------
   FACT_ECOM_ORDERS
   Grain: One row per e-commerce order
   Represents:
   - Online order-level revenue
   - Channel, device, and traffic attribution
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.fact_ecom_orders;
GO

CREATE TABLE core.fact_ecom_orders (
    ecom_order_sk   INT IDENTITY(1,1) PRIMARY KEY,
    order_id        VARCHAR(100) NOT NULL,
    date_sk         INT NOT NULL,
    order_status    VARCHAR(50),
    channel         VARCHAR(50),
    device_type     VARCHAR(50),
    traffic_source  VARCHAR(255),
    total_amount    DECIMAL(12,2),
    discount_amount DECIMAL(12,2),
    shipping_cost   DECIMAL(12,2),
    net_revenue     DECIMAL(12,2),
    load_timestamp  DATETIME,
    source_file     VARCHAR(255)
);
GO

;WITH deduplicate AS (
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
        eo.net_revenue,
        eo.load_timestamp,
        eo.source_file,
        ROW_NUMBER() OVER (
            PARTITION BY eo.order_id
            ORDER BY eo.load_timestamp DESC
        ) AS rn
    FROM 
        staging.ecom_orders_clean eo
    JOIN 
        core.dim_date dd
        ON CAST(eo.order_timestamp AS DATE) = dd.full_date
)
INSERT INTO core.fact_ecom_orders
SELECT
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
FROM deduplicate
WHERE rn = 1;
GO


/* -----------------------------------------------------------------------------
   FACT_SALES_ITEMS
   Grain:
   - One row per product per transaction per day
   - Unified POS and E-Commerce line-level sales
   -----------------------------------------------------------------------------
   Design Notes:
   - source_system distinguishes POS vs ECOM
   - store_sk is nullable for E-Commerce rows
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.fact_sales_items;
GO

CREATE TABLE core.fact_sales_items (
    sales_item_sk  INT IDENTITY(1,1) PRIMARY KEY,
    source_system  VARCHAR(10) NOT NULL,
    transaction_id VARCHAR(100) NOT NULL,
    product_sk     INT NOT NULL,
    store_sk       INT NULL,
    date_sk        INT NOT NULL,
    quantity       INT,
    unit_price     DECIMAL(12,2),
    line_revenue   DECIMAL(12,2),
    load_timestamp DATETIME,
    source_file    VARCHAR(255)
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
    FROM 
        staging.pos_items_clean pi
    JOIN 
        staging.pos_transactions_clean pt
        ON pi.transaction_id = pt.transaction_id
    JOIN 
        core.dim_product dp
        ON pi.product_id = dp.product_id
    JOIN 
        core.dim_store ds
        ON pt.store_id = ds.store_id
    JOIN 
        core.dim_date dd
        ON CAST(pt.transaction_timestamp AS DATE) = dd.full_date
),
ecom_sales AS (
    SELECT
        'ECOM',
        ei.order_id,
        dp.product_sk,
        NULL,
        dd.date_sk,
        ei.quantity,
        ei.unit_price,
        ei.calculated_line_total,
        ei.load_timestamp,
        ei.source_file
    FROM 
        staging.ecom_items_clean ei
    JOIN 
        staging.ecom_orders_clean eo
        ON ei.order_id = eo.order_id
    JOIN 
        core.dim_product dp
        ON ei.product_id = dp.product_id
    JOIN 
       core.dim_date dd
        ON CAST(eo.order_timestamp AS DATE) = dd.full_date
),
combined AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY source_system, transaction_id, product_sk
               ORDER BY load_timestamp DESC
           ) AS rn
    FROM (
        SELECT * FROM pos_sales
        UNION ALL
        SELECT * FROM ecom_sales
    ) x
)
INSERT INTO core.fact_sales_items
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
FROM combined
WHERE rn = 1;
GO


/* -----------------------------------------------------------------------------
   FACT_RETURNS
   Grain: One row per returned product per return event
   Represents:
   - Refunds and customer dissatisfaction signals
   - Supports return-rate and loss analysis
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.fact_returns;
GO

CREATE TABLE core.fact_returns (
    return_sk         INT IDENTITY(1,1) PRIMARY KEY,
    return_id         VARCHAR(100) NOT NULL,
    product_sk        INT NOT NULL,
    store_sk          INT NULL,        
    date_sk           INT NOT NULL,
    quantity_returned INT,
    refund_amount     DECIMAL(12,2),
    return_reason     VARCHAR(100),
    return_channel    VARCHAR(50),
    load_timestamp    DATETIME,
    source_file       VARCHAR(255)
);
GO

;WITH deduplicate AS (
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
        r.source_file,
        ROW_NUMBER() OVER (
            PARTITION BY r.return_id
            ORDER BY r.load_timestamp DESC
        ) AS rn
    FROM 
        staging.returns_clean r
    JOIN 
        core.dim_product dp
        ON r.product_id = dp.product_id
    LEFT JOIN 
        staging.pos_transactions_clean pt
        ON r.transaction_id = pt.transaction_id
    LEFT JOIN 
        core.dim_store ds
        ON pt.store_id = ds.store_id
    JOIN 
        core.dim_date dd
        ON r.return_date = dd.full_date
)
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
FROM deduplicate
WHERE rn = 1;
GO


/* -----------------------------------------------------------------------------
   FACT_INVENTORY_SNAPSHOTS
   Grain: One row per product per store per snapshot date
   Represents:
   - Point-in-time inventory positions
   - Enables stock-level and supply analysis
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.fact_inventory_snapshots;
GO

CREATE TABLE core.fact_inventory_snapshots (
    inventory_sk        INT IDENTITY(1,1) PRIMARY KEY,
    product_sk          INT NOT NULL,
    store_sk            INT NOT NULL,
    date_sk             INT NOT NULL,
    beginning_inventory INT,
    ending_inventory    INT,
    inventory_value     DECIMAL(14,2),
    safety_stock        INT,
    stock_status        VARCHAR(50),
    load_timestamp      DATETIME,
    source_file         VARCHAR(255)
);
GO

;WITH deduplicate AS (
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
        i.source_file,
        ROW_NUMBER() OVER (
            PARTITION BY dp.product_sk, ds.store_sk, dd.date_sk
            ORDER BY i.load_timestamp DESC
        ) AS rn
    FROM 
        staging.inventory_snapshots_clean i
    JOIN 
        core.dim_product dp
        ON i.product_id = dp.product_id
    JOIN 
        core.dim_store ds
        ON i.store_id = ds.store_id
    JOIN 
        core.dim_date dd
        ON i.snapshot_date = dd.full_date
)
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
FROM deduplicate
WHERE rn = 1;
GO
