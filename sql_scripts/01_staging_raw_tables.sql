/* =============================================================================
   STAGING LAYER - RAW SOURCE TABLES
   -----------------------------------------------------------------------------
   Purpose:
   - Store unmodified data as received from source systems
   - Act as a permanent landing zone for reprocessing and auditability
   -----------------------------------------------------------------------------
   Design Principles:
   - Schema-on-read: all fields stored as VARCHAR
   - No constraints or transformations
   - Mirrors source CSV structure exactly
   - Ingestion metadata added for traceability
============================================================================= */

/* -----------------------------------------------------------------------------
   POS TRANSACTIONS - RAW
   Grain: One row per POS transaction
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.pos_transactions_raw;
GO

CREATE TABLE staging.pos_transactions_raw (
    transaction_id        VARCHAR(255),
    store_id              VARCHAR(255),
    transaction_timestamp VARCHAR(255),
    cashier_id            VARCHAR(255),
    customer_id           VARCHAR(255),
    payment_method        VARCHAR(255),
    total_amount          VARCHAR(255),
    discount_amount       VARCHAR(255),
    tax_amount            VARCHAR(255),
    load_timestamp        DATETIME DEFAULT GETDATE(),
    source_file           VARCHAR(255)
);
GO


/* -----------------------------------------------------------------------------
   POS ORDER ITEMS - RAW
   Grain: One row per product per transaction
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.pos_items_raw;
GO

CREATE TABLE staging.pos_items_raw (
    transaction_id VARCHAR(255),
    product_id     VARCHAR(255),
    quantity       VARCHAR(255),
    unit_price     VARCHAR(255),
    line_total     VARCHAR(255),
    load_timestamp DATETIME DEFAULT GETDATE(),
    source_file    VARCHAR(255)
);
GO


/* -----------------------------------------------------------------------------
   E-COMMERCE ORDERS - RAW
   Grain: One row per e-commerce order
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.ecom_orders_raw;
GO

CREATE TABLE staging.ecom_orders_raw (
    order_id        VARCHAR(255),
    customer_id     VARCHAR(255),
    order_timestamp VARCHAR(255),
    order_status    VARCHAR(255),
    channel         VARCHAR(255),
    shipping_cost   VARCHAR(255),
    total_amount    VARCHAR(255),
    discount_amount VARCHAR(255),
    device_type     VARCHAR(255),
    traffic_source  VARCHAR(255),
    load_timestamp DATETIME DEFAULT GETDATE(),
    source_file    VARCHAR(255)
);
GO


/* -----------------------------------------------------------------------------
   E-COMMERCE ORDER ITEMS - RAW
   Grain: One row per product per e-commerce order
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.ecom_items_raw;
GO

CREATE TABLE staging.ecom_items_raw (
    order_item_id  VARCHAR(255),
    order_id       VARCHAR(255),
    product_id     VARCHAR(255),
    quantity       VARCHAR(255),
    unit_price     VARCHAR(255),
    line_total     VARCHAR(255),
    load_timestamp DATETIME DEFAULT GETDATE(),
    source_file    VARCHAR(255)
);
GO


/* -----------------------------------------------------------------------------
   INVENTORY SNAPSHOTS - RAW
   Grain: One row per product per store per snapshot date
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.inventory_snapshots_raw;
GO

CREATE TABLE staging.inventory_snapshots_raw (
    snapshot_date       VARCHAR(255),
    store_id            VARCHAR(255),
    product_id          VARCHAR(255),
    beginning_inventory VARCHAR(255),
    ending_inventory    VARCHAR(255),
    inventory_value     VARCHAR(255),
    stock_status        VARCHAR(255),
    safety_stock        VARCHAR(255),
    load_timestamp      DATETIME DEFAULT GETDATE(),
    source_file         VARCHAR(255)
);
GO


/* -----------------------------------------------------------------------------
   RETURNS - RAW
   Grain: One row per return event
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.returns_raw;
GO

CREATE TABLE staging.returns_raw (
    return_id         VARCHAR(255),
    transaction_id    VARCHAR(255),
    product_id        VARCHAR(255),
    return_date       VARCHAR(255),
    return_reason     VARCHAR(255),
    refund_amount     VARCHAR(255),
    quantity_returned VARCHAR(255),
    return_channel    VARCHAR(255),
    load_timestamp    DATETIME DEFAULT GETDATE(),
    source_file       VARCHAR(255)
);
GO


/* -----------------------------------------------------------------------------
   PRODUCTS - RAW
   Grain: One row per product
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.products_raw;
GO

CREATE TABLE staging.products_raw (
    product_id     VARCHAR(255),
    sku            VARCHAR(255),
    product_name   VARCHAR(255),
    category       VARCHAR(255),
    subcategory    VARCHAR(255),
    brand          VARCHAR(255),
    cost           VARCHAR(255),
    price          VARCHAR(255),
    season         VARCHAR(255),
    launch_date    VARCHAR(255),
    status         VARCHAR(255),
    load_timestamp DATETIME DEFAULT GETDATE(),
    source_file    VARCHAR(255)
);
GO


/* -----------------------------------------------------------------------------
   STORES - RAW
   Grain: One row per store location
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.stores_raw;
GO

CREATE TABLE staging.stores_raw (
    store_id       VARCHAR(255),
    store_name     VARCHAR(255),
    store_type     VARCHAR(255),
    region         VARCHAR(255),
    address        VARCHAR(255),
    opening_date   VARCHAR(255),
    manager_id     VARCHAR(255),
    load_timestamp DATETIME DEFAULT GETDATE(),
    source_file    VARCHAR(255)
);
GO
