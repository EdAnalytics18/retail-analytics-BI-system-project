/* =============================================================================
   STAGING LAYER - RAW SOURCE TABLES
   -----------------------------------------------------------------------------
   Purpose:
   - Store unmodified data as received from source systems
   - Act as a permanent landing zone for reprocessing and auditability
   -----------------------------------------------------------------------------
   Design Principles:
   - All fields stored as VARCHAR (schema-on-read)
   - No constraints or transformations
   - Exactly mirrors the source CSV structure
============================================================================= */

/* -----------------------------------------------------------------------------
   POS Transactions (Store Sales) - RAW
   Grain: One row per POS transaction
----------------------------------------------------------------------------- */
CREATE TABLE staging.pos_transactions_raw (
    transaction_id        VARCHAR(50),
    store_id              VARCHAR(50),
    transaction_timestamp VARCHAR(50),
    cashier_id            VARCHAR(50),
    customer_id           VARCHAR(50),
    payment_method        VARCHAR(50),
    total_amount          VARCHAR(50),
    discount_amount       VARCHAR(50),
    tax_amount            VARCHAR(50)
);
GO


/* -----------------------------------------------------------------------------
   POS ORDER ITEMS - RAW
   Source System: In-Store POS
   Grain: One row per product per transaction
----------------------------------------------------------------------------- */
CREATE TABLE staging.pos_items_raw (
    transaction_id    VARCHAR(50),
    product_id        VARCHAR(50),
    quantity          VARCHAR(50),
    unit_price        VARCHAR(50),
    line_total        VARCHAR(50)
);
GO


/* -----------------------------------------------------------------------------
   E-COMMERCE ORDERS - RAW
   Source System: Online Store Platform
   Grain: One row per e-commerce order
----------------------------------------------------------------------------- */
CREATE TABLE staging.ecom_orders_raw (
    order_id        VARCHAR(50),
    customer_id     VARCHAR(50),
    order_timestamp VARCHAR(50),
    order_status    VARCHAR(50),
    channel         VARCHAR(50),
    shipping_cost   VARCHAR(50),
    total_amount    VARCHAR(50),
    discount_amount VARCHAR(50),
    device_type     VARCHAR(50),
    traffic_source  VARCHAR(50)
);
GO


/* -----------------------------------------------------------------------------
   E-COMMERCE ORDER ITEMS - RAW
   Source System: Online Store Platform
   Grain: One row per product per e-commerce order
----------------------------------------------------------------------------- */
CREATE TABLE staging.ecom_items_raw (
    order_item_id VARCHAR(50),
    order_id      VARCHAR(50),
    product_id    VARCHAR(50),
    quantity      VARCHAR(50),
    unit_price    VARCHAR(50),
    line_total    VARCHAR(50)
);
GO


/* -----------------------------------------------------------------------------
   INVENTORY SNAPSHOTS - RAW
   Source System: Inventory / ERP System
   Grain: One row per product per store per snapshot date
----------------------------------------------------------------------------- */
CREATE TABLE staging.inventory_snapshots_raw (
    snapshot_date       VARCHAR(50),
    store_id            VARCHAR(50),
    product_id          VARCHAR(50),
    beginning_inventory VARCHAR(50),
    ending_inventory    VARCHAR(50),
    inventory_value     VARCHAR(50),
    stock_status        VARCHAR(50),
    safety_stock        VARCHAR(50)
);
GO


/* -----------------------------------------------------------------------------
   RETURNS - RAW
   Source System: POS & E-Commerce Returns
   Grain: One row per return event
----------------------------------------------------------------------------- */
CREATE TABLE staging.returns_raw (
    return_id         VARCHAR(50),
    transaction_id    VARCHAR(50),
    product_id        VARCHAR(50),
    return_date       VARCHAR(50),
    return_reason     VARCHAR(50),
    refund_amount     VARCHAR(50),
    quantity_returned VARCHAR(50),
    return_channel    VARCHAR(50)
);
GO


/* -----------------------------------------------------------------------------
   PRODUCTS - RAW
   Source System: Product Master / PIM
   Grain: One row per product
----------------------------------------------------------------------------- */
CREATE TABLE staging.products_raw (
    product_id   VARCHAR(50),
    sku          VARCHAR(50),
    product_name VARCHAR(50),
    category     VARCHAR(50),
    subcategory  VARCHAR(50),
    brand        VARCHAR(50),
    cost         VARCHAR(50),
    price        VARCHAR(50),
    season       VARCHAR(50),
    launch_date  VARCHAR(50),
    status       VARCHAR(50)
);
GO


/* -----------------------------------------------------------------------------
   STORES - RAW
   Source System: Store Master
   Grain: One row per store location
----------------------------------------------------------------------------- */
CREATE TABLE staging.stores_raw (
    store_id     VARCHAR(50),
    store_name   VARCHAR(50),
    store_type   VARCHAR(50),
    region       VARCHAR(50),
    address      VARCHAR(50),
    opening_date VARCHAR(50),
    manager_id   VARCHAR(50)
);
GO