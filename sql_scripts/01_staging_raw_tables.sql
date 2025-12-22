/* =============================================================================
   STAGING LAYER - RAW SOURCE DATA (SYSTEM OF RECORD)
   -----------------------------------------------------------------------------
   Purpose:
   This layer serves as the first entry point for all incoming retail data
   from source systems such as POS, e-commerce platforms, inventory tools,
   and product master data.

   Data in this layer is stored exactly as received, without modification,
   ensuring full transparency, traceability, and reprocessability.

   -----------------------------------------------------------------------------
   Business Value:
   - Preserves original source data for audits, investigations, and backfills
   - Prevents data loss or distortion during ingestion
   - Enables safe reprocessing when business rules change
   - Creates a clear separation between raw data and business logic

   -----------------------------------------------------------------------------
   Design Principles:
   - Schema-on-read:
     All fields are stored as VARCHAR to avoid ingestion failures caused by
     unexpected formats, missing values, or upstream system changes.

   - No transformations or constraints:
     Raw data remains untouched so downstream logic is explicit, testable,
     and version-controlled.

   - Source-aligned structure:
     Tables mirror the structure of incoming CSV files exactly, making it
     easy to validate row counts and field mappings.

   - Traceability-ready:
     This layer supports ingestion metadata (e.g. load timestamps, source
     system identifiers) for debugging and governance.
============================================================================= */


/* -----------------------------------------------------------------------------
   POS TRANSACTIONS - RAW
   -----------------------------------------------------------------------------
   Description:
   Captures point-of-sale transactions exactly as recorded at retail locations.

   Grain:
   One row per completed POS transaction.

   Business Use:
   Forms the foundation for in-store revenue reporting, payment analysis,
   cashier performance, and tax calculations.
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
    tax_amount            VARCHAR(255)
);
GO


/* -----------------------------------------------------------------------------
   POS ORDER ITEMS - RAW
   -----------------------------------------------------------------------------
   Description:
   Captures individual products sold within each POS transaction.

   Grain:
   One row per product per transaction.

   Business Use:
   Enables product-level sales analysis, basket composition insights,
   and margin calculations.
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.pos_items_raw;
GO

CREATE TABLE staging.pos_items_raw (
    transaction_id VARCHAR(255),
    product_id     VARCHAR(255),
    quantity       VARCHAR(255),
    unit_price     VARCHAR(255),
    line_total     VARCHAR(255)
);
GO


/* -----------------------------------------------------------------------------
   E-COMMERCE ORDERS - RAW
   -----------------------------------------------------------------------------
   Description:
   Captures online customer orders from the e-commerce platform.

   Grain:
   One row per e-commerce order.

   Business Use:
   Supports digital revenue tracking, channel performance analysis,
   and customer behavior insights.
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
    traffic_source  VARCHAR(255)
);
GO


/* -----------------------------------------------------------------------------
   E-COMMERCE ORDER ITEMS - RAW
   -----------------------------------------------------------------------------
   Description:
   Captures individual products purchased within each online order.

   Grain:
   One row per product per e-commerce order.

   Business Use:
   Enables SKU-level digital sales analysis, conversion insights,
   and cross-sell / upsell reporting.
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.ecom_items_raw;
GO

CREATE TABLE staging.ecom_items_raw (
    order_item_id  VARCHAR(255),
    order_id       VARCHAR(255),
    product_id     VARCHAR(255),
    quantity       VARCHAR(255),
    unit_price     VARCHAR(255),
    line_total     VARCHAR(255)
);
GO


/* -----------------------------------------------------------------------------
   INVENTORY SNAPSHOTS - RAW
   -----------------------------------------------------------------------------
   Description:
   Captures point-in-time inventory levels by product and store.

   Grain:
   One row per product per store per snapshot date.

   Business Use:
   Supports stock availability analysis, shrink detection,
   replenishment planning, and inventory valuation.
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
    safety_stock        VARCHAR(255)
);
GO


/* -----------------------------------------------------------------------------
   RETURNS - RAW
   -----------------------------------------------------------------------------
   Description:
   Captures product return events across sales channels.

   Grain:
   One row per return event.

   Business Use:
   Enables return rate analysis, refund tracking,
   and identification of product or operational issues.
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
    return_channel    VARCHAR(255)
);
GO


/* -----------------------------------------------------------------------------
   PRODUCTS - RAW
   -----------------------------------------------------------------------------
   Description:
   Master list of products as defined by upstream systems.

   Grain:
   One row per product.

   Business Use:
   Serves as the foundation for product performance, pricing,
   margin analysis, and category reporting.
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
    status         VARCHAR(255)
);
GO


/* -----------------------------------------------------------------------------
   STORES - RAW
   -----------------------------------------------------------------------------
   Description:
   Master list of physical store locations.

   Grain:
   One row per store.

   Business Use:
   Enables regional performance analysis, store comparisons,
   and operational reporting.
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
    manager_id     VARCHAR(255)
);
GO
