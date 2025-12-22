/* =============================================================================
   STAGING LAYER - DATA CLEANING & STANDARDIZATION (SILVER PREP)
   -----------------------------------------------------------------------------
   Executive Summary:
   This script transforms raw source data into clean, standardized,
   analytics-ready structures without discarding problematic records.

   Instead of silently fixing or deleting bad data, this layer:
   - Converts fields to correct data types
   - Standardizes business-critical categories
   - Explicitly flags data quality issues for visibility and auditability

   This approach ensures downstream metrics (revenue, margin, inventory,
   returns) are accurate, explainable, and trusted by the business.

   -----------------------------------------------------------------------------
   Business Value:
   - Protects executive dashboards from hidden data issues
   - Preserves problematic records for investigation and root-cause analysis
   - Enables analysts to filter or exclude bad data intentionally
   - Creates a transparent quality contract before data enters the core model

   -----------------------------------------------------------------------------
   Design Philosophy:
   - Fail-safe conversions using TRY_CONVERT (no pipeline breaks)
   - Business-rule-driven standardization (not ad hoc fixes)
   - Observability via explicit data quality flags
   - Deduplication handled deterministically using window functions

   This layer is intentionally verbose, clarity and trust take priority over
   minimalism.
============================================================================= */


/* -----------------------------------------------------------------------------
   POS TRANSACTIONS - CLEAN & STANDARDIZED
   -----------------------------------------------------------------------------
   Description:
   Represents cleaned point-of-sale transaction headers, standardized for
   analytical consistency across stores and reporting periods.

   Grain:
   One row per unique transaction_id.

   Key Business Rules:
   - All monetary values must be non-negative
   - Payment methods are normalized to a controlled vocabulary
   - Invalid dates or store references are flagged, not removed
   - Duplicate transactions are retained but explicitly marked

   Why This Matters:
   - Prevents overstated or understated revenue
   - Ensures consistent payment method reporting
   - Allows Finance and Ops to trace anomalies back to source data
----------------------------------------------------------------------------- */

DROP TABLE IF EXISTS staging.pos_transactions_clean;
GO

CREATE TABLE staging.pos_transactions_clean (
    transaction_id        VARCHAR(100) NOT NULL,
    store_id              INT,
    transaction_timestamp DATETIME2,
    cashier_id            VARCHAR(100),
    customer_id           VARCHAR(100),
    payment_method        VARCHAR(50),
    total_amount          DECIMAL(12,2),
    discount_amount       DECIMAL(12,2),
    tax_amount            DECIMAL(12,2),

    is_bad_date            BIT,
    is_bad_store           BIT,
    is_bad_total           BIT,
    is_duplicate           BIT DEFAULT 0,

    load_timestamp        DATETIME2,
    source_file           VARCHAR(255)
);
GO

INSERT INTO staging.pos_transactions_clean
SELECT
    LTRIM(RTRIM(r.transaction_id)),
    TRY_CONVERT(INT, r.store_id),
    TRY_CONVERT(DATETIME2, r.transaction_timestamp),
    LTRIM(RTRIM(r.cashier_id)),
    NULLIF(LTRIM(RTRIM(r.customer_id)), ''),
    CASE
        WHEN UPPER(r.payment_method) LIKE '%CASH%'   THEN 'CASH'
        WHEN UPPER(r.payment_method) LIKE '%DEBIT%'  THEN 'DEBIT'
        WHEN UPPER(r.payment_method) LIKE '%CREDIT%' THEN 'CREDIT_CARD'
        ELSE UPPER(LTRIM(RTRIM(r.payment_method)))
    END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.total_amount) >= 0 THEN TRY_CONVERT(DECIMAL(12,2), r.total_amount) END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.discount_amount) >= 0 THEN TRY_CONVERT(DECIMAL(12,2), r.discount_amount) END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.tax_amount) >= 0 THEN TRY_CONVERT(DECIMAL(12,2), r.tax_amount) END,
    CASE WHEN TRY_CONVERT(DATETIME2, r.transaction_timestamp) IS NULL THEN 1 ELSE 0 END,
    CASE WHEN TRY_CONVERT(INT, r.store_id) IS NULL THEN 1 ELSE 0 END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.total_amount) IS NULL THEN 1 ELSE 0 END,
    0,
    r.load_timestamp,
    r.source_file
FROM staging.pos_transactions_raw r;
GO

WITH dups AS (
    SELECT transaction_id,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY load_timestamp DESC) rn
    FROM staging.pos_transactions_clean
)
UPDATE c
SET is_duplicate = 1
FROM staging.pos_transactions_clean c
JOIN dups d ON c.transaction_id = d.transaction_id
WHERE d.rn > 1;
GO


/* -----------------------------------------------------------------------------
   POS ORDER ITEMS - CLEAN & RECONCILED
   -----------------------------------------------------------------------------
   Description:
   Represents individual products sold within each POS transaction, with
   financial reconciliation logic applied.

   Grain:
   One row per product per transaction.

   Key Business Rules:
   - Quantities and prices must be non-negative
   - Line totals are recalculated using quantity x unit price
   - Discrepancies between source and calculated totals are flagged

   Why This Matters:
   - Detects pricing errors at the transaction level
   - Prevents margin distortion in product-level reporting
   - Supports auditability for financial reviews
----------------------------------------------------------------------------- */

DROP TABLE IF EXISTS staging.pos_items_clean;
GO

CREATE TABLE staging.pos_items_clean (
    transaction_id        VARCHAR(100) NOT NULL,
    product_id            INT NOT NULL,
    quantity              INT,
    unit_price            DECIMAL(12,2),
    line_total            DECIMAL(12,2),
    calculated_line_total DECIMAL(12,2),

    is_bad_qty             BIT,
    is_bad_price           BIT,
    is_bad_line_total      BIT,
    is_line_total_mismatch BIT,
    is_duplicate           BIT DEFAULT 0,

    load_timestamp        DATETIME2,
    source_file           VARCHAR(255)
);
GO

INSERT INTO staging.pos_items_clean
SELECT
    LTRIM(RTRIM(r.transaction_id)),
    TRY_CONVERT(INT, r.product_id),
    CASE WHEN TRY_CONVERT(INT, r.quantity) >= 0 THEN TRY_CONVERT(INT, r.quantity) END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.unit_price) >= 0 THEN TRY_CONVERT(DECIMAL(12,2), r.unit_price) END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.line_total) >= 0 THEN TRY_CONVERT(DECIMAL(12,2), r.line_total) END,
    ROUND(
        COALESCE(TRY_CONVERT(INT, r.quantity),0)
        * COALESCE(TRY_CONVERT(DECIMAL(12,2), r.unit_price),0), 2
    ),
    CASE WHEN TRY_CONVERT(INT, r.quantity) IS NULL THEN 1 ELSE 0 END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.unit_price) IS NULL THEN 1 ELSE 0 END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.line_total) IS NULL THEN 1 ELSE 0 END,
    CASE WHEN ABS(
        TRY_CONVERT(DECIMAL(12,2), r.line_total)
      - (TRY_CONVERT(INT, r.quantity) * TRY_CONVERT(DECIMAL(12,2), r.unit_price))
    ) > 0.05 THEN 1 ELSE 0 END,
    0,
    r.load_timestamp,
    r.source_file
FROM staging.pos_items_raw r;
GO

WITH dups AS (
    SELECT transaction_id, product_id,
           ROW_NUMBER() OVER (
             PARTITION BY transaction_id, product_id
             ORDER BY load_timestamp DESC
           ) rn
    FROM staging.pos_items_clean
)
UPDATE c
SET is_duplicate = 1
FROM staging.pos_items_clean c
JOIN dups d
  ON c.transaction_id = d.transaction_id
 AND c.product_id     = d.product_id
WHERE d.rn > 1;
GO


/* -----------------------------------------------------------------------------
   E-COMMERCE ORDERS - CLEAN & DERIVED METRICS
   -----------------------------------------------------------------------------
   Description:
   Represents cleaned online orders with standardized attributes and
   derived revenue fields.

   Grain:
   One row per e-commerce order.

   Key Business Rules:
   - Order statuses, channels, and devices standardized
   - Net revenue derived explicitly from totals, discounts, and shipping
   - Invalid dates or totals are flagged for visibility

   Why This Matters:
   - Ensures digital revenue is calculated consistently
   - Supports accurate channel and campaign attribution
   - Prevents silent errors in growth and conversion reporting
----------------------------------------------------------------------------- */

DROP TABLE IF EXISTS staging.ecom_orders_clean;
GO

CREATE TABLE staging.ecom_orders_clean (
    order_id          VARCHAR(100) NOT NULL,
    customer_id       VARCHAR(100),
    order_timestamp   DATETIME2,
    order_status      VARCHAR(50),
    channel           VARCHAR(50),
    device_type       VARCHAR(50),
    traffic_source    VARCHAR(50),
    shipping_cost     DECIMAL(12,2),
    total_amount      DECIMAL(12,2),
    discount_amount   DECIMAL(12,2),
    net_revenue       DECIMAL(12,2),

    is_bad_date       BIT,
    is_bad_total      BIT,
    is_duplicate      BIT DEFAULT 0,

    load_timestamp    DATETIME2,
    source_file       VARCHAR(255)
);
GO

INSERT INTO staging.ecom_orders_clean
SELECT
    LTRIM(RTRIM(r.order_id)),
    NULLIF(LTRIM(RTRIM(r.customer_id)), ''),
    TRY_CONVERT(DATETIME2, r.order_timestamp),
    UPPER(LTRIM(RTRIM(r.order_status))),
    UPPER(LTRIM(RTRIM(r.channel))),
    UPPER(LTRIM(RTRIM(r.device_type))),  
    NULLIF(LTRIM(RTRIM(r.traffic_source)), ''), 
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.shipping_cost) >= 0 THEN TRY_CONVERT(DECIMAL(12,2), r.shipping_cost) END,
    TRY_CONVERT(DECIMAL(12,2), r.total_amount),
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.discount_amount) >= 0 THEN TRY_CONVERT(DECIMAL(12,2), r.discount_amount) END,
    ROUND(
        COALESCE(TRY_CONVERT(DECIMAL(12,2), r.total_amount),0)
      - COALESCE(TRY_CONVERT(DECIMAL(12,2), r.discount_amount),0)
      + COALESCE(TRY_CONVERT(DECIMAL(12,2), r.shipping_cost),0), 2
    ),
    CASE WHEN TRY_CONVERT(DATETIME2, r.order_timestamp) IS NULL THEN 1 ELSE 0 END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.total_amount) IS NULL THEN 1 ELSE 0 END,
    0,
    r.load_timestamp,
    r.source_file
FROM staging.ecom_orders_raw r;
GO

WITH dups AS (
    SELECT order_id,
           ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY load_timestamp DESC) rn
    FROM staging.ecom_orders_clean
)
UPDATE c
SET is_duplicate = 1
FROM staging.ecom_orders_clean c
JOIN dups d ON c.order_id = d.order_id
WHERE d.rn > 1;
GO


/* -----------------------------------------------------------------------------
   E-COMMERCE ORDER ITEMS - CLEAN & FINANCIALLY RECONCILED
   -----------------------------------------------------------------------------
   Description:
   Represents individual products purchased within each e-commerce order.
   This table applies pricing validation and reconciliation logic to ensure
   product-level revenue is accurate and auditable.

   Grain:
   One row per product per e-commerce order.

   Key Business Rules:
   - Quantities and unit prices must be valid and non-negative
   - Line totals are recalculated using quantity x unit price
   - Differences between source and calculated totals are explicitly flagged
   - Duplicate order items are retained but clearly marked

   Why This Matters:
   - Prevents revenue distortion at the SKU level
   - Protects margin, AOV, and conversion reporting
   - Enables Finance and Analytics teams to trust product-level metrics
   - Preserves discrepancies for investigation instead of hiding them
----------------------------------------------------------------------------- */

DROP TABLE IF EXISTS staging.ecom_items_clean;
GO

CREATE TABLE staging.ecom_items_clean (
    order_item_id          INT NOT NULL,
    order_id               VARCHAR(100),
    product_id             INT,
    quantity               INT,
    unit_price             DECIMAL(12,2),
    line_total             DECIMAL(12,2),
    calculated_line_total  DECIMAL(12,2),

    is_bad_qty              BIT,
    is_bad_price            BIT,
    is_bad_line_total       BIT,
    is_line_total_mismatch  BIT,
    is_duplicate            BIT DEFAULT 0,

    load_timestamp          DATETIME2,
    source_file             VARCHAR(255)
);
GO

INSERT INTO staging.ecom_items_clean
SELECT
    TRY_CONVERT(INT, r.order_item_id),
    LTRIM(RTRIM(r.order_id)),
    TRY_CONVERT(INT, r.product_id),

    CASE WHEN TRY_CONVERT(INT, r.quantity) >= 0
         THEN TRY_CONVERT(INT, r.quantity) END,

    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.unit_price) >= 0
         THEN TRY_CONVERT(DECIMAL(12,2), r.unit_price) END,

    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.line_total) >= 0
         THEN TRY_CONVERT(DECIMAL(12,2), r.line_total) END,

    ROUND(
        COALESCE(TRY_CONVERT(INT, r.quantity),0)
      * COALESCE(TRY_CONVERT(DECIMAL(12,2), r.unit_price),0), 2
    ),

    CASE WHEN TRY_CONVERT(INT, r.quantity) IS NULL THEN 1 ELSE 0 END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.unit_price) IS NULL THEN 1 ELSE 0 END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.line_total) IS NULL THEN 1 ELSE 0 END,
    CASE WHEN ABS(
        TRY_CONVERT(DECIMAL(12,2), r.line_total)
      - (TRY_CONVERT(INT, r.quantity) * TRY_CONVERT(DECIMAL(12,2), r.unit_price))
    ) > 0.05 THEN 1 ELSE 0 END,

    0,
    r.load_timestamp,
    r.source_file
FROM staging.ecom_items_raw r;
GO

WITH dups AS (
    SELECT order_item_id,
           ROW_NUMBER() OVER (
               PARTITION BY order_item_id
               ORDER BY load_timestamp DESC
           ) rn
    FROM staging.ecom_items_clean
)
UPDATE c
SET is_duplicate = 1
FROM staging.ecom_items_clean c
JOIN dups d ON c.order_item_id = d.order_item_id
WHERE d.rn > 1;
GO


/* -----------------------------------------------------------------------------
   INVENTORY SNAPSHOTS - CLEAN & VALIDATED
   -----------------------------------------------------------------------------
   Description:
   Represents point-in-time inventory levels by store and product.

   Grain:
   One row per product per store per snapshot date.

   Key Business Rules:
   - Inventory values must be non-negative
   - Inventory deltas calculated for sanity checks
   - Safety stock breaches explicitly flagged

   Why This Matters:
   - Enables proactive stock-out and replenishment analysis
   - Protects inventory valuation reporting
   - Supports operational decision-making, not just reporting
----------------------------------------------------------------------------- */

DROP TABLE IF EXISTS staging.inventory_snapshots_clean;
GO

CREATE TABLE staging.inventory_snapshots_clean (
    snapshot_date              DATE,
    store_id                   INT  NOT NULL,
    product_id                 INT  NOT NULL,
    beginning_inventory        INT,
    ending_inventory           INT,
    inventory_value            DECIMAL(14,2),
    safety_stock               INT,
    stock_status               VARCHAR(50),
    calculated_inventory_delta INT,

    is_bad_date                BIT,
    is_bad_store               BIT,
    is_below_safety_stock      BIT,
    is_duplicate               BIT DEFAULT 0,

    load_timestamp             DATETIME2,
    source_file                VARCHAR(255)
);
GO

INSERT INTO staging.inventory_snapshots_clean
SELECT
    TRY_CONVERT(DATE, r.snapshot_date),
    TRY_CONVERT(INT, r.store_id),
    TRY_CONVERT(INT, r.product_id),

    CASE WHEN TRY_CONVERT(INT, r.beginning_inventory) >= 0
         THEN TRY_CONVERT(INT, r.beginning_inventory) END,

    CASE WHEN TRY_CONVERT(INT, r.ending_inventory) >= 0
         THEN TRY_CONVERT(INT, r.ending_inventory) END,

    CASE WHEN TRY_CONVERT(DECIMAL(14,2), r.inventory_value) >= 0
         THEN TRY_CONVERT(DECIMAL(14,2), r.inventory_value) END,

    CASE WHEN TRY_CONVERT(INT, r.safety_stock) >= 0
         THEN TRY_CONVERT(INT, r.safety_stock) END,

    CASE
        WHEN UPPER(r.stock_status) LIKE '%OUT%'  THEN 'OUT_OF_STOCK'
        WHEN UPPER(r.stock_status) LIKE '%LOW%'  THEN 'LOW_STOCK'
        WHEN UPPER(r.stock_status) LIKE '%IN%'   THEN 'IN_STOCK'
        WHEN UPPER(r.stock_status) LIKE '%BACK%' THEN 'BACKORDER'
        ELSE UPPER(LTRIM(RTRIM(r.stock_status)))
    END,

    TRY_CONVERT(INT, r.ending_inventory)
      - TRY_CONVERT(INT, r.beginning_inventory),

    CASE WHEN TRY_CONVERT(DATE, r.snapshot_date) IS NULL THEN 1 ELSE 0 END,
    CASE WHEN TRY_CONVERT(INT, r.store_id) IS NULL THEN 1 ELSE 0 END,
    CASE WHEN TRY_CONVERT(INT, r.ending_inventory)
           < TRY_CONVERT(INT, r.safety_stock)
         THEN 1 ELSE 0 END,

    0,
    r.load_timestamp,
    r.source_file
FROM staging.inventory_snapshots_raw r;
GO

WITH dups AS (
    SELECT snapshot_date, store_id, product_id,
           ROW_NUMBER() OVER (
               PARTITION BY snapshot_date, store_id, product_id
               ORDER BY load_timestamp DESC
           ) rn
    FROM staging.inventory_snapshots_clean
)
UPDATE c
SET is_duplicate = 1
FROM staging.inventory_snapshots_clean c
JOIN dups d
  ON c.snapshot_date = d.snapshot_date
 AND c.store_id      = d.store_id
 AND c.product_id    = d.product_id
WHERE d.rn > 1;
GO


/* -----------------------------------------------------------------------------
   RETURNS - CLEAN & STANDARDIZED
   -----------------------------------------------------------------------------
   Description:
   Represents customer return events with normalized reasons and channels.

   Grain:
   One row per return event.

   Key Business Rules:
   - Return quantities must be positive
   - Refund amounts validated
   - Return reasons and channels standardized

   Why This Matters:
   - Enables accurate return rate and refund analysis
   - Identifies product quality or fulfillment issues
   - Supports customer experience and loss prevention insights
----------------------------------------------------------------------------- */

DROP TABLE IF EXISTS staging.returns_clean;
GO

CREATE TABLE staging.returns_clean (
    return_id          VARCHAR(100) NOT NULL,
    transaction_id     VARCHAR(100),
    product_id         INT,
    return_date        DATE,
    return_reason      VARCHAR(100),
    refund_amount      DECIMAL(12,2),
    quantity_returned  INT,
    return_channel     VARCHAR(50),

    is_bad_return_date BIT,
    is_bad_qty         BIT,
    is_bad_refund      BIT,
    is_duplicate       BIT DEFAULT 0,

    load_timestamp     DATETIME2,
    source_file        VARCHAR(255)
);
GO

INSERT INTO staging.returns_clean
SELECT
    LTRIM(RTRIM(r.return_id)),
    LTRIM(RTRIM(r.transaction_id)),
    TRY_CONVERT(INT, r.product_id),
    TRY_CONVERT(DATE, r.return_date),

    CASE
        WHEN UPPER(r.return_reason) LIKE '%DEFECT%' THEN 'DEFECTIVE'
        WHEN UPPER(r.return_reason) LIKE '%DAMAG%'  THEN 'DAMAGED'
        WHEN UPPER(r.return_reason) LIKE '%SIZE%'   THEN 'SIZE_FIT'
        ELSE UPPER(LTRIM(RTRIM(r.return_reason)))
    END,

    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.refund_amount) >= 0
         THEN TRY_CONVERT(DECIMAL(12,2), r.refund_amount) END,

    CASE WHEN TRY_CONVERT(INT, r.quantity_returned) > 0
         THEN TRY_CONVERT(INT, r.quantity_returned) END,

    CASE
        WHEN UPPER(r.return_channel) LIKE '%STORE%'  THEN 'IN_STORE'
        WHEN UPPER(r.return_channel) LIKE '%ONLINE%' THEN 'ONLINE'
        ELSE UPPER(LTRIM(RTRIM(r.return_channel)))
    END,

    CASE WHEN TRY_CONVERT(DATE, r.return_date) IS NULL THEN 1 ELSE 0 END,
    CASE WHEN TRY_CONVERT(INT, r.quantity_returned) <= 0 THEN 1 ELSE 0 END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.refund_amount) IS NULL THEN 1 ELSE 0 END,

    0,
    r.load_timestamp,
    r.source_file
FROM staging.returns_raw r;
GO

WITH dups AS (
    SELECT return_id,
           ROW_NUMBER() OVER (
               PARTITION BY return_id
               ORDER BY load_timestamp DESC
           ) rn
    FROM staging.returns_clean
)
UPDATE c
SET is_duplicate = 1
FROM staging.returns_clean c
JOIN dups d ON c.return_id = d.return_id
WHERE d.rn > 1;
GO


/* -----------------------------------------------------------------------------
   PRODUCTS - CLEAN & PROFITABILITY-READY
   -----------------------------------------------------------------------------
   Description:
   Represents standardized product master data with margin calculation.

   Grain:
   One row per product.

   Key Business Rules:
   - Cost and price must be non-negative
   - Product margin calculated explicitly
   - Negative margins flagged for investigation

   Why This Matters:
   - Prevents misleading profitability analysis
   - Surfaces pricing or cost ingestion issues early
   - Enables trusted category and brand reporting
----------------------------------------------------------------------------- */

DROP TABLE IF EXISTS staging.products_clean;
GO

CREATE TABLE staging.products_clean (
    product_id         INT NOT NULL,
    sku                VARCHAR(100),
    product_name       VARCHAR(255),
    category           VARCHAR(50),
    subcategory        VARCHAR(50), 
    brand              VARCHAR(50),
    cost               DECIMAL(12,2),
    price              DECIMAL(12,2),
    margin             DECIMAL(12,2),
    season             VARCHAR(50), 
    launch_date        DATE,
    status            VARCHAR(50),

    is_negative_margin BIT,
    is_duplicate       BIT DEFAULT 0,

    load_timestamp     DATETIME2,
    source_file        VARCHAR(255)
);
GO

INSERT INTO staging.products_clean (
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    brand,
    cost,
    price,
    margin,
    season,
    launch_date,
    status,
    is_negative_margin,
    is_duplicate,
    load_timestamp,
    source_file
)
SELECT
    TRY_CONVERT(INT, r.product_id),
    LTRIM(RTRIM(r.sku)),
    LTRIM(RTRIM(r.product_name)),
    UPPER(LTRIM(RTRIM(r.category))),
    UPPER(LTRIM(RTRIM(r.subcategory))),  
    UPPER(LTRIM(RTRIM(r.brand))),
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.cost) >= 0
         THEN TRY_CONVERT(DECIMAL(12,2), r.cost) END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.price) >= 0
         THEN TRY_CONVERT(DECIMAL(12,2), r.price) END,
    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), r.price) IS NOT NULL
         AND TRY_CONVERT(DECIMAL(12,2), r.cost)  IS NOT NULL
        THEN TRY_CONVERT(DECIMAL(12,2), r.price)
           - TRY_CONVERT(DECIMAL(12,2), r.cost)
    END,
    UPPER(LTRIM(RTRIM(r.season))),
    TRY_CONVERT(DATE, r.launch_date),
    UPPER(LTRIM(RTRIM(r.status))),
    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), r.price)
           < TRY_CONVERT(DECIMAL(12,2), r.cost)
        THEN 1 ELSE 0
    END,
    0, 
    r.load_timestamp,
    r.source_file
FROM staging.products_raw r;
GO


/* -----------------------------------------------------------------------------
   STORES - CLEAN & STANDARDIZED
   -----------------------------------------------------------------------------
   Description:
   Represents physical store locations with standardized attributes.

   Grain:
   One row per store.

   Key Business Rules:
   - Store identifiers validated
   - Regions and store types standardized
   - Duplicate records retained and flagged

   Why This Matters:
   - Enables accurate regional performance analysis
   - Prevents double-counting in store-level metrics
   - Provides consistent organizational context for reporting
----------------------------------------------------------------------------- */

DROP TABLE IF EXISTS staging.stores_clean;
GO

CREATE TABLE staging.stores_clean (
    store_id        INT NOT NULL,
    store_name      VARCHAR(255),
    store_type      VARCHAR(50),
    region          VARCHAR(50),
    address         VARCHAR(255),
    opening_date    DATE,
    manager_id      VARCHAR(100),

    is_duplicate    BIT DEFAULT 0,

    load_timestamp  DATETIME2,
    source_file     VARCHAR(255)
);
GO

INSERT INTO staging.stores_clean (
    store_id,
    store_name,
    store_type,
    region,
    address,
    opening_date,
    manager_id,
    is_duplicate,
    load_timestamp,
    source_file
)
SELECT
    TRY_CONVERT(INT, r.store_id),
    LTRIM(RTRIM(r.store_name)),
    UPPER(LTRIM(RTRIM(r.store_type))),
    UPPER(LTRIM(RTRIM(r.region))),
    LTRIM(RTRIM(r.address)),
    TRY_CONVERT(DATE, r.opening_date),
    NULLIF(LTRIM(RTRIM(r.manager_id)), ''),
    0,
    r.load_timestamp,
    r.source_file
FROM staging.stores_raw r;
GO

WITH dups AS (
    SELECT
        store_id,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY load_timestamp DESC
        ) AS rn
    FROM staging.stores_clean
)
UPDATE c
SET is_duplicate = 1
FROM staging.stores_clean c
JOIN dups d
  ON c.store_id = d.store_id
WHERE d.rn > 1;
GO
