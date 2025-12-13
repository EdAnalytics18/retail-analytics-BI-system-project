/* =============================================================================
   STAGING LAYER - DATA CLEANING & STANDARDIZATION
   -----------------------------------------------------------------------------
   Purpose:
   - Convert raw VARCHAR data into strongly typed, analytics-ready structures
   - Standardize categorical values across source systems
   - Capture data quality issues without data loss
   -----------------------------------------------------------------------------
   Key Techniques:
   - TRY_CONVERT for safe type casting
   - CASE-based normalization
   - Calculated reconciliation and validation fields
   - data_quality_flag for observability and auditing
   - Window functions for deduplication
============================================================================= */


/* -----------------------------------------------------------------------------
   POS TRANSACTIONS - CLEAN
   Grain: One row per transaction_id
   Key Business Rules:
   - Monetary values must be non-negative
   - Payment methods are standardized
   - Invalid or missing dates are flagged (not dropped)
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.pos_transactions_clean;
GO

CREATE TABLE staging.pos_transactions_clean (
    transaction_id        VARCHAR(100),
    store_id              INT,
    transaction_timestamp DATETIME,
    cashier_id            VARCHAR(100),
    customer_id           VARCHAR(100),
    payment_method        VARCHAR(50),
    total_amount          DECIMAL(12,2),
    discount_amount       DECIMAL(12,2),
    tax_amount            DECIMAL(12,2),
    data_quality_flag     VARCHAR(255),
    load_timestamp        DATETIME,
    source_file           VARCHAR(255)
);
GO

INSERT INTO staging.pos_transactions_clean
SELECT
    LTRIM(RTRIM(r.transaction_id)),
    TRY_CONVERT(INT, r.store_id),
    TRY_CONVERT(DATETIME, r.transaction_timestamp),
    LTRIM(RTRIM(r.cashier_id)),
    NULLIF(LTRIM(RTRIM(r.customer_id)), ''),
    CASE
        WHEN UPPER(r.payment_method) LIKE '%CASH%'   THEN 'CASH'
        WHEN UPPER(r.payment_method) LIKE '%DEBIT%'  THEN 'DEBIT'
        WHEN UPPER(r.payment_method) LIKE '%CREDIT%' THEN 'CREDIT_CARD'
        WHEN UPPER(r.payment_method) LIKE '%APPLE%'  THEN 'APPLE_PAY'
        WHEN UPPER(r.payment_method) LIKE '%GOOGLE%' THEN 'GOOGLE_PAY'
        ELSE UPPER(LTRIM(RTRIM(r.payment_method)))
    END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.total_amount) >= 0
         THEN TRY_CONVERT(DECIMAL(12,2), r.total_amount) END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.discount_amount) >= 0
         THEN TRY_CONVERT(DECIMAL(12,2), r.discount_amount) ELSE 0 END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.tax_amount) >= 0
         THEN TRY_CONVERT(DECIMAL(12,2), r.tax_amount) ELSE 0 END,
    CONCAT(
        CASE WHEN TRY_CONVERT(DATETIME, r.transaction_timestamp) IS NULL THEN '[BAD_DATE];' ELSE '' END,
        CASE WHEN TRY_CONVERT(INT, r.store_id) IS NULL THEN '[BAD_STORE];' ELSE '' END,
        CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.total_amount) IS NULL THEN '[BAD_TOTAL];' ELSE '' END
    ),
    r.load_timestamp,
    r.source_file
FROM staging.pos_transactions_raw r;
GO

;WITH d AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY transaction_id
        ORDER BY load_timestamp DESC
    ) rn
    FROM staging.pos_transactions_clean
)
DELETE FROM d WHERE rn > 1;
GO


/* -----------------------------------------------------------------------------
   POS ORDER ITEMS - CLEAN
   Grain: One row per product per transaction
   Key Business Rules:
   - Quantities and prices must be non-negative
   - Line totals are validated against quantity * unit price
   - Mismatches are flagged for investigation
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.pos_items_clean;
GO

CREATE TABLE staging.pos_items_clean (
    transaction_id         VARCHAR(100),
    product_id             INT,
    quantity               INT,
    unit_price             DECIMAL(12,2),
    line_total             DECIMAL(12,2),
    calculated_line_total  DECIMAL(12,2),
    data_quality_flag      VARCHAR(255),
    load_timestamp         DATETIME,
    source_file            VARCHAR(255)
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
        COALESCE(TRY_CONVERT(INT, r.quantity), 0)
        * COALESCE(TRY_CONVERT(DECIMAL(12,2), r.unit_price), 0), 2
    ),
    CONCAT(
        CASE WHEN TRY_CONVERT(INT, r.quantity) IS NULL THEN '[BAD_QTY];' ELSE '' END,
        CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.unit_price) IS NULL THEN '[BAD_PRICE];' ELSE '' END,
        CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.line_total) IS NULL THEN '[BAD_LINE_TOTAL];' ELSE '' END,
        CASE WHEN ABS(
            TRY_CONVERT(DECIMAL(12,2), r.line_total)
            - (TRY_CONVERT(INT, r.quantity) * TRY_CONVERT(DECIMAL(12,2), r.unit_price))
        ) > 0.05 THEN '[LINE_TOTAL_MISMATCH];' ELSE '' END
    ),
    r.load_timestamp,
    r.source_file
FROM staging.pos_items_raw r;
GO

;WITH deduplicate AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY transaction_id, product_id
               ORDER BY load_timestamp DESC
           ) AS rn
    FROM staging.pos_items_clean
)
DELETE FROM deduplicate
WHERE rn > 1;
GO


/* -----------------------------------------------------------------------------
   E-COMMERCE ORDERS - CLEAN
   Grain: One row per e-commerce order
   Key Business Rules:
   - Order statuses and channels standardized
   - Net revenue derived from totals, discounts, and shipping
   - Invalid financial records are flagged
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.ecom_orders_clean;
GO

CREATE TABLE staging.ecom_orders_clean (
    order_id          VARCHAR(100),
    customer_id       VARCHAR(100),
    order_timestamp   DATETIME,
    order_status      VARCHAR(50),
    channel           VARCHAR(50),
    shipping_cost     DECIMAL(12,2),
    total_amount      DECIMAL(12,2),
    discount_amount   DECIMAL(12,2),
    net_revenue       DECIMAL(12,2),
    device_type       VARCHAR(50),
    traffic_source    VARCHAR(255),
    data_quality_flag VARCHAR(255),
    load_timestamp    DATETIME,
    source_file       VARCHAR(255)
);
GO

INSERT INTO staging.ecom_orders_clean
SELECT
    LTRIM(RTRIM(r.order_id)),
    NULLIF(LTRIM(RTRIM(r.customer_id)), ''),
    TRY_CONVERT(DATETIME, r.order_timestamp),
    CASE
        WHEN UPPER(r.order_status) LIKE '%COMPLETE%' THEN 'COMPLETED'
        WHEN UPPER(r.order_status) LIKE '%CANCEL%'   THEN 'CANCELLED'
        ELSE UPPER(LTRIM(RTRIM(r.order_status)))
    END,
    CASE
        WHEN UPPER(r.channel) LIKE '%APP%' THEN 'MOBILE_APP'
        WHEN UPPER(r.channel) LIKE '%WEB%' THEN 'WEB'
        ELSE UPPER(LTRIM(RTRIM(r.channel)))
    END,
    COALESCE(TRY_CONVERT(DECIMAL(12,2), r.shipping_cost), 0),
    TRY_CONVERT(DECIMAL(12,2), r.total_amount),
    COALESCE(TRY_CONVERT(DECIMAL(12,2), r.discount_amount), 0),
    ROUND(
        COALESCE(TRY_CONVERT(DECIMAL(12,2), r.total_amount), 0)
        - COALESCE(TRY_CONVERT(DECIMAL(12,2), r.discount_amount), 0)
        + COALESCE(TRY_CONVERT(DECIMAL(12,2), r.shipping_cost), 0), 2
    ),
    UPPER(LTRIM(RTRIM(r.device_type))),
    NULLIF(LTRIM(RTRIM(r.traffic_source)), ''),
    CONCAT(
        CASE WHEN TRY_CONVERT(DATETIME, r.order_timestamp) IS NULL THEN '[BAD_DATE];' ELSE '' END,
        CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.total_amount) IS NULL THEN '[BAD_TOTAL];' ELSE '' END
    ),
    r.load_timestamp,
    r.source_file
FROM staging.ecom_orders_raw r;
GO

;WITH deduplicate AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY order_id
               ORDER BY load_timestamp DESC
           ) AS rn
    FROM staging.ecom_orders_clean
)
DELETE FROM deduplicate
WHERE rn > 1;
GO


/* -----------------------------------------------------------------------------
   E-COMMERCE ORDER ITEMS - CLEAN
   Grain: One row per product per e-commerce order
   Key Business Rules:
   - Quantity and pricing validation
   - Line totals recalculated and reconciled
   - Data inconsistencies flagged
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.ecom_items_clean;
GO

CREATE TABLE staging.ecom_items_clean (
    order_item_id          INT,
    order_id               VARCHAR(100),
    product_id             INT,
    quantity               INT,
    unit_price             DECIMAL(12,2),
    line_total             DECIMAL(12,2),
    calculated_line_total  DECIMAL(12,2),
    data_quality_flag      VARCHAR(255),
    load_timestamp         DATETIME,
    source_file            VARCHAR(255)
);
GO

INSERT INTO staging.ecom_items_clean
SELECT
    TRY_CONVERT(INT, r.order_item_id),
    LTRIM(RTRIM(r.order_id)),
    TRY_CONVERT(INT, r.product_id),
    CASE WHEN TRY_CONVERT(INT, r.quantity) >= 0 THEN TRY_CONVERT(INT, r.quantity) END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.unit_price) >= 0 THEN TRY_CONVERT(DECIMAL(12,2), r.unit_price) END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.line_total) >= 0 THEN TRY_CONVERT(DECIMAL(12,2), r.line_total) END,
    ROUND(
        COALESCE(TRY_CONVERT(INT, r.quantity), 0)
        * COALESCE(TRY_CONVERT(DECIMAL(12,2), r.unit_price), 0),
        2
    ),
    CONCAT(
        CASE WHEN TRY_CONVERT(INT, r.quantity) IS NULL THEN '[BAD_QTY];' ELSE '' END,
        CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.unit_price) IS NULL THEN '[BAD_PRICE];' ELSE '' END,
        CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.line_total) IS NULL THEN '[BAD_LINE_TOTAL];' ELSE '' END,
        CASE WHEN ABS(
            TRY_CONVERT(DECIMAL(12,2), r.line_total)
            - (TRY_CONVERT(INT, r.quantity) * TRY_CONVERT(DECIMAL(12,2), r.unit_price))
        ) > 0.05 THEN '[LINE_TOTAL_MISMATCH];' ELSE '' END
    ),
    r.load_timestamp,
    r.source_file
FROM staging.ecom_items_raw r;
GO

;WITH d AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY order_item_id
        ORDER BY load_timestamp DESC
    ) rn
    FROM staging.ecom_items_clean
)
DELETE FROM d WHERE rn > 1;
GO


/* -----------------------------------------------------------------------------
   INVENTORY SNAPSHOTS - CLEAN
   Grain: One row per product per store per snapshot date
   Key Business Rules:
   - Inventory values must be non-negative
   - Inventory deltas are calculated for validation
   - Safety stock breaches are flagged
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.inventory_snapshots_clean;
GO

CREATE TABLE staging.inventory_snapshots_clean (
    snapshot_date              DATE,
    store_id                   INT,
    product_id                 INT,
    beginning_inventory        INT,
    ending_inventory           INT,
    inventory_value            DECIMAL(14,2),
    safety_stock               INT,
    stock_status               VARCHAR(50),
    calculated_inventory_delta INT,
    data_quality_flag          VARCHAR(255),
    load_timestamp             DATETIME,
    source_file                VARCHAR(255)
);
GO

INSERT INTO staging.inventory_snapshots_clean
SELECT
    TRY_CONVERT(DATE, r.snapshot_date),
    TRY_CONVERT(INT, r.store_id),
    TRY_CONVERT(INT, r.product_id),
    CASE WHEN TRY_CONVERT(INT, r.beginning_inventory) >= 0 THEN TRY_CONVERT(INT, r.beginning_inventory) END,
    CASE WHEN TRY_CONVERT(INT, r.ending_inventory) >= 0 THEN TRY_CONVERT(INT, r.ending_inventory) END,
    CASE WHEN TRY_CONVERT(DECIMAL(14,2), r.inventory_value) >= 0 THEN TRY_CONVERT(DECIMAL(14,2), r.inventory_value) END,
    CASE WHEN TRY_CONVERT(INT, r.safety_stock) >= 0 THEN TRY_CONVERT(INT, r.safety_stock) END,
    CASE
        WHEN UPPER(r.stock_status) LIKE '%OUT%'  THEN 'OUT_OF_STOCK'
        WHEN UPPER(r.stock_status) LIKE '%LOW%'  THEN 'LOW_STOCK'
        WHEN UPPER(r.stock_status) LIKE '%IN%'   THEN 'IN_STOCK'
        WHEN UPPER(r.stock_status) LIKE '%BACK%' THEN 'BACKORDER'
        ELSE UPPER(LTRIM(RTRIM(r.stock_status)))
    END,
    TRY_CONVERT(INT, r.ending_inventory)
      - TRY_CONVERT(INT, r.beginning_inventory),
    CONCAT(
        CASE WHEN TRY_CONVERT(DATE, r.snapshot_date) IS NULL THEN '[BAD_DATE];' ELSE '' END,
        CASE WHEN TRY_CONVERT(INT, r.store_id) IS NULL THEN '[BAD_STORE];' ELSE '' END,
        CASE WHEN TRY_CONVERT(INT, r.ending_inventory)
               < TRY_CONVERT(INT, r.safety_stock)
             THEN '[BELOW_SAFETY_STOCK];' ELSE '' END
    ),
    r.load_timestamp,
    r.source_file
FROM staging.inventory_snapshots_raw r;
GO

;WITH d AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY snapshot_date, store_id, product_id
        ORDER BY load_timestamp DESC
    ) rn
    FROM staging.inventory_snapshots_clean
)
DELETE FROM d WHERE rn > 1;
GO


/* -----------------------------------------------------------------------------
   RETURNS - CLEAN
   Grain: One row per return event
   Key Business Rules:
   - Return quantities must be positive
   - Refund amounts validated
   - Return channels and reasons standardized
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.returns_clean;
GO

CREATE TABLE staging.returns_clean (
    return_id          VARCHAR(100),
    transaction_id     VARCHAR(100),
    product_id         INT,
    return_date        DATE,
    return_reason      VARCHAR(100),
    refund_amount      DECIMAL(12,2),
    quantity_returned  INT,
    return_channel     VARCHAR(50),
    data_quality_flag  VARCHAR(255),
    load_timestamp     DATETIME,
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
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.refund_amount) >= 0 THEN TRY_CONVERT(DECIMAL(12,2), r.refund_amount) END,
    CASE WHEN TRY_CONVERT(INT, r.quantity_returned) > 0 THEN TRY_CONVERT(INT, r.quantity_returned) END,
    CASE
        WHEN UPPER(r.return_channel) LIKE '%STORE%'  THEN 'IN_STORE'
        WHEN UPPER(r.return_channel) LIKE '%ONLINE%' THEN 'ONLINE'
        ELSE UPPER(LTRIM(RTRIM(r.return_channel)))
    END,
    CONCAT(
        CASE WHEN TRY_CONVERT(DATE, r.return_date) IS NULL THEN '[BAD_RETURN_DATE];' ELSE '' END,
        CASE WHEN TRY_CONVERT(INT, r.quantity_returned) <= 0 THEN '[BAD_QTY];' ELSE '' END,
        CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.refund_amount) IS NULL THEN '[BAD_REFUND];' ELSE '' END
    ),
    r.load_timestamp,
    r.source_file
FROM staging.returns_raw r;
GO

;WITH d AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY return_id
        ORDER BY load_timestamp DESC
    ) rn
    FROM staging.returns_clean
)
DELETE FROM d WHERE rn > 1;
GO


/* -----------------------------------------------------------------------------
   PRODUCTS - CLEAN
   Grain: One row per product
   Key Business Rules:
   - Cost and price must be non-negative
   - Margins calculated and validated
   - Product lifecycle attributes standardized
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.products_clean;
GO

CREATE TABLE staging.products_clean (
    product_id        INT,
    sku               VARCHAR(100),
    product_name      VARCHAR(255),
    category          VARCHAR(50),
    subcategory       VARCHAR(50),
    brand             VARCHAR(50),
    cost              DECIMAL(12,2),
    price             DECIMAL(12,2),
    margin            DECIMAL(12,2),
    season            VARCHAR(50),
    launch_date       DATE,
    status            VARCHAR(50),
    data_quality_flag VARCHAR(255),
    load_timestamp    DATETIME,
    source_file       VARCHAR(255)
);
GO

INSERT INTO staging.products_clean
SELECT
    TRY_CONVERT(INT, r.product_id),
    LTRIM(RTRIM(r.sku)),
    LTRIM(RTRIM(r.product_name)),
    UPPER(LTRIM(RTRIM(r.category))),
    UPPER(LTRIM(RTRIM(r.subcategory))),
    UPPER(LTRIM(RTRIM(r.brand))),
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.cost) >= 0 THEN TRY_CONVERT(DECIMAL(12,2), r.cost) END,
    CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.price) >= 0 THEN TRY_CONVERT(DECIMAL(12,2), r.price) END,
    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), r.price) IS NOT NULL
         AND TRY_CONVERT(DECIMAL(12,2), r.cost)  IS NOT NULL
        THEN TRY_CONVERT(DECIMAL(12,2), r.price)
           - TRY_CONVERT(DECIMAL(12,2), r.cost)
    END,
    CASE
        WHEN UPPER(r.season) LIKE '%SPRING%' THEN 'SPRING'
        WHEN UPPER(r.season) LIKE '%SUMMER%' THEN 'SUMMER'
        WHEN UPPER(r.season) LIKE '%FALL%'   THEN 'FALL'
        WHEN UPPER(r.season) LIKE '%WINTER%' THEN 'WINTER'
        ELSE UPPER(LTRIM(RTRIM(r.season)))
    END,
    TRY_CONVERT(DATE, r.launch_date),
    CASE
        WHEN UPPER(r.status) LIKE '%ACTIVE%'   THEN 'ACTIVE'
        WHEN UPPER(r.status) LIKE '%DISCONT%'  THEN 'DISCONTINUED'
        ELSE UPPER(LTRIM(RTRIM(r.status)))
    END,
    CONCAT(
        CASE WHEN TRY_CONVERT(DATE, r.launch_date) IS NULL THEN '[BAD_LAUNCH_DATE];' ELSE '' END,
        CASE WHEN TRY_CONVERT(DECIMAL(12,2), r.price) < TRY_CONVERT(DECIMAL(12,2), r.cost)
             THEN '[NEGATIVE_MARGIN];' ELSE '' END
    ),
    r.load_timestamp,
    r.source_file
FROM staging.products_raw r;
GO

;WITH d AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY product_id
        ORDER BY load_timestamp DESC
    ) rn
    FROM staging.products_clean
)
DELETE FROM d WHERE rn > 1;
GO


/* -----------------------------------------------------------------------------
   STORES - CLEAN
   Grain: One row per store
   Key Business Rules:
   - Store identifiers must be valid
   - Regions and store types standardized
   - Opening dates validated
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS staging.stores_clean;
GO

CREATE TABLE staging.stores_clean (
    store_id          INT,
    store_name        VARCHAR(255),
    store_type        VARCHAR(50),
    region            VARCHAR(50),
    address           VARCHAR(255),
    opening_date      DATE,
    manager_id        VARCHAR(100),
    data_quality_flag VARCHAR(255),
    load_timestamp    DATETIME,
    source_file       VARCHAR(255)
);
GO

INSERT INTO staging.stores_clean
SELECT
    TRY_CONVERT(INT, r.store_id),
    LTRIM(RTRIM(r.store_name)),
    CASE
        WHEN UPPER(r.store_type) LIKE '%FLAGSHIP%' THEN 'FLAGSHIP'
        WHEN UPPER(r.store_type) LIKE '%OUTLET%'   THEN 'OUTLET'
        ELSE UPPER(LTRIM(RTRIM(r.store_type)))
    END,
    CASE
        WHEN UPPER(r.region) LIKE '%WEST%' THEN 'WEST'
        WHEN UPPER(r.region) LIKE '%EAST%' THEN 'EAST'
        ELSE UPPER(LTRIM(RTRIM(r.region)))
    END,
    LTRIM(RTRIM(r.address)),
    TRY_CONVERT(DATE, r.opening_date),
    NULLIF(LTRIM(RTRIM(r.manager_id)), ''),
    CONCAT(
        CASE WHEN TRY_CONVERT(INT, r.store_id) IS NULL THEN '[BAD_STORE_ID];' ELSE '' END,
        CASE WHEN TRY_CONVERT(DATE, r.opening_date) IS NULL THEN '[BAD_OPENING_DATE];' ELSE '' END
    ),
    r.load_timestamp,
    r.source_file
FROM staging.stores_raw r;
GO

;WITH d AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY store_id
        ORDER BY load_timestamp DESC
    ) rn
    FROM staging.stores_clean
)
DELETE FROM d WHERE rn > 1;
GO
