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
    transaction_id        VARCHAR(50),
    store_id              INT,
    transaction_timestamp DATETIME,
    cashier_id            VARCHAR(50),
    customer_id           VARCHAR(50),
    payment_method        VARCHAR(50),
    total_amount          DECIMAL(12,2),
    discount_amount       DECIMAL(12,2),
    tax_amount            DECIMAL(12,2),
    data_quality_flag     VARCHAR(255)
);
GO

INSERT INTO staging.pos_transactions_clean (
    transaction_id,        
    store_id,              
    transaction_timestamp,
    cashier_id,            
    customer_id,        
    payment_method,       
    total_amount,        
    discount_amount,
    tax_amount,            
    data_quality_flag    
)
SELECT 
    LTRIM(RTRIM(transaction_id)) AS transaction_id,

    TRY_CONVERT(INT, LTRIM(RTRIM(store_id))) AS store_id,

    TRY_CONVERT(DATETIME, transaction_timestamp) AS transaction_timestamp,

    LTRIM(RTRIM(cashier_id)) AS cashier_id,

    NULLIF(LTRIM(RTRIM(customer_id)), '') AS customer_id,

    CASE
        WHEN payment_method LIKE '%CASH%'   THEN 'CASH'
        WHEN payment_method LIKE '%DEBIT%'  THEN 'DEBIT'
        WHEN payment_method LIKE '%CREDIT%' THEN 'CREDIT_CARD'
        WHEN payment_method LIKE '%APPLE%'  THEN 'APPLE_PAY'
        WHEN payment_method LIKE '%GOOGLE%' THEN 'GOOGLE_PAY'
        ELSE UPPER(LTRIM(RTRIM(payment_method)))
    END AS payment_method,

    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), total_amount) < 0
            THEN NULL
        ELSE TRY_CONVERT(DECIMAL(12,2), total_amount)
    END AS total_amount,

    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), discount_amount) < 0
            THEN 0
        ELSE COALESCE(TRY_CONVERT(DECIMAL(12,2), discount_amount), 0)
    END AS discount_amount,

    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), tax_amount) < 0
            THEN 0
        ELSE COALESCE(TRY_CONVERT(DECIMAL(12,2), tax_amount), 0)
    END AS tax_amount,

    CONCAT(
        CASE WHEN TRY_CONVERT(DATETIME, transaction_timestamp) IS NULL
             THEN '[BAD_DATE]; ' ELSE '' END,

        CASE WHEN TRY_CONVERT(INT, store_id) IS NULL
             THEN '[BAD_STORE]; ' ELSE '' END,

        CASE WHEN TRY_CONVERT(DECIMAL(12,2), total_amount) IS NULL
                  OR TRY_CONVERT(DECIMAL(12,2), total_amount) < 0
             THEN '[BAD_TOTAL]; ' ELSE '' END
    ) AS data_quality_flag
FROM staging.pos_transactions_raw;
GO

;WITH deduplicate AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id
            ORDER BY transaction_timestamp DESC
        ) AS rn
    FROM staging.pos_transactions_clean
)
DELETE FROM deduplicate
WHERE rn > 1;
GO

FROM staging.pos_transactions_clean
WHERE data_quality_flag <> '';
GO

SELECT TOP 20 *
FROM staging.pos_transactions_clean
ORDER BY transaction_timestamp;
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
    transaction_id        VARCHAR(50),
    product_id            INT,
    quantity              INT,
    unit_price            DECIMAL(12,2),
    line_total            DECIMAL(12,2),
    calculated_line_total DECIMAL(12,2),
    data_quality_flag     VARCHAR(255)
);
GO

INSERT INTO staging.pos_items_clean (
    transaction_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    calculated_line_total,
    data_quality_flag
)
SELECT 
    LTRIM(RTRIM(transaction_id)) AS transaction_id,

    TRY_CONVERT(INT, LTRIM(RTRIM(product_id))) AS product_id,

    CASE
        WHEN TRY_CONVERT(INT, quantity) < 0
            THEN NULL
        ELSE TRY_CONVERT(INT, quantity)
    END AS quantity,

    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), unit_price) < 0
            THEN NULL
        ELSE TRY_CONVERT(DECIMAL(12,2), unit_price)
    END AS unit_price,

    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), line_total) < 0
            THEN NULL
        ELSE TRY_CONVERT(DECIMAL(12,2), line_total)
    END AS line_total,

    ROUND(
        COALESCE(TRY_CONVERT(INT, quantity), 0)
        * COALESCE(TRY_CONVERT(DECIMAL(12,2), unit_price), 0),
        2
    ) AS calculated_line_total,

    CONCAT(
        CASE
            WHEN TRY_CONVERT(INT, quantity) IS NULL
                 OR TRY_CONVERT(INT, quantity) < 0
                THEN '[BAD_QTY]; ' ELSE '' END,

        CASE
            WHEN TRY_CONVERT(DECIMAL(12,2), unit_price) IS NULL
                 OR TRY_CONVERT(DECIMAL(12,2), unit_price) < 0
                THEN '[BAD_PRICE]; ' ELSE '' END,

        CASE
            WHEN TRY_CONVERT(DECIMAL(12,2), line_total) IS NULL
                THEN '[BAD_LINE_TOTAL]; ' ELSE '' END,

        CASE
            WHEN TRY_CONVERT(DECIMAL(12,2), line_total) IS NOT NULL
             AND ABS(
                    TRY_CONVERT(DECIMAL(12,2), line_total)
                    - (
                        TRY_CONVERT(INT, quantity)
                        * TRY_CONVERT(DECIMAL(12,2), unit_price)
                      )
                 ) > 0.05
                THEN '[LINE_TOTAL_MISMATCH]; ' ELSE '' END
    ) AS data_quality_flag
FROM staging.pos_items_raw;
GO

;WITH deduplicate AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY transaction_id, product_id
            ORDER BY calculated_line_total DESC
        ) AS rn
    FROM staging.pos_items_clean
)
DELETE FROM deduplicate
WHERE rn > 1;
GO

SELECT *
FROM staging.pos_items_clean
WHERE data_quality_flag <> '';
GO

SELECT TOP 20 *
FROM staging.pos_items_clean
ORDER BY transaction_id;
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
    order_id           VARCHAR(50),
    customer_id        VARCHAR(50),
    order_timestamp    DATETIME,
    order_status       VARCHAR(50),
    channel            VARCHAR(50),
    shipping_cost      DECIMAL(12,2),
    total_amount       DECIMAL(12,2),
    discount_amount    DECIMAL(12,2),
    net_revenue        DECIMAL(12,2),
    device_type        VARCHAR(50),
    traffic_source     VARCHAR(100),
    data_quality_flag  VARCHAR(255)
);
GO

INSERT INTO staging.ecom_orders_clean (
    order_id,
    customer_id,
    order_timestamp,
    order_status,
    channel,
    shipping_cost,
    total_amount,
    discount_amount,
    net_revenue,
    device_type,
    traffic_source,
    data_quality_flag
)
SELECT
    LTRIM(RTRIM(order_id)) AS order_id,

    NULLIF(LTRIM(RTRIM(customer_id)), '') AS customer_id,

    TRY_CONVERT(DATETIME, order_timestamp) AS order_timestamp,
    
    CASE
        WHEN order_status LIKE '%COMPLETE%'   THEN 'COMPLETED'
        WHEN order_status LIKE '%PAID%'       THEN 'PAID'
        WHEN order_status LIKE '%CANCEL%'     THEN 'CANCELLED'
        WHEN order_status LIKE '%RETURN%'     THEN 'RETURNED'
        WHEN order_status LIKE '%REFUND%'     THEN 'REFUNDED'
        WHEN order_status LIKE '%PENDING%'    THEN 'PENDING'
        ELSE UPPER(LTRIM(RTRIM(order_status)))
    END AS order_status,

    CASE
        WHEN channel LIKE '%WEB%'    THEN 'WEB'
        WHEN channel LIKE '%APP%'    THEN 'MOBILE_APP'
        WHEN channel LIKE '%MOBILE%' THEN 'MOBILE_WEB'
        ELSE UPPER(LTRIM(RTRIM(channel)))
    END AS channel,

    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), shipping_cost) < 0
            THEN 0
        ELSE COALESCE(TRY_CONVERT(DECIMAL(12,2), shipping_cost), 0)
    END AS shipping_cost,

    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), total_amount) < 0
            THEN NULL
        ELSE TRY_CONVERT(DECIMAL(12,2), total_amount)
    END AS total_amount,

    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), discount_amount) < 0
            THEN 0
        ELSE COALESCE(TRY_CONVERT(DECIMAL(12,2), discount_amount), 0)
    END AS discount_amount,

    ROUND(
        COALESCE(TRY_CONVERT(DECIMAL(12,2), total_amount), 0)
        - COALESCE(TRY_CONVERT(DECIMAL(12,2), discount_amount), 0)
        + COALESCE(TRY_CONVERT(DECIMAL(12,2), shipping_cost), 0),
        2
    ) AS net_revenue,

    CASE
        WHEN device_type LIKE '%MOB%'     THEN 'MOBILE'
        WHEN device_type LIKE '%DESK%'    THEN 'DESKTOP'
        WHEN device_type LIKE '%TABLET%'  THEN 'TABLET'
        ELSE UPPER(LTRIM(RTRIM(device_type)))
    END AS device_type,

    NULLIF(LTRIM(RTRIM(traffic_source)), '') AS traffic_source,

    CONCAT(
        CASE WHEN TRY_CONVERT(DATETIME, order_timestamp) IS NULL
             THEN '[BAD_DATE]; ' ELSE '' END,

        CASE WHEN TRY_CONVERT(DECIMAL(12,2), total_amount) IS NULL
             THEN '[BAD_TOTAL]; ' ELSE '' END,

        CASE WHEN order_status IS NULL OR LTRIM(RTRIM(order_status)) = ''
             THEN '[MISSING_STATUS]; ' ELSE '' END
    ) AS data_quality_flag
FROM staging.ecom_orders_raw;
GO 

;WITH deduplicate AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY order_timestamp DESC
        ) AS rn
    FROM staging.ecom_orders_clean
)
DELETE FROM deduplicate
WHERE rn > 1;
GO

SELECT *
FROM staging.ecom_orders_clean
WHERE data_quality_flag <> '';
GO

SELECT TOP 20 *
FROM staging.ecom_orders_clean
ORDER BY order_timestamp;
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
    order_item_id        INT,
    order_id             VARCHAR(50),
    product_id           INT,
    quantity             INT,
    unit_price           DECIMAL(12,2),
    line_total           DECIMAL(12,2),
    calculated_line_total DECIMAL(12,2),
    data_quality_flag    VARCHAR(255)
);
GO

INSERT INTO staging.ecom_items_clean (
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price,
    line_total,
    calculated_line_total,
    data_quality_flag
)
SELECT
    TRY_CONVERT(INT, LTRIM(RTRIM(order_item_id))) AS order_item_id,

    LTRIM(RTRIM(order_id))      AS order_id,

    TRY_CONVERT(INT, LTRIM(RTRIM(product_id))) AS product_id,

    CASE
        WHEN TRY_CONVERT(INT, quantity) < 0
            THEN NULL
        ELSE TRY_CONVERT(INT, quantity)
    END AS quantity,

    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), unit_price) < 0
            THEN NULL
        ELSE TRY_CONVERT(DECIMAL(12,2), unit_price)
    END AS unit_price,

    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), line_total) < 0
            THEN NULL
        ELSE TRY_CONVERT(DECIMAL(12,2), line_total)
    END AS line_total,

    ROUND(
        COALESCE(TRY_CONVERT(INT, quantity), 0)
        * COALESCE(TRY_CONVERT(DECIMAL(12,2), unit_price), 0),
        2
    ) AS calculated_line_total,

    CONCAT(
        CASE
            WHEN TRY_CONVERT(INT, quantity) IS NULL
                 OR TRY_CONVERT(INT, quantity) < 0
                THEN '[BAD_QTY]; ' ELSE '' END,

        CASE
            WHEN TRY_CONVERT(DECIMAL(12,2), unit_price) IS NULL
                 OR TRY_CONVERT(DECIMAL(12,2), unit_price) < 0
                THEN '[BAD_PRICE]; ' ELSE '' END,

        CASE
            WHEN TRY_CONVERT(DECIMAL(12,2), line_total) IS NULL
                THEN '[BAD_LINE_TOTAL]; ' ELSE '' END,

        CASE
            WHEN TRY_CONVERT(DECIMAL(12,2), line_total) IS NOT NULL
             AND ABS(
                    TRY_CONVERT(DECIMAL(12,2), line_total)
                    - (
                        TRY_CONVERT(INT, quantity)
                        * TRY_CONVERT(DECIMAL(12,2), unit_price)
                      )
                 ) > 0.05
                THEN '[LINE_TOTAL_MISMATCH]; ' ELSE '' END
    ) AS data_quality_flag
FROM staging.ecom_items_raw;
GO

;WITH deduplicate AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_item_id
            ORDER BY calculated_line_total DESC
        ) AS rn
    FROM staging.ecom_items_clean
)
DELETE FROM deduplicate
WHERE rn > 1;
GO

SELECT *
FROM staging.ecom_items_clean
WHERE data_quality_flag <> '';
GO

SELECT TOP 20 *
FROM staging.ecom_items_clean
ORDER BY order_id, order_item_id;
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
    data_quality_flag          VARCHAR(255)
);
GO

INSERT INTO staging.inventory_snapshots_clean (
    snapshot_date,
    store_id,
    product_id,
    beginning_inventory,
    ending_inventory,
    inventory_value,
    safety_stock,
    stock_status,
    calculated_inventory_delta,
    data_quality_flag
)
SELECT
    TRY_CONVERT(DATE, snapshot_date) AS snapshot_date,

    TRY_CONVERT(INT, LTRIM(RTRIM(store_id))) AS store_id,

    TRY_CONVERT(INT,LTRIM(RTRIM(product_id))) AS product_id,

    CASE
        WHEN TRY_CONVERT(INT, beginning_inventory) < 0
            THEN NULL
        ELSE TRY_CONVERT(INT, beginning_inventory)
    END AS beginning_inventory,

    CASE
        WHEN TRY_CONVERT(INT, ending_inventory) < 0
            THEN NULL
        ELSE TRY_CONVERT(INT, ending_inventory)
    END AS ending_inventory,

    CASE
        WHEN TRY_CONVERT(DECIMAL(14,2), inventory_value) < 0
            THEN NULL
        ELSE TRY_CONVERT(DECIMAL(14,2), inventory_value)
    END AS inventory_value,

    CASE
        WHEN TRY_CONVERT(INT, safety_stock) < 0
            THEN NULL
        ELSE TRY_CONVERT(INT, safety_stock)
    END AS safety_stock,

    CASE
        WHEN stock_status LIKE '%OUT%'      THEN 'OUT_OF_STOCK'
        WHEN stock_status LIKE '%LOW%'      THEN 'LOW_STOCK'
        WHEN stock_status LIKE '%IN%'       THEN 'IN_STOCK'
        WHEN stock_status LIKE '%BACK%'     THEN 'BACKORDER'
        ELSE UPPER(LTRIM(RTRIM(stock_status)))
    END AS stock_status,

    TRY_CONVERT(INT, ending_inventory)
    - TRY_CONVERT(INT, beginning_inventory) AS calculated_inventory_delta,

    CONCAT(
        CASE WHEN TRY_CONVERT(DATE, snapshot_date) IS NULL
             THEN '[BAD_DATE]; ' ELSE '' END,

        CASE WHEN TRY_CONVERT(INT, store_id) IS NULL
             THEN '[BAD_STORE]; ' ELSE '' END,

        CASE WHEN TRY_CONVERT(INT, beginning_inventory) IS NULL
             THEN '[BAD_BEGIN_INV]; ' ELSE '' END,

        CASE WHEN TRY_CONVERT(INT, ending_inventory) IS NULL
             THEN '[BAD_END_INV]; ' ELSE '' END,

        CASE WHEN TRY_CONVERT(INT, ending_inventory)
               < TRY_CONVERT(INT, safety_stock)
             THEN '[BELOW_SAFETY_STOCK]; ' ELSE '' END
    ) AS data_quality_flag
FROM staging.inventory_snapshots_raw;
GO

;WITH deduplicate AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY snapshot_date, store_id, product_id
            ORDER BY ending_inventory DESC
        ) AS rn
    FROM staging.inventory_snapshots_clean
)
DELETE FROM deduplicate
WHERE rn > 1;
GO

SELECT *
FROM staging.inventory_snapshots_clean
WHERE data_quality_flag <> '';
GO

SELECT TOP 20 *
FROM staging.inventory_snapshots_clean
ORDER BY snapshot_date, store_id, product_id;
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
    return_id             VARCHAR(50),
    transaction_id        VARCHAR(50),
    product_id            INT,
    return_date           DATE,
    return_reason         VARCHAR(100),
    refund_amount         DECIMAL(12,2),
    quantity_returned     INT,
    return_channel        VARCHAR(50),
    data_quality_flag     VARCHAR(255)
);
GO

INSERT INTO staging.returns_clean (
    return_id,
    transaction_id,
    product_id,
    return_date,
    return_reason,
    refund_amount,
    quantity_returned,
    return_channel,
    data_quality_flag
)
SELECT
    LTRIM(RTRIM(return_id)) AS return_id,

    LTRIM(RTRIM(transaction_id)) AS transaction_id,

    TRY_CONVERT(INT, LTRIM(RTRIM(product_id))) AS product_id,

    TRY_CONVERT(DATE, return_date) AS return_date,

    CASE
        WHEN return_reason LIKE '%DEFECT%'   THEN 'DEFECTIVE'
        WHEN return_reason LIKE '%DAMAG%'    THEN 'DAMAGED'
        WHEN return_reason LIKE '%SIZE%'     THEN 'SIZE_FIT'
        WHEN return_reason LIKE '%NOT%'      THEN 'NOT_AS_DESCRIBED'
        WHEN return_reason LIKE '%CHANGE%'  THEN 'MIND_CHANGED'
        WHEN return_reason LIKE '%LATE%'     THEN 'LATE_DELIVERY'
        ELSE UPPER(LTRIM(RTRIM(return_reason)))
    END AS return_reason,

    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), refund_amount) < 0
            THEN NULL
        ELSE TRY_CONVERT(DECIMAL(12,2), refund_amount)
    END AS refund_amount,

    CASE
        WHEN TRY_CONVERT(INT, quantity_returned) <= 0
            THEN NULL
        ELSE TRY_CONVERT(INT, quantity_returned)
    END AS quantity_returned,

    CASE
        WHEN return_channel LIKE '%STORE%'  THEN 'IN_STORE'
        WHEN return_channel LIKE '%ONLINE%' THEN 'ONLINE'
        WHEN return_channel LIKE '%MAIL%'   THEN 'MAIL'
        ELSE UPPER(LTRIM(RTRIM(return_channel)))
    END AS return_channel,

    CONCAT(
        CASE WHEN TRY_CONVERT(DATE, return_date) IS NULL
             THEN '[BAD_RETURN_DATE]; ' ELSE '' END,

        CASE WHEN TRY_CONVERT(INT, quantity_returned) IS NULL
             OR TRY_CONVERT(INT, quantity_returned) <= 0
             THEN '[BAD_QTY]; ' ELSE '' END,

        CASE WHEN TRY_CONVERT(DECIMAL(12,2), refund_amount) IS NULL
             THEN '[BAD_REFUND]; ' ELSE '' END
    ) AS data_quality_flag
FROM staging.returns_raw;
GO

;WITH deduplicate AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY return_id
            ORDER BY return_date DESC
        ) AS rn
    FROM staging.returns_clean
)
DELETE FROM deduplicate
WHERE rn > 1;
GO

SELECT *
FROM staging.returns_clean
WHERE data_quality_flag <> '';
GO

SELECT TOP 20 *
FROM staging.returns_clean
ORDER BY return_date;
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
    sku               VARCHAR(50),
    product_name      VARCHAR(100),
    category           VARCHAR(50),
    subcategory        VARCHAR(50),
    brand              VARCHAR(50),
    cost               DECIMAL(12,2),
    price              DECIMAL(12,2),
    margin             DECIMAL(12,2),
    season             VARCHAR(50),
    launch_date        DATE,
    status             VARCHAR(50),
    data_quality_flag  VARCHAR(255)
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
    data_quality_flag
)
SELECT
    TRY_CONVERT(INT, LTRIM(RTRIM(product_id))) AS product_id,

    LTRIM(RTRIM(sku)) AS sku,

    LTRIM(RTRIM(product_name)) AS product_name,

    UPPER(LTRIM(RTRIM(category)))    AS category,

    UPPER(LTRIM(RTRIM(subcategory))) AS subcategory,

    UPPER(LTRIM(RTRIM(brand))) AS brand,

    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), cost) < 0
            THEN NULL
        ELSE TRY_CONVERT(DECIMAL(12,2), cost)
    END AS cost,

    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), price) < 0
            THEN NULL
        ELSE TRY_CONVERT(DECIMAL(12,2), price)
    END AS price,

    CASE
        WHEN TRY_CONVERT(DECIMAL(12,2), price) IS NOT NULL
         AND TRY_CONVERT(DECIMAL(12,2), cost)  IS NOT NULL
            THEN TRY_CONVERT(DECIMAL(12,2), price)
               - TRY_CONVERT(DECIMAL(12,2), cost)
        ELSE NULL
    END AS margin,

    CASE
        WHEN season LIKE '%SPRING%' THEN 'SPRING'
        WHEN season LIKE '%SUMMER%' THEN 'SUMMER'
        WHEN season LIKE '%FALL%'   THEN 'FALL'
        WHEN season LIKE '%AUTUMN%' THEN 'FALL'
        WHEN season LIKE '%WINTER%' THEN 'WINTER'
        ELSE UPPER(LTRIM(RTRIM(season)))
    END AS season,

    TRY_CONVERT(DATE, launch_date) AS launch_date,

    CASE
        WHEN status LIKE '%ACTIVE%'     THEN 'ACTIVE'
        WHEN status LIKE '%DISCONT%'    THEN 'DISCONTINUED'
        WHEN status LIKE '%INACTIVE%'   THEN 'INACTIVE'
        WHEN status LIKE '%OUT%'        THEN 'OUT_OF_CATALOG'
        ELSE UPPER(LTRIM(RTRIM(status)))
    END AS status,

    CONCAT(
        CASE WHEN TRY_CONVERT(DATE, launch_date) IS NULL
             THEN '[BAD_LAUNCH_DATE]; ' ELSE '' END,

        CASE WHEN TRY_CONVERT(DECIMAL(12,2), price) IS NULL
             THEN '[BAD_PRICE]; ' ELSE '' END,

        CASE WHEN TRY_CONVERT(DECIMAL(12,2), cost) IS NULL
             THEN '[BAD_COST]; ' ELSE '' END,

        CASE WHEN TRY_CONVERT(DECIMAL(12,2), price) <
                  TRY_CONVERT(DECIMAL(12,2), cost)
             THEN '[NEGATIVE_MARGIN]; ' ELSE '' END
    ) AS data_quality_flag
FROM staging.products_raw;
GO

;WITH deduplicate AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY launch_date DESC
        ) AS rn
    FROM staging.products_clean
)
DELETE FROM deduplicate
WHERE rn > 1;
GO

SELECT *
FROM staging.products_clean
WHERE data_quality_flag <> '';
GO

SELECT TOP 20 *
FROM staging.products_clean
ORDER BY product_id;
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
    store_id           INT,
    store_name         VARCHAR(100),
    store_type         VARCHAR(50),
    region             VARCHAR(50),
    address            VARCHAR(150),
    opening_date       DATE,
    manager_id         VARCHAR(50),
    data_quality_flag  VARCHAR(255)
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
    data_quality_flag
)
SELECT
    TRY_CONVERT(INT, store_id) AS store_id,

    LTRIM(RTRIM(store_name)) AS store_name,

    CASE
        WHEN store_type LIKE '%FLAGSHIP%' THEN 'FLAGSHIP'
        WHEN store_type LIKE '%OUTLET%'   THEN 'OUTLET'
        WHEN store_type LIKE '%POP%'      THEN 'POP_UP'
        WHEN store_type LIKE '%KIOSK%'    THEN 'KIOSK'
        ELSE UPPER(LTRIM(RTRIM(store_type)))
    END AS store_type,

    CASE
        WHEN region LIKE '%WEST%'  THEN 'WEST'
        WHEN region LIKE '%EAST%'  THEN 'EAST'
        WHEN region LIKE '%CENT%'  THEN 'CENTRAL'
        WHEN region LIKE '%NORTH%' THEN 'NORTH'
        WHEN region LIKE '%SOUTH%' THEN 'SOUTH'
        ELSE UPPER(LTRIM(RTRIM(region)))
    END AS region,

    LTRIM(RTRIM(address)) AS address,

    TRY_CONVERT(DATE, opening_date) AS opening_date,

    NULLIF(LTRIM(RTRIM(manager_id)), '') AS manager_id,

    CONCAT(
        CASE WHEN TRY_CONVERT(INT, store_id) IS NULL
             THEN '[BAD_STORE_ID]; ' ELSE '' END,

        CASE WHEN LTRIM(RTRIM(store_name)) IS NULL
             OR LTRIM(RTRIM(store_name)) = ''
             THEN '[MISSING_STORE_NAME]; ' ELSE '' END,

        CASE WHEN TRY_CONVERT(DATE, opening_date) IS NULL
             THEN '[BAD_OPENING_DATE]; ' ELSE '' END
    ) AS data_quality_flag
FROM staging.stores_raw;
GO

;WITH deduplicate AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY store_id
            ORDER BY opening_date DESC
        ) AS rn
    FROM staging.stores_clean
)
DELETE FROM deduplicate
WHERE rn > 1;
GO

SELECT *
FROM staging.stores_clean
WHERE data_quality_flag <> '';
GO

SELECT TOP 20 *
FROM staging.stores_clean
ORDER BY store_id;
GO