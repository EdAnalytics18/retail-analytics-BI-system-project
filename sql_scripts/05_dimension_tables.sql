/* =============================================================================
   CORE LAYER - DIMENSION TABLES
   -----------------------------------------------------------------------------
   Purpose:
   - Store descriptive attributes used to slice facts
   - Provide conformed dimensions shared across all fact tables
   -----------------------------------------------------------------------------
   Design Decisions:
   - Surrogate keys for performance
   - Natural key uniqueness enforcement
   - BI-friendly attributes
============================================================================= */

/* -----------------------------------------------------------------------------
DIM_DATE
   Grain: One row per calendar date
   Role:
   - Unified time dimension for all facts
   - Enables time-series analysis (YoY, MoM, QoQ)
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.dim_date;
GO

CREATE TABLE core.dim_date (
    date_sk        INT PRIMARY KEY,
    full_date      DATE NOT NULL UNIQUE,
    year_num       INT NOT NULL,
    quarter_num    INT NOT NULL,
    month_num      INT NOT NULL,
    month_name     VARCHAR(20),
    day_num        INT NOT NULL,
    day_name       VARCHAR(20),
    is_weekend     BIT NOT NULL,
    load_timestamp DATETIME DEFAULT GETDATE()
);
GO

-- Generate calendar dates (MAXRECURSION disabled intentionally)
;WITH dates AS (
    SELECT CAST('2018-01-01' AS DATE) AS dt
    UNION ALL
    SELECT DATEADD(DAY, 1, dt)
    FROM dates
    WHERE dt < '2030-12-31'
)
INSERT INTO core.dim_date
SELECT
    CONVERT(INT, FORMAT(dt, 'yyyyMMdd')) AS date_sk,
    dt,
    YEAR(dt),
    DATEPART(QUARTER, dt),
    MONTH(dt),
    DATENAME(MONTH, dt),
    DAY(dt),
    DATENAME(WEEKDAY, dt),
    CASE WHEN DATENAME(WEEKDAY, dt) IN ('Saturday','Sunday') THEN 1 ELSE 0 END,
    GETDATE()
FROM dates
OPTION (MAXRECURSION 0);
GO


/* -----------------------------------------------------------------------------
   DIM_PRODUCT
   Grain: One row per product (natural key: product_id)
   Role:
   - Central product master used across sales, inventory, and returns
   - Stores pricing, cost, margin, and classification attributes
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.dim_product;
GO

CREATE TABLE core.dim_product (
    product_sk     INT IDENTITY(1,1) PRIMARY KEY,
    product_id     INT NOT NULL UNIQUE,
    sku            VARCHAR(100),
    product_name   VARCHAR(255),
    category       VARCHAR(50),
    subcategory    VARCHAR(50),
    brand          VARCHAR(50),
    cost           DECIMAL(12,2),
    price          DECIMAL(12,2),
    margin         DECIMAL(12,2),
    season         VARCHAR(50),
    launch_date    DATE,
    status         VARCHAR(50),
    load_timestamp DATETIME,
    source_file    VARCHAR(255)
);
GO

;WITH deduplicate AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY product_id
               ORDER BY load_timestamp DESC
           ) AS rn
    FROM staging.products_clean
)
INSERT INTO core.dim_product (
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
    load_timestamp,
    source_file
)
SELECT
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
    load_timestamp,
    source_file
FROM deduplicate
WHERE rn = 1;
GO


/* -----------------------------------------------------------------------------
   DIM_STORE
   Grain: One row per physical store location
   Role:
   - Store-level analysis for sales, inventory, and returns
   - Enables regional and store-type performance insights
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.dim_store;
GO

CREATE TABLE core.dim_store (
    store_sk       INT IDENTITY(1,1) PRIMARY KEY,
    store_id       INT NOT NULL UNIQUE,
    store_name     VARCHAR(255),
    store_type     VARCHAR(50),
    region         VARCHAR(50),
    address        VARCHAR(255),
    opening_date   DATE,
    manager_id     VARCHAR(100),
    load_timestamp DATETIME,
    source_file    VARCHAR(255)
);
GO

;WITH deduplicate AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY store_id
               ORDER BY load_timestamp DESC
           ) AS rn
    FROM staging.stores_clean
)
INSERT INTO core.dim_store (
    store_id,
    store_name,
    store_type,
    region,
    address,
    opening_date,
    manager_id,
    load_timestamp,
    source_file
)
SELECT
    store_id,
    store_name,
    store_type,
    region,
    address,
    opening_date,
    manager_id,
    load_timestamp,
    source_file
FROM deduplicate
WHERE rn = 1;
GO
