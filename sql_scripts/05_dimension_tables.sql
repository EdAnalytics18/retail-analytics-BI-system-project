/* =============================================================================
   CORE LAYER - ENTERPRISE STAR SCHEMA (DIMENSIONS & FACT TABLES)
   =============================================================================
   Executive Summary:
   This layer represents the trusted analytical backbone of the Retail
   Analytics BI System. Data here is modeled using a star schema to ensure
   fast, consistent, and business-friendly analytics.

   The core layer establishes a single source of truth for key business
   entities (dates, products, stores) that are shared across all fact tables.

   -----------------------------------------------------------------------------
   Business Value:
   - Ensures consistent KPIs across Finance, Marketing, and Operations
   - Enables high-performance BI dashboards and ad-hoc analysis
   - Eliminates conflicting metric definitions across teams
   - Provides a stable foundation for forecasting and executive reporting

   -----------------------------------------------------------------------------
   Input Layer:
   - staging.*_clean tables (Silver / Clean)
   - These inputs have already:
     - Enforced correct data types
     - Standardized categorical values
     - Flagged bad records without data loss
     - Flagged duplicates using is_duplicate (not deleted)

   The core layer focuses strictly on dimensional modeling, not data cleanup.
============================================================================= */


/* =============================================================================
   DIMENSIONS
   =============================================================================
   Purpose:
   Dimension tables provide descriptive context for fact tables.
   They allow metrics to be sliced, filtered, and grouped in ways that align
   with how the business thinks about performance.

   Design Principles:
   - Surrogate keys for performance and stability
   - Natural keys retained for traceability
   - One row per real-world business entity
============================================================================= */


/* -----------------------------------------------------------------------------
   DIM_DATE - CALENDAR DIMENSION
   -----------------------------------------------------------------------------
   Description:
   Centralized calendar dimension used for all date-based reporting.

   Grain:
   One row per calendar date.

   Why This Matters:
   - Guarantees consistent definitions of year, quarter, and month
   - Prevents duplicate date logic across dashboards
   - Enables time-series analysis, seasonality, and YoY comparisons

   Notes:
   - Uses an integer surrogate key (YYYYMMDD) for efficient joins
   - Pre-generated for a fixed date range to simplify analytics
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.dim_date;
GO

CREATE TABLE core.dim_date (
    date_sk        INT           NOT NULL PRIMARY KEY,
    full_date      DATE          NOT NULL UNIQUE,
    year_num       INT           NOT NULL,
    quarter_num    INT           NOT NULL,
    month_num      INT           NOT NULL,
    month_name     VARCHAR(20)   NOT NULL,
    day_num        INT           NOT NULL,
    day_name       VARCHAR(20)   NOT NULL,
    is_weekend     BIT           NOT NULL,
    load_timestamp DATETIME2     NOT NULL DEFAULT SYSDATETIME()
);
GO

;WITH dates AS (
    SELECT CAST('2018-01-01' AS DATE) AS dt
    UNION ALL
    SELECT DATEADD(DAY, 1, dt)
    FROM dates
    WHERE dt < '2030-12-31'
)
INSERT INTO core.dim_date (
    date_sk,
    full_date,
    year_num,
    quarter_num,
    month_num,
    month_name,
    day_num,
    day_name,
    is_weekend
)
SELECT
    CONVERT(INT, FORMAT(dt, 'yyyyMMdd')) AS date_sk,
    dt,
    YEAR(dt),
    DATEPART(QUARTER, dt),
    MONTH(dt),
    DATENAME(MONTH, dt),
    DAY(dt),
    DATENAME(WEEKDAY, dt),
    CASE
        WHEN DATENAME(WEEKDAY, dt) IN ('Saturday', 'Sunday') THEN 1
        ELSE 0
    END
FROM dates
OPTION (MAXRECURSION 0);
GO


/* -----------------------------------------------------------------------------
   DIM_PRODUCT - PRODUCT MASTER DIMENSION
   -----------------------------------------------------------------------------
   Description:
   Trusted product master data used across all sales, inventory,
   and profitability reporting.

   Grain:
   One row per product_id (natural key).

   Source:
   staging.products_clean

   Business Rules:
   - Only the latest valid version of each product is promoted
   - Duplicate records are excluded using deterministic rules
   - Financial attributes (cost, price, margin) already validated upstream

   Why This Matters:
   - Prevents duplicated or inconsistent product reporting
   - Ensures margin calculations are consistent organization-wide
   - Supports category, brand, and lifecycle analysis
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.dim_product;
GO

CREATE TABLE core.dim_product (
    product_sk     INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
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
    load_timestamp DATETIME2,
    source_file    VARCHAR(255)
);
GO

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
FROM staging.products_clean
WHERE is_duplicate = 0
  AND product_id IS NOT NULL;
GO


/* -----------------------------------------------------------------------------
   DIM_STORE - STORE / LOCATION DIMENSION
   -----------------------------------------------------------------------------
   Description:
   Represents physical retail locations and their organizational attributes.

   Grain:
   One row per store_id (natural key).

   Source:
   staging.stores_clean

   Business Rules:
   - Only current, non-duplicate store records are included
   - Regional and store-type attributes standardized upstream

   Why This Matters:
   - Enables accurate regional and store-level performance analysis
   - Prevents double-counting due to duplicated store records
   - Provides consistent organizational context for all store-based KPIs
----------------------------------------------------------------------------- */
DROP TABLE IF EXISTS core.dim_store;
GO

CREATE TABLE core.dim_store (
    store_sk       INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    store_id       INT NOT NULL UNIQUE,
    store_name     VARCHAR(255),
    store_type     VARCHAR(50),
    region         VARCHAR(50),
    address        VARCHAR(255),
    opening_date   DATE,
    manager_id     VARCHAR(100),
    load_timestamp DATETIME2,
    source_file    VARCHAR(255)
);
GO

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
FROM staging.stores_clean
WHERE is_duplicate = 0
  AND store_id IS NOT NULL;
GO
