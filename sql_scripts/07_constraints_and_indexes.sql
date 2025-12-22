/* =============================================================================
   RETAIL ANALYTICS DATA WAREHOUSE
   -----------------------------------------------------------------------------
   Section: DATA INTEGRITY & PERFORMANCE OPTIMIZATION
   -----------------------------------------------------------------------------
   Executive Summary:
   This script hardens the analytics warehouse by enforcing data integrity,
   protecting fact table grains, and optimizing query performance for BI
   dashboards and analytical workloads.

   While earlier layers focus on ingestion, cleaning, and modeling, this
   section ensures the warehouse remains:
   - Correct (no broken relationships or duplicated facts)
   - Trustworthy (metrics cannot drift silently)
   - Fast (optimized for dashboard and ad-hoc query performance)

   -----------------------------------------------------------------------------
   Business Value:
   - Prevents inaccurate KPIs caused by orphaned or duplicated records
   - Guarantees consistency between facts and dimensions
   - Improves dashboard responsiveness for executives and analysts
   - Aligns with enterprise BI and data governance best practices

   -----------------------------------------------------------------------------
   Operational Notes:
   - Script is IDEMPOTENT (safe to re-run)
   - Constraints are added WITH CHECK to validate existing data
   - Index creation is guarded using system catalog checks
============================================================================= */

SET NOCOUNT ON;
GO


/* =============================================================================
   01) REFERENTIAL INTEGRITY - FOREIGN KEY CONSTRAINTS
   =============================================================================
   Purpose:
   Enforce valid relationships between fact tables and their corresponding
   dimensions.

   Why This Matters:
   - Prevents orphaned fact records
   - Guarantees dimensional joins always resolve correctly
   - Ensures metrics roll up accurately across dimensions

   Note:
   Constraints are added WITH CHECK to ensure existing data is validated,
   preventing "untrusted" relationships.
============================================================================= */


/* ---------------------------------------------------------------------------
   FACT_POS_TRANSACTIONS - DIM_DATE, DIM_STORE
--------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_fact_pos_date')
BEGIN
    ALTER TABLE core.fact_pos_transactions
    WITH CHECK ADD CONSTRAINT fk_fact_pos_date
    FOREIGN KEY (date_sk) REFERENCES core.dim_date (date_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_fact_pos_store')
BEGIN
    ALTER TABLE core.fact_pos_transactions
    WITH CHECK ADD CONSTRAINT fk_fact_pos_store
    FOREIGN KEY (store_sk) REFERENCES core.dim_store (store_sk);
END;
GO


/* ---------------------------------------------------------------------------
   FACT_ECOM_ORDERS - DIM_DATE
--------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_fact_ecom_date')
BEGIN
    ALTER TABLE core.fact_ecom_orders
    WITH CHECK ADD CONSTRAINT fk_fact_ecom_date
    FOREIGN KEY (date_sk) REFERENCES core.dim_date (date_sk);
END;
GO


/* ---------------------------------------------------------------------------
   FACT_SALES_ITEMS - DIM_PRODUCT, DIM_STORE, DIM_DATE
   Note: store_sk is nullable by design (e-commerce orders)
--------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_fact_sales_product')
BEGIN
    ALTER TABLE core.fact_sales_items
    WITH CHECK ADD CONSTRAINT fk_fact_sales_product
    FOREIGN KEY (product_sk) REFERENCES core.dim_product (product_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_fact_sales_store')
BEGIN
    ALTER TABLE core.fact_sales_items
    WITH CHECK ADD CONSTRAINT fk_fact_sales_store
    FOREIGN KEY (store_sk) REFERENCES core.dim_store (store_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_fact_sales_date')
BEGIN
    ALTER TABLE core.fact_sales_items
    WITH CHECK ADD CONSTRAINT fk_fact_sales_date
    FOREIGN KEY (date_sk) REFERENCES core.dim_date (date_sk);
END;
GO


/* ---------------------------------------------------------------------------
   FACT_RETURNS - DIM_PRODUCT, DIM_STORE, DIM_DATE
--------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_fact_returns_product')
BEGIN
    ALTER TABLE core.fact_returns
    WITH CHECK ADD CONSTRAINT fk_fact_returns_product
    FOREIGN KEY (product_sk) REFERENCES core.dim_product (product_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_fact_returns_store')
BEGIN
    ALTER TABLE core.fact_returns
    WITH CHECK ADD CONSTRAINT fk_fact_returns_store
    FOREIGN KEY (store_sk) REFERENCES core.dim_store (store_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_fact_returns_date')
BEGIN
    ALTER TABLE core.fact_returns
    WITH CHECK ADD CONSTRAINT fk_fact_returns_date
    FOREIGN KEY (date_sk) REFERENCES core.dim_date (date_sk);
END;
GO


/* ---------------------------------------------------------------------------
   FACT_INVENTORY_SNAPSHOTS - DIM_PRODUCT, DIM_STORE, DIM_DATE
--------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_fact_inventory_product')
BEGIN
    ALTER TABLE core.fact_inventory_snapshots
    WITH CHECK ADD CONSTRAINT fk_fact_inventory_product
    FOREIGN KEY (product_sk) REFERENCES core.dim_product (product_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_fact_inventory_store')
BEGIN
    ALTER TABLE core.fact_inventory_snapshots
    WITH CHECK ADD CONSTRAINT fk_fact_inventory_store
    FOREIGN KEY (store_sk) REFERENCES core.dim_store (store_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_fact_inventory_date')
BEGIN
    ALTER TABLE core.fact_inventory_snapshots
    WITH CHECK ADD CONSTRAINT fk_fact_inventory_date
    FOREIGN KEY (date_sk) REFERENCES core.dim_date (date_sk);
END;
GO


/* =============================================================================
   02) GRAIN PROTECTION - UNIQUENESS CONSTRAINTS
   =============================================================================
   Purpose:
   Protect each fact tables declared grain from accidental duplication.

   Why This Matters:
   - Prevents silent double-counting in KPIs
   - Enforces modeling assumptions at the database level
   - Acts as a safety net against bad loads or logic regressions
============================================================================= */

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'ux_fact_inventory_snapshots_grain'
)
BEGIN
    CREATE UNIQUE INDEX ux_fact_inventory_snapshots_grain
    ON core.fact_inventory_snapshots (product_sk, store_sk, date_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ux_fact_pos_transaction')
BEGIN
    CREATE UNIQUE INDEX ux_fact_pos_transaction
    ON core.fact_pos_transactions (transaction_id);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ux_fact_ecom_order')
BEGIN
    CREATE UNIQUE INDEX ux_fact_ecom_order
    ON core.fact_ecom_orders (order_id);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ux_fact_return')
BEGIN
    CREATE UNIQUE INDEX ux_fact_return
    ON core.fact_returns (return_id);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ux_fact_sales_items_grain')
BEGIN
    CREATE UNIQUE INDEX ux_fact_sales_items_grain
    ON core.fact_sales_items (source_system, transaction_id, product_sk, date_sk);
END;
GO


/* =============================================================================
   03) PERFORMANCE INDEXES - BI & ANALYTICS
   =============================================================================
   Purpose:
   Optimize query performance for the most common dashboard access patterns.

   Why This Matters:
   - Faster executive dashboards
   - Reduced load on the database
   - Better scalability as data volume grows
============================================================================= */

/* Date-centric analysis */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_pos_date')
BEGIN
    CREATE INDEX ix_pos_date
    ON core.fact_pos_transactions (date_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ecom_date')
BEGIN
    CREATE INDEX ix_ecom_date
    ON core.fact_ecom_orders (date_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_sales_date')
BEGIN
    CREATE INDEX ix_sales_date
    ON core.fact_sales_items (date_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_inventory_date')
BEGIN
    CREATE INDEX ix_inventory_date
    ON core.fact_inventory_snapshots (date_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_returns_date')
BEGIN
    CREATE INDEX ix_returns_date
    ON core.fact_returns (date_sk);
END;
GO


/* Product-centric analysis */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_sales_product')
BEGIN
    CREATE INDEX ix_sales_product
    ON core.fact_sales_items (product_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_returns_product')
BEGIN
    CREATE INDEX ix_returns_product
    ON core.fact_returns (product_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_inventory_product')
BEGIN
    CREATE INDEX ix_inventory_product
    ON core.fact_inventory_snapshots (product_sk);
END;
GO


/* Store-centric analysis */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_pos_store')
BEGIN
    CREATE INDEX ix_pos_store
    ON core.fact_pos_transactions (store_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_sales_store')
BEGIN
    CREATE INDEX ix_sales_store
    ON core.fact_sales_items (store_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_inventory_store')
BEGIN
    CREATE INDEX ix_inventory_store
    ON core.fact_inventory_snapshots (store_sk);
END;
GO


/* =============================================================================
   04) DIMENSION LOOKUP OPTIMIZATION
   =============================================================================
   Purpose:
   Ensure fast and reliable joins from facts to dimensions using natural keys.
============================================================================= */

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ux_dim_product_nk')
BEGIN
    CREATE UNIQUE INDEX ux_dim_product_nk
    ON core.dim_product (product_id);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ux_dim_store_nk')
BEGIN
    CREATE UNIQUE INDEX ux_dim_store_nk
    ON core.dim_store (store_id);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ux_dim_date_nk')
BEGIN
    CREATE UNIQUE INDEX ux_dim_date_nk
    ON core.dim_date (full_date);
END;
GO


/* =============================================================================
   05) COVERING INDEXES - HIGH-FREQUENCY DASHBOARDS
   =============================================================================
   Purpose:
   Support common dashboard queries with minimal table access.

   Example Use Case:
   - Product performance dashboards by date
   - High-level sales trend visualizations
============================================================================= */

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_sales_product_date_cover')
BEGIN
    CREATE INDEX ix_sales_product_date_cover
    ON core.fact_sales_items (product_sk, date_sk)
    INCLUDE (quantity, line_revenue);
END;
GO

