/* =============================================================================
   RETAIL ANALYTICS DATA WAREHOUSE
   -----------------------------------------------------------------------------
   Section: DATA INTEGRITY & PERFORMANCE OPTIMIZATION
   Platform: SQL Server
   Author: Ed Hideaki
   -----------------------------------------------------------------------------
   Purpose:
   - Enforce referential integrity between facts and dimensions
   - Protect fact table grains from accidental duplication
   - Optimize query performance for BI dashboards and analytics workloads
   -----------------------------------------------------------------------------
   Why This Matters:
   - Prevents silent data corruption
   - Guarantees analytical correctness
   - Ensures fast dashboard response times at scale
   -----------------------------------------------------------------------------
   Notes:
   - Script is IDEMPOTENT (safe to re-run)
   - Constraints are added WITH CHECK (validates existing data)
   - Index creation guarded via sys.indexes checks
============================================================================= */

/* =============================================================================
   01) FOREIGN KEY CONSTRAINTS - REFERENTIAL INTEGRITY
   -----------------------------------------------------------------------------
   Enforces valid relationships between facts and dimensions.
   WITH CHECK ensures SQL Server validates existing rows (no "untrusted" FKs).
============================================================================= */

/* -----------------------------------------------------------------------------
   FACT_POS_TRANSACTIONS -> DIM_DATE, DIM_STORE
----------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_pos_transactions_date')
BEGIN
    ALTER TABLE core.fact_pos_transactions
    WITH CHECK ADD CONSTRAINT fk_pos_transactions_date
    FOREIGN KEY (date_sk) REFERENCES core.dim_date (date_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_pos_transactions_store')
BEGIN
    ALTER TABLE core.fact_pos_transactions
    WITH CHECK ADD CONSTRAINT fk_pos_transactions_store
    FOREIGN KEY (store_sk) REFERENCES core.dim_store (store_sk);
END;
GO


/* -----------------------------------------------------------------------------
   FACT_ECOM_ORDERS -> DIM_DATE
----------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_ecom_orders_date')
BEGIN
    ALTER TABLE core.fact_ecom_orders
    WITH CHECK ADD CONSTRAINT fk_ecom_orders_date
    FOREIGN KEY (date_sk) REFERENCES core.dim_date (date_sk);
END;
GO


/* -----------------------------------------------------------------------------
   FACT_SALES_ITEMS -> DIM_PRODUCT, DIM_STORE, DIM_DATE
   NOTE: store_sk is intentionally nullable for E-Commerce rows.
----------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_sales_items_product')
BEGIN
    ALTER TABLE core.fact_sales_items
    WITH CHECK ADD CONSTRAINT fk_sales_items_product
    FOREIGN KEY (product_sk) REFERENCES core.dim_product (product_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_sales_items_store')
BEGIN
    ALTER TABLE core.fact_sales_items
    WITH CHECK ADD CONSTRAINT fk_sales_items_store
    FOREIGN KEY (store_sk) REFERENCES core.dim_store (store_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_sales_items_date')
BEGIN
    ALTER TABLE core.fact_sales_items
    WITH CHECK ADD CONSTRAINT fk_sales_items_date
    FOREIGN KEY (date_sk) REFERENCES core.dim_date (date_sk);
END;
GO


/* -----------------------------------------------------------------------------
   FACT_RETURNS -> DIM_PRODUCT, DIM_STORE, DIM_DATE
   NOTE: store_sk is nullable when a return cannot be tied to a physical store.
----------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_returns_product')
BEGIN
    ALTER TABLE core.fact_returns
    WITH CHECK ADD CONSTRAINT fk_returns_product
    FOREIGN KEY (product_sk) REFERENCES core.dim_product (product_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_returns_store')
BEGIN
    ALTER TABLE core.fact_returns
    WITH CHECK ADD CONSTRAINT fk_returns_store
    FOREIGN KEY (store_sk) REFERENCES core.dim_store (store_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_returns_date')
BEGIN
    ALTER TABLE core.fact_returns
    WITH CHECK ADD CONSTRAINT fk_returns_date
    FOREIGN KEY (date_sk) REFERENCES core.dim_date (date_sk);
END;
GO


/* -----------------------------------------------------------------------------
   FACT_INVENTORY_SNAPSHOTS -> DIM_PRODUCT, DIM_STORE, DIM_DATE
----------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_inventory_product')
BEGIN
    ALTER TABLE core.fact_inventory_snapshots
    WITH CHECK ADD CONSTRAINT fk_inventory_product
    FOREIGN KEY (product_sk) REFERENCES core.dim_product (product_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_inventory_store')
BEGIN
    ALTER TABLE core.fact_inventory_snapshots
    WITH CHECK ADD CONSTRAINT fk_inventory_store
    FOREIGN KEY (store_sk) REFERENCES core.dim_store (store_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = 'fk_inventory_date')
BEGIN
    ALTER TABLE core.fact_inventory_snapshots
    WITH CHECK ADD CONSTRAINT fk_inventory_date
    FOREIGN KEY (date_sk) REFERENCES core.dim_date (date_sk);
END;
GO


/* =============================================================================
   02) GRAIN PROTECTION - UNIQUENESS ENFORCEMENT
   -----------------------------------------------------------------------------
   Prevents duplicate fact records that would distort metrics and KPIs.
============================================================================= */

/* -----------------------------------------------------------------------------
   Inventory Snapshot Grain Protection
   Grain: One row per product per store per date
----------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ux_inventory_snapshot_grain')
BEGIN
    CREATE UNIQUE INDEX ux_inventory_snapshot_grain
    ON core.fact_inventory_snapshots (product_sk, store_sk, date_sk);
END;
GO


/* -----------------------------------------------------------------------------
   POS Transactions - Natural Key Protection
----------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ux_pos_transaction_id')
BEGIN
    CREATE UNIQUE INDEX ux_pos_transaction_id
    ON core.fact_pos_transactions (transaction_id);
END;
GO


/* -----------------------------------------------------------------------------
   E-Commerce Orders - Natural Key Protection
----------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ux_ecom_order_id')
BEGIN
    CREATE UNIQUE INDEX ux_ecom_order_id
    ON core.fact_ecom_orders (order_id);
END;
GO


/* -----------------------------------------------------------------------------
   Returns - Natural Key Protection
----------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ux_return_id')
BEGIN
    CREATE UNIQUE INDEX ux_return_id
    ON core.fact_returns (return_id);
END;
GO


/* -----------------------------------------------------------------------------
   Unified Sales Items - Grain Protection
   Grain: One row per (source_system, transaction_id/order_id, product, date)
----------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ux_sales_items_grain')
BEGIN
    CREATE UNIQUE INDEX ux_sales_items_grain
    ON core.fact_sales_items (source_system, transaction_id, product_sk, date_sk);
END;
GO


/* =============================================================================
   03) PERFORMANCE INDEXES - BI & ANALYTICS OPTIMIZATION
   -----------------------------------------------------------------------------
   Designed for common dashboard queries:
   - Trends by date
   - Product performance
   - Store performance
============================================================================= */

/* -----------------------------------------------------------------------------
   Date-Based Indexes (Most Important for BI Trend Charts)
----------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_pos_transactions_date')
BEGIN
    CREATE INDEX ix_pos_transactions_date
    ON core.fact_pos_transactions (date_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_ecom_orders_date')
BEGIN
    CREATE INDEX ix_ecom_orders_date
    ON core.fact_ecom_orders (date_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_sales_items_date')
BEGIN
    CREATE INDEX ix_sales_items_date
    ON core.fact_sales_items (date_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_inventory_snapshots_date')
BEGIN
    CREATE INDEX ix_inventory_snapshots_date
    ON core.fact_inventory_snapshots (date_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_returns_date')
BEGIN
    CREATE INDEX ix_returns_date
    ON core.fact_returns (date_sk);
END;
GO


/* -----------------------------------------------------------------------------
   Product-Centric Analysis Indexes
----------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_sales_items_product')
BEGIN
    CREATE INDEX ix_sales_items_product
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


/* -----------------------------------------------------------------------------
   Store-Centric Analysis Indexes
----------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_pos_transactions_store')
BEGIN
    CREATE INDEX ix_pos_transactions_store
    ON core.fact_pos_transactions (store_sk);
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_sales_items_store')
BEGIN
    CREATE INDEX ix_sales_items_store
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
   -----------------------------------------------------------------------------
   Ensures fast joins from facts to dimensions and enforces natural key uniqueness.
   Note: UNIQUE constraints already exist on NKs in table DDL in many designs.
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
   05) OPTIONAL: COVERING INDEXES FOR HIGH-FREQUENCY DASHBOARDS
   -----------------------------------------------------------------------------
   Use INCLUDE to reduce key lookups for common visuals.
============================================================================= */

/* -----------------------------------------------------------------------------
   Product Revenue & Trend Dashboards (Product x Date, includes measures)
----------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = 'ix_sales_items_product_date')
BEGIN
    CREATE INDEX ix_sales_items_product_date
    ON core.fact_sales_items (product_sk, date_sk)
    INCLUDE (quantity, line_revenue);
END;
GO
