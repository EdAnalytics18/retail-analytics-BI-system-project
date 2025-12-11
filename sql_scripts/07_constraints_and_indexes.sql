/* =============================================================================
   DATA INTEGRITY & PERFORMANCE OPTIMIZATION
   -----------------------------------------------------------------------------
   Purpose:
   - Enforce referential integrity between facts and dimensions
   - Protect fact table grains from accidental duplication
   - Optimize query performance for BI, dashboards, and analytics
   -----------------------------------------------------------------------------
   Why This Matters:
   - Prevents silent data corruption
   - Guarantees analytical correctness
   - Ensures fast dashboard response times at scale
============================================================================= */


/* =============================================================================
   FOREIGN KEY CONSTRAINTS ? REFERENTIAL INTEGRITY
   -----------------------------------------------------------------------------
   Enforces valid relationships between fact and dimension tables
============================================================================= */


/* -----------------------------------------------------------------------------
   FACT_POS_TRANSACTIONS Foreign Keys
----------------------------------------------------------------------------- */
ALTER TABLE core.fact_pos_transactions
ADD CONSTRAINT fk_pos_transactions_date
FOREIGN KEY (date_sk)
REFERENCES core.dim_date (date_sk);

ALTER TABLE core.fact_pos_transactions
ADD CONSTRAINT fk_pos_transactions_store
FOREIGN KEY (store_sk)
REFERENCES core.dim_store (store_sk);


/* -----------------------------------------------------------------------------
   FACT_ECOM_ORDERS Foreign Keys
----------------------------------------------------------------------------- */
ALTER TABLE core.fact_ecom_orders
ADD CONSTRAINT fk_ecom_orders_date
FOREIGN KEY (date_sk)
REFERENCES core.dim_date (date_sk);


/* -----------------------------------------------------------------------------
   FACT_SALES_ITEMS Foreign Keys
----------------------------------------------------------------------------- */
ALTER TABLE core.fact_sales_items
ADD CONSTRAINT fk_sales_items_product
FOREIGN KEY (product_sk)
REFERENCES core.dim_product (product_sk);

ALTER TABLE core.fact_sales_items
ADD CONSTRAINT fk_sales_items_store
FOREIGN KEY (store_sk)
REFERENCES core.dim_store (store_sk);

ALTER TABLE core.fact_sales_items
ADD CONSTRAINT fk_sales_items_date
FOREIGN KEY (date_sk)
REFERENCES core.dim_date (date_sk);

-- NOTE:
-- store_sk is intentionally nullable for E-Commerce sales rows


/* -----------------------------------------------------------------------------
   FACT_RETURNS Foreign Keys
----------------------------------------------------------------------------- */
ALTER TABLE core.fact_returns
ADD CONSTRAINT fk_returns_product
FOREIGN KEY (product_sk)
REFERENCES core.dim_product (product_sk);

ALTER TABLE core.fact_returns
ADD CONSTRAINT fk_returns_store
FOREIGN KEY (store_sk)
REFERENCES core.dim_store (store_sk);

ALTER TABLE core.fact_returns
ADD CONSTRAINT fk_returns_date
FOREIGN KEY (date_sk)
REFERENCES core.dim_date (date_sk);


/* -----------------------------------------------------------------------------
   FACT_INVENTORY_SNAPSHOTS Foreign Keys
----------------------------------------------------------------------------- */
ALTER TABLE core.fact_inventory_snapshots
ADD CONSTRAINT fk_inventory_product
FOREIGN KEY (product_sk)
REFERENCES core.dim_product (product_sk);

ALTER TABLE core.fact_inventory_snapshots
ADD CONSTRAINT fk_inventory_store
FOREIGN KEY (store_sk)
REFERENCES core.dim_store (store_sk);

ALTER TABLE core.fact_inventory_snapshots
ADD CONSTRAINT fk_inventory_date
FOREIGN KEY (date_sk)
REFERENCES core.dim_date (date_sk);



/* =============================================================================
   GRAIN PROTECTION - UNIQUENESS ENFORCEMENT
   -----------------------------------------------------------------------------
   Prevents duplicate fact records, which would distort metrics
============================================================================= */


/* -----------------------------------------------------------------------------
   Inventory Snapshot Grain Protection
   Grain: One row per product per store per date
----------------------------------------------------------------------------- */
CREATE UNIQUE INDEX ux_inventory_snapshot_grain
ON core.fact_inventory_snapshots (product_sk, store_sk, date_sk);


/* -----------------------------------------------------------------------------
   POS Transactions - Natural Key Protection
----------------------------------------------------------------------------- */
CREATE UNIQUE INDEX ux_pos_transaction_id
ON core.fact_pos_transactions (transaction_id);


/* -----------------------------------------------------------------------------
   E-Commerce Orders - Natural Key Protection
----------------------------------------------------------------------------- */
CREATE UNIQUE INDEX ux_ecom_order_id
ON core.fact_ecom_orders (order_id);


/* -----------------------------------------------------------------------------
   Returns - Natural Key Protection
----------------------------------------------------------------------------- */
CREATE UNIQUE INDEX ux_return_id
ON core.fact_returns (return_id);



/* =============================================================================
   PERFORMANCE INDEXES - BI & ANALYTICS OPTIMIZATION
   -----------------------------------------------------------------------------
   Designed to optimize common dashboard and reporting queries
============================================================================= */


/* -----------------------------------------------------------------------------
   Date-Based Indexes (Most Important for BI)
----------------------------------------------------------------------------- */
CREATE INDEX ix_pos_transactions_date
ON core.fact_pos_transactions (date_sk);

CREATE INDEX ix_ecom_orders_date
ON core.fact_ecom_orders (date_sk);

CREATE INDEX ix_sales_items_date
ON core.fact_sales_items (date_sk);

CREATE INDEX ix_inventory_snapshots_date
ON core.fact_inventory_snapshots (date_sk);

CREATE INDEX ix_returns_date
ON core.fact_returns (date_sk);


/* -----------------------------------------------------------------------------
   Product-Centric Analysis Indexes
----------------------------------------------------------------------------- */
CREATE INDEX ix_sales_items_product
ON core.fact_sales_items (product_sk);

CREATE INDEX ix_returns_product
ON core.fact_returns (product_sk);

CREATE INDEX ix_inventory_product
ON core.fact_inventory_snapshots (product_sk);


/* -----------------------------------------------------------------------------
   Store-Centric Analysis Indexes
----------------------------------------------------------------------------- */
CREATE INDEX ix_pos_transactions_store
ON core.fact_pos_transactions (store_sk);

CREATE INDEX ix_sales_items_store
ON core.fact_sales_items (store_sk);

CREATE INDEX ix_inventory_store
ON core.fact_inventory_snapshots (store_sk);



/* =============================================================================
   DIMENSION LOOKUP OPTIMIZATION
   -----------------------------------------------------------------------------
   Ensures fast join performance from facts to dimensions
============================================================================= */

CREATE UNIQUE INDEX ux_dim_product_nk
ON core.dim_product (product_id);

CREATE UNIQUE INDEX ux_dim_store_nk
ON core.dim_store (store_id);

CREATE UNIQUE INDEX ux_dim_date_nk
ON core.dim_date (full_date);



/* =============================================================================
   OPTIONAL: COVERING INDEXES FOR DASHBOARDS (ADVANCED)
   -----------------------------------------------------------------------------
   Optimizes high-frequency BI visuals by reducing lookup cost
============================================================================= */

/* -----------------------------------------------------------------------------
   Product Revenue & Trend Dashboards
----------------------------------------------------------------------------- */
CREATE INDEX ix_sales_items_product_date
ON core.fact_sales_items (product_sk, date_sk)
INCLUDE (quantity, line_revenue);