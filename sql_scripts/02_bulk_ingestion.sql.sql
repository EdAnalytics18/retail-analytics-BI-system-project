/* =============================================================================
   DATA INGESTION - CSV BULK LOAD (BRONZE / RAW LAYER)
   -----------------------------------------------------------------------------
   Purpose:
   - Load external CSV extracts into raw staging tables
   - Simulate real enterprise file-based ingestion pipelines
   - Ensure idempotent loads by truncating tables before ingestion
   -----------------------------------------------------------------------------
   Notes:
   - FIRSTROW = 2 removes CSV headers
   - KEEPNULLS preserves missing values for data quality analysis
   - FILE PATHS are environment-specific and centralized below
============================================================================= */


/* =============================================================================
   INGESTION CONFIGURATION
   -----------------------------------------------------------------------------
    UPDATE THIS BASE PATH TO MATCH YOUR LOCAL ENVIRONMENT
   Examples:
     Windows: C:\Retail Analytics BI System Project\raw_data\
============================================================================= */

DECLARE @base_path VARCHAR(255)
    = 'C:\Retail Analytics BI System Project\raw_data\';


/* =============================================================================
   POS TRANSACTIONS (HEADER LEVEL)
============================================================================= */
TRUNCATE TABLE staging.pos_transactions_raw;

EXEC (
    'BULK INSERT staging.pos_transactions_raw
     FROM ''' + @base_path + 'pos_transactions_raw.csv''
     WITH (
         FIRSTROW = 2,
         FIELDTERMINATOR = '','',
         ROWTERMINATOR = ''0x0A'',
         FORMAT = ''CSV'',
         TABLOCK,
         KEEPNULLS
     );'
);
GO


/* =============================================================================
   POS ORDER ITEMS (LINE LEVEL)
============================================================================= */
TRUNCATE TABLE staging.pos_items_raw;

EXEC (
    'BULK INSERT staging.pos_items_raw
     FROM ''' + @base_path + 'pos_order_items_raw.csv''
     WITH (
         FIRSTROW = 2,
         FIELDTERMINATOR = '','',
         ROWTERMINATOR = ''0x0A'',
         FORMAT = ''CSV'',
         TABLOCK,
         KEEPNULLS
     );'
);
GO


/* =============================================================================
   E-COMMERCE ORDERS
============================================================================= */
TRUNCATE TABLE staging.ecom_orders_raw;

EXEC (
    'BULK INSERT staging.ecom_orders_raw
     FROM ''' + @base_path + 'ecom_orders_raw.csv''
     WITH (
         FIRSTROW = 2,
         FIELDTERMINATOR = '','',
         ROWTERMINATOR = ''0x0A'',
         FORMAT = ''CSV'',
         TABLOCK,
         KEEPNULLS
     );'
);
GO


/* =============================================================================
   E-COMMERCE ORDER ITEMS
============================================================================= */
TRUNCATE TABLE staging.ecom_items_raw;

EXEC (
    'BULK INSERT staging.ecom_items_raw
     FROM ''' + @base_path + 'ecom_order_items_raw.csv''
     WITH (
         FIRSTROW = 2,
         FIELDTERMINATOR = '','',
         ROWTERMINATOR = ''0x0A'',
         FORMAT = ''CSV'',
         TABLOCK,
         KEEPNULLS
     );'
);
GO


/* =============================================================================
   INVENTORY SNAPSHOTS
============================================================================= */
TRUNCATE TABLE staging.inventory_snapshots_raw;

EXEC (
    'BULK INSERT staging.inventory_snapshots_raw
     FROM ''' + @base_path + 'inventory_raw.csv''
     WITH (
         FIRSTROW = 2,
         FIELDTERMINATOR = '','',
         ROWTERMINATOR = ''0x0A'',
         FORMAT = ''CSV'',
         TABLOCK,
         KEEPNULLS
     );'
);
GO


/* =============================================================================
   RETURNS
============================================================================= */
TRUNCATE TABLE staging.returns_raw;

EXEC (
    'BULK INSERT staging.returns_raw
     FROM ''' + @base_path + 'returns_raw.csv''
     WITH (
         FIRSTROW = 2,
         FIELDTERMINATOR = '','',
         ROWTERMINATOR = ''0x0A'',
         FORMAT = ''CSV'',
         TABLOCK,
         KEEPNULLS
     );'
);
GO


/* =============================================================================
   PRODUCTS MASTER DATA
============================================================================= */
TRUNCATE TABLE staging.products_raw;

EXEC (
    'BULK INSERT staging.products_raw
     FROM ''' + @base_path + 'products_raw.csv''
     WITH (
         FIRSTROW = 2,
         FIELDTERMINATOR = '','',
         ROWTERMINATOR = ''0x0A'',
         FORMAT = ''CSV'',
         TABLOCK,
         KEEPNULLS
     );'
);
GO


/* =============================================================================
   STORES MASTER DATA
============================================================================= */
TRUNCATE TABLE staging.stores_raw;

EXEC (
    'BULK INSERT staging.stores_raw
     FROM ''' + @base_path + 'stores_raw.csv''
     WITH (
         FIRSTROW = 2,
         FIELDTERMINATOR = '','',
         ROWTERMINATOR = ''0x0A'',
         FORMAT = ''CSV'',
         TABLOCK,
         KEEPNULLS
     );'
);
GO
