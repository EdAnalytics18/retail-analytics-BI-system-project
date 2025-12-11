/* =============================================================================
   DATA INGESTION - CSV BULK LOAD
   -----------------------------------------------------------------------------
   Purpose:
   - Load external CSV extracts into raw staging tables
   - Simulate real enterprise file-based ingestion
   -----------------------------------------------------------------------------
   Notes:
   - FIRSTROW = 2 removes headers
   - KEEPNULLS preserves missing values
   - FILE PATHS are environment-specific
============================================================================= */
BULK INSERT staging.pos_transactions_raw
FROM 'C:\Retail Analytics BI System Project\raw_data\pos_transactions_raw.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    FORMAT = 'CSV',
    TABLOCK,
    KEEPNULLS
);
GO

BULK INSERT staging.pos_items_raw
FROM 'C:\Retail Analytics BI System Project\raw_data\pos_order_items_raw.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    FORMAT = 'CSV',
    TABLOCK,
    KEEPNULLS
);
GO

BULK INSERT staging.ecom_orders_raw
FROM 'C:\Retail Analytics BI System Project\raw_data\ecom_orders_raw.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    FORMAT = 'CSV',
    TABLOCK,
    KEEPNULLS
);
GO

BULK INSERT staging.ecom_items_raw
FROM 'C:\Retail Analytics BI System Project\raw_data\ecom_order_items_raw.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    FORMAT = 'CSV',
    TABLOCK,
    KEEPNULLS
);
GO

BULK INSERT staging.inventory_snapshots_raw
FROM 'C:\Retail Analytics BI System Project\raw_data\inventory_raw.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    FORMAT = 'CSV',
    TABLOCK,
    KEEPNULLS
);
GO

BULK INSERT staging.returns_raw
FROM 'C:\Retail Analytics BI System Project\raw_data\returns_raw.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    FORMAT = 'CSV',
    TABLOCK,
    KEEPNULLS
);
GO

BULK INSERT staging.products_raw
FROM 'C:\Retail Analytics BI System Project\raw_data\products_raw.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    FORMAT = 'CSV',
    TABLOCK,
    KEEPNULLS
);
GO

BULK INSERT staging.stores_raw
FROM 'C:\Retail Analytics BI System Project\raw_data\stores_raw.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0A',
    FORMAT = 'CSV',
    TABLOCK,
    KEEPNULLS
);
GO