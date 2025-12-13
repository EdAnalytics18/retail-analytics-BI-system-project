/* =============================================================================
   RETAIL ANALYTICS DATA WAREHOUSE
   -----------------------------------------------------------------------------
   Project: Retail Analytics BI System
   Author: Ed Hideaki
   Platform: SQL Server
   -----------------------------------------------------------------------------
   Purpose:
   - Initialize the Retail Data Warehouse environment
   - Create layered architecture following modern analytics best practices
   -----------------------------------------------------------------------------
   Architecture Layers:
   - staging   - Raw and cleaned source data (ELT pattern)
   - core      - Conformed dimension & fact tables (star / snowflake schema)
   - analytics - BI-ready views (reporting & dashboards)
============================================================================= */

IF DB_ID('retail_dw') IS NULL
BEGIN
    CREATE DATABASE retail_dw;
END
GO

USE retail_dw;
GO


/* -----------------------------------------------------------------------------
   Create logical schemas to separate responsibilities
----------------------------------------------------------------------------- */
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'core')
    EXEC('CREATE SCHEMA core');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'analytics')
    EXEC('CREATE SCHEMA analytics');
GO


