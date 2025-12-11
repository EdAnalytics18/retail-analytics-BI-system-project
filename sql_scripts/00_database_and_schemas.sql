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
   - core      - Conformed dimensions & fact tables (snowflake schema)
   - analytics - BI-ready views (reporting & dashboards)
============================================================================= */

CREATE DATABASE retail_dw;
GO

USE retail_dw;
GO

/* -----------------------------------------------------------------------------
   Create logical schemas to separate responsibilities
----------------------------------------------------------------------------- */
CREATE SCHEMA staging;
GO
CREATE SCHEMA core;
GO
CREATE SCHEMA analytics;
GO
