/* =============================================================================
   RETAIL ANALYTICS DATA WAREHOUSE
   -----------------------------------------------------------------------------
   Project: Retail Analytics Business Intelligence System
   Platform: Microsoft SQL Server
   -----------------------------------------------------------------------------
   Executive Overview:
   This script initializes the foundational environment for a modern Retail
   Analytics Data Warehouse. It establishes a clean, scalable structure that
   transforms raw transactional data into reliable, decision-ready insights.

   By separating data into clearly defined layers, this architecture ensures:
   - High data quality and traceability
   - Consistent metrics across teams (Finance, Marketing, Operations)
   - Faster, safer reporting for dashboards and analytics
   - A strong foundation for future growth (new data sources, BI tools, ML)

   -----------------------------------------------------------------------------
   Business Value:
   - Executives gain trustworthy KPIs for revenue, margin, and customer behavior
   - Analysts work with clean, well-modeled data instead of raw exports
   - Engineering avoids duplicated logic and reporting inconsistencies
   - The organization can scale analytics without reworking core logic

   -----------------------------------------------------------------------------
   Data Architecture (Layered Design):
   This warehouse follows modern analytics best practices used by high-performing
   data teams.

   1) staging
      - Landing zone for raw and lightly cleaned source data
      - Preserves original records for auditability and debugging
      - Supports ELT workflows commonly used in cloud and enterprise analytics

   2) core
      - Conformed dimension and fact tables
      - Enforces a single source of truth for business entities
        (customers, products, orders, dates, stores)
      - Optimized for analytical accuracy and long-term stability

   3) analytics
      - Business-ready views designed for BI tools and stakeholders
      - Encapsulates complex logic into reusable, trusted metrics
      - Enables fast dashboard development and self-serve analytics

   -----------------------------------------------------------------------------
   Outcome:
   This structure allows the business to move confidently from raw data
   to insights - without sacrificing accuracy, performance, or governance.
============================================================================= */

-- Create the Retail Data Warehouse database if it does not already exist
-- This ensures the script is safe to run multiple times (idempotent setup)
IF DB_ID('retail_dw') IS NULL
BEGIN
    CREATE DATABASE retail_dw;
END
GO

USE retail_dw;
GO


/* -----------------------------------------------------------------------------
   Create logical schemas to clearly separate responsibilities
   -----------------------------------------------------------------------------
   Why this matters:
   - Improves data governance and security
   - Makes the warehouse easier to understand for new team members
   - Aligns with enterprise BI and analytics engineering standards
----------------------------------------------------------------------------- */

-- Staging schema: raw and cleaned source data
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'staging')
    EXEC('CREATE SCHEMA staging');
GO

-- Core schema: trusted dimensional model (facts & dimensions)
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'core')
    EXEC('CREATE SCHEMA core');
GO

-- Analytics schema: BI-ready views for dashboards and reporting
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'analytics')
    EXEC('CREATE SCHEMA analytics');
GO
