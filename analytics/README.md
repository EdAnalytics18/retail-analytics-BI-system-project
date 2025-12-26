#  Analytics Layer

This folder contains **business-facing analytics artifacts** built on top of the
Retail Analytics BI System.

The analytics layer represents the **semantic and consumption layer** of the
warehouse, translating conformed fact and dimension tables into clear,
reusable insights for stakeholders.

---

##  Contents

### `business_questions.md`
Defines the core business questions supported by the data warehouse, aligned
with real-world use cases across Retail, E-commerce, Finance, and Operations
teams.

---

### `metrics_definitions.md`
Provides formal definitions for key business KPIs, including formulas, source
tables, and calculation logic to ensure consistent and trustworthy metrics
across dashboards and ad-hoc analysis.

---

### `sample_queries.sql`
Contains example SQL queries demonstrating how business questions are answered
using the dimensional model. These queries illustrate how facts and dimensions
work together to support common retail analytics use cases.

> **Note:** In production environments, equivalent logic is typically exposed
> through analytics-layer (Gold) views and consumed directly by BI tools.

---

##  Purpose

The analytics layer enables:

- Consistent KPI definitions across BI tools
- Faster dashboard development
- Clear traceability from business questions to data
- Self-service analytics for stakeholders
