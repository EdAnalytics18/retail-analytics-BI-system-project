#  Dashboards

This folder contains **business intelligence dashboards** built on top of the
Retail Analytics BI System.

Dashboards consume **analytics-layer (Gold) views** to ensure consistent KPI
definitions and avoid duplicating business logic inside BI tools.

---

##  Purpose

The dashboards are designed to demonstrate how the data warehouse supports:

- Executive-level KPI monitoring
- Revenue and channel performance analysis
- Product and category insights
- Inventory health and operational efficiency
- Returns and revenue leakage analysis

---

##  Tools

Dashboards are designed to be compatible with common BI tools, including:

- **Power BI**
- **Tableau**
- **SQL-based exploratory analysis**

---

##  Planned Dashboard Examples

Typical dashboards built on this system include:

- **Executive KPI Overview**
  - Net Revenue
  - Average Order Value (AOV)
  - Units Sold
  - Return Rate

- **Revenue & Channel Performance**
  - POS vs E-commerce trends
  - Monthly and YoY revenue growth
  - Channel contribution analysis

- **Product & Inventory Performance**
  - Top and bottom products by revenue and margin
  - Inventory turnover and stock-out risk
  - Safety stock monitoring

- **Returns & Customer Experience**
  - Return rate by product and category
  - Refund impact on net revenue
  - High-risk products and channels

---

##  Notes

- Dashboard files (e.g., `.pbix`, `.twbx`) or exported images may be added to this
  folder as development progresses.
- All dashboard logic is derived from **analytics-layer views** to ensure
  consistency across reporting and ad-hoc analysis.

---

##  Relationship to Analytics Layer

This folder represents the **final consumption layer** of the analytics system.

Business questions and KPI definitions are documented in:
- `analytics/business_questions.md`
- `analytics/metrics_definitions.md`

Sample SQL used to derive insights is available in:
- `analytics/sample_queries.sql`
