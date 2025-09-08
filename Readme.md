# Data Warehouse with Medallion Architecture in PostgreSQL

## Overview

This project is the implementation of a **Data Warehouse** using the **Medallion Architecture** (🟫 Bronze, ⚪ Silver, 🟨 Gold) on a **PostgreSQL** database. It successfully ingests, processes, and models data through the three layers of the architecture. The final Gold layer is organized using a **Star Schema**, enabling efficient analytical queries.

---

## Architecture

### Medallion Architecture Layers

1. **🟫 Bronze Layer (Raw Data)**
   - Ingests raw data from raw sources(form csv format) without transformation.
   - Stores the data in its original form for traceability and reprocessing.

2. **⚪ Silver Layer (Cleaned and Enriched Data)**
   - Transforms raw data into cleaned and structured formats.
   - Applies business rules, filtering, and joins to prepare data for analytics.

3. **🟨 Gold Layer (Business-Focused, Analytical Data)**
   - Contains aggregated and curated data modeled for business intelligence.
   - Follows the **Star Schema** for dimensional modeling to support reporting and analytical workloads.

---

## Database: PostgreSQL

All layers are implemented using **PostgreSQL**. Schemas are logically separated within the database:

- **bronze**: Stores raw ingested data
- **silver**: Stores cleaned and enriched data
- **gold**: Stores dimensional and fact tables in a star schema format

---

## Star Schema in Gold Layer

The **🟨 Gold Layer** uses a classic **Star Schema** model:

### Fact Table
- `fact_sales` (e.g., transactional or aggregated business metrics)

### Dimension Tables
- `dim_customers`
- `dim_products`

This design improves query performance and supports business intelligence tools for reporting and dashboards.

---

## Features

- Full ETL pipeline with clear separation between raw, cleaned, and business-ready data.
- Reproducible data flow for auditing and debugging.
- Designed for scalability and ease of maintenance.
- Query-optimized Gold layer with Star Schema for analytical use cases.

---

## How to Use

1. **Set up PostgreSQL** and create schemas for **bronze**, **silver**, and **gold**.
2. **Ingest data** into the Bronze layer.
3. **Transform data** using SQL or ETL tools into the  Silver layer.
4. **Model data** into facts and dimensions in the Gold layer.
5. **Run analytics** or connect  BI tools to the Gold layer.

---

## Future Enhancements

- Implement automated data quality checks
- Add versioning and audit logging for data lineage
- Support full load and real-time data loading

---

## Conclusion

This project showcases a practical implementation of the **Medallion Architecture** for modern data warehousing using **PostgreSQL**, ending with a performant and structured **Star Schema** in the Gold layer for business insights and analytics.
