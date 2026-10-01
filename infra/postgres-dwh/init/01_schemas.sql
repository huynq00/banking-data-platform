-- =============================================================
-- Data Warehouse schemas (medallion layers inside the warehouse)
-- Runs automatically on the FIRST start of an empty volume.
-- =============================================================

-- raw:    bronze data copied 1:1 from the lake, untouched
CREATE SCHEMA IF NOT EXISTS raw;

-- silver: cleaned, deduplicated, PII-masked (built by dbt, week 4)
CREATE SCHEMA IF NOT EXISTS silver;

-- gold:   star schema & KPIs for dashboards (built by dbt, week 5)
CREATE SCHEMA IF NOT EXISTS gold;
