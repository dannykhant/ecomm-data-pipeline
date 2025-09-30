# Runbook: ecomm_data_pipeline

## 1. Overview
- **Pipeline Name**: `ecomm_data_pipeline`
- **Description**: ETL/ELT pipeline for ecommerce analytics of {company_name}
- **Objective**: Ingest OLTP → S3 → Databricks DLT (Bronze/Silver/Gold) → dbt metrics → Tableau dashboards
- **Frequency**: Daily batch (01:00 ICT)
- **Owner**: Data Engineering Team
- **Contact**: #data-eng Slack / data-eng@company.com

---

## 2. Normal Execution Flow
1. **Airflow DAG (`batch_ecomm_pipeline`)** triggers extract:
   - Postgres tables (`orders`, `products`, `customers`, `order_items`) exported → S3 (`/ecomm_db_extracts/daily_extracts`).
2. **DLT Pipeline** runs:
   - Bronze: Load raw CSV with schema evolution (rescue mode).
   - Silver: Cast types, enforce PKs, enrich with processed timestamp. Builds `fact_orders`, `dim_orders`, `dim_customer`, `dim_product`.
   - Gold: Create analytics & aggregations (`sales_analytics`, `agg_daily_sales`).
3. **dbt Job (`DailyRefresh`)** runs at 05:00 ICT:
   - Aggregates business metrics (`agg_sales_kpi`, `agg_product_kpi`, `agg_customer_kpi`).
4. **Tableau** extracts refresh → dashboards updated.

**Expected Duration**:  
- Extraction + DLT pipeline: ~2–2.5 hrs  
- dbt job: ~20 mins  
- Tableau refresh: <30 mins

---

## 3. Monitoring
- **DLT**: Monitor via Databricks pipeline event logs; notifications configured for failure.
- **dbt**: Review run logs/artifacts in dbt Cloud (`DailyRefresh` job).
- **Airflow**: DAG status and task logs.
- **Tableau**: Extract refresh history.

---

## 4. Common Issues & Troubleshooting

### A. Extraction (Airflow → S3)
- **Symptom**: Missing or empty file in S3.
- **Steps**:
  1. Check Airflow task logs (`extract_postgres_to_s3`).
  2. Verify Postgres connectivity and `updated_at` watermark logic.
  3. Manually trigger backfill using Airflow with custom `execution_date`.

### B. Bronze Layer (DLT)
- **Symptom**: Pipeline fails with schema mismatch.
- **Steps**:
  1. Open DLT logs → check schema evolution warnings.
  2. Confirm S3 file schema matches Postgres export.
  3. If new column → confirm schema evolution handles via rescue.

### C. Silver Layer (DLT)
- **Symptom**: Records dropped unexpectedly.
- **Steps**:
  1. Check expectations for PK validity.
  2. Confirm `updated_at` logic and deduplication rules.
  3. Validate timestamps standardized to UTC.

### D. Gold Layer (DLT)
- **Symptom**: Missing or incorrect `sales_analytics` or `agg_daily_sales`.
- **Steps**:
  1. Inspect upstream Silver tables (`fact_orders`, `dim_orders`).
  2. Re-run DLT pipeline manually if partial failure.

### E. dbt Metrics
- **Symptom**: dbt job `DailyRefresh` fails.
- **Steps**:
  1. Check dbt Cloud run logs.
  2. Validate Gold tables (`sales_analytics`, `agg_daily_sales`) exist.
  3. Run model selectively: `dbt run -m agg_sales_kpi`.

### F. Tableau
- **Symptom**: Dashboard shows stale data.
- **Steps**:
  1. Check Tableau extract refresh status.
  2. Trigger manual refresh.
  3. Confirm dbt metric tables populated in Databricks SQL Warehouse.

---

## 5. Escalation
- **Priority 1 (Pipeline Down / No Data in Tableau)**:
  - Notify #data-eng Slack immediately.
  - Escalate to On-call DE (PagerDuty).
- **Priority 2 (Partial Data / Missing KPIs)**:
  - File Jira ticket.
  - Notify Data Analytics team.
- **Priority 3 (Minor Schema Drift / Cosmetic Errors)**:
  - Add to backlog for next sprint.

---

## 6. Recovery Procedures
- **Re-run Full DAG**: Trigger `batch_ecomm_pipeline` in Airflow.
- **Backfill Specific Date**: Provide `execution_date` param in Airflow DAG.
- **Re-run dbt Job**: Trigger `DailyRefresh` manually from dbt Cloud.
- **Re-run Tableau Extract**: Trigger manual extract refresh in Tableau Server.

---

## 7. References
- [Airflow DAG UI](https://airflow.company.com)
- [Databricks DLT Pipelines](https://databricks.com)
- [dbt Cloud](https://cloud.getdbt.com)
- [Tableau Server](https://tableau.company.com)
