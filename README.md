## Data Pipeline Specification

1. Overview

    - Pipeline Name: ecomm_orders_pipeline

    - Objective: Ingest OLTP data into S3, process through Databricks DLT (Bronze/Silver/Gold), use dbt to define aggregate metrics, and expose results to Tableau dashboards.

    - Frequency: Daily batch (01:00 ICT).