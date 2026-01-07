"""
Airflow ETL Project – Complete Documentation
===========================================

This file contains the FULL documentation of the Airflow ETL project
in text format, written inside a Python file for easy access, review,
and future conversion to README.md if required.
"""


PROJECT_DOCUMENTATION = """
=====================================================
📊 Airflow ETL Pipeline with Docker & PostgreSQL
=====================================================

PROJECT OVERVIEW
----------------
This project demonstrates a complete, production-style data engineering
workflow using Apache Airflow and Docker.

It covers:
- Data ingestion
- Data transformation
- Data export
- Conditional workflows
- Failure handling and cleanup

PostgreSQL is used as both:
- Airflow metadata database
- Data warehouse


ARCHITECTURE
------------
CSV File
   ↓
PostgreSQL (raw_employee_data)
   ↓
PostgreSQL (transformed_employee_data)
   ↓
Parquet File (analytics-ready)


TECH STACK
----------
- Apache Airflow 2.8.0
- Docker & Docker Compose
- PostgreSQL
- Python
- Pandas
- PyArrow (Parquet)
- Pytest


PROJECT STRUCTURE
-----------------
```
airflow-etl-project/
│
├── docker-compose.yml
├── requirements.txt
├── README.md
│
├── dags/
│   ├── dag1_csv_to_postgres.py
│   ├── dag2_data_transformation.py
│   ├── dag3_postgres_to_parquet.py
│   ├── dag4_conditional_workflow.py
│   └── dag5_notification_workflow.py
│
├── data/
│   └── input.csv
│
├── output/
│   └── parquet files
│
├── tests/
│   ├── test_dag1.py
│   ├── test_dag2.py
│   └── test_utils.py
│
├── logs/
└── plugins/
```

DAG 1 – CSV TO POSTGRES INGESTION
--------------------------------
DAG ID: csv_to_postgres_ingestion
Schedule: @daily

Purpose:
- Create raw_employee_data table
- Truncate table for idempotency
- Load CSV data into PostgreSQL

Key Concepts:
- Idempotent pipeline
- Pandas + PostgresHook
- Deterministic data loading


DAG 2 – DATA TRANSFORMATION PIPELINE
-----------------------------------
DAG ID: data_transformation_pipeline
Schedule: @daily

Transformations:
- full_info = name + " - " + city
- age_group = Young / Mid / Senior
- salary_category = Low / Medium / High
- year_joined extracted from join_date

Output:
- transformed_employee_data table


DAG 3 – POSTGRES TO PARQUET EXPORT
---------------------------------
DAG ID: postgres_to_parquet_export
Schedule: @weekly

Features:
- Source table validation
- Export to Parquet format
- Snappy compression
- Schema validation

Output directory:
- /opt/airflow/output/


DAG 4 – CONDITIONAL WORKFLOW (BRANCHING)
---------------------------------------
DAG ID: conditional_workflow_pipeline
Schedule: @daily

Logic:
- Monday–Wednesday → Weekday processing
- Thursday–Friday → End-of-week processing
- Saturday–Sunday → Weekend processing

Key Operator:
- BranchPythonOperator

Important Notes:
- Only ONE branch runs
- Other branches are SKIPPED (expected behavior)
- End task uses safe trigger rule


DAG 5 – SUCCESS / FAILURE NOTIFICATION WORKFLOW
-----------------------------------------------
DAG ID: notification_workflow
Schedule: @daily

Features:
- Risky operation that may fail
- Success callback
- Failure callback
- Cleanup task that always runs

Key Concepts:
- on_success_callback
- on_failure_callback
- trigger_rule = all_done

This demonstrates production-grade failure handling.


UNIT TESTING
------------
- Tests validate DAG structure
- DAG loading without errors
- Task counts
- Dependencies
- No cycles
- Correct schedules

Tests do NOT require Airflow to be running.


HOW TO RUN
----------
1. docker-compose up -d
2. Open http://localhost:8081
3. Login with admin / admin
4. Enable DAGs
5. Trigger DAGs manually
6. Monitor via Graph, Grid, and Logs


FINAL STATUS
------------
All DAGs executed successfully:
- DAG 1 ✅
- DAG 2 ✅
- DAG 3 ✅
- DAG 4 ✅
- DAG 5 ✅

This project represents a complete, real-world Airflow ETL pipeline
suitable for portfolio, interviews, and learning.


