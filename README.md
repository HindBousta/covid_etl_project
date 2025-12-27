# COVID ETL Project – Airflow + dbt

## 1. Project Overview

This project implements an **end-to-end ELT data pipeline** using **Apache Airflow**, **dbt**, **PostgreSQL**, and **Docker**. The goal is to ingest raw COVID-19 data, transform it into analytics-ready tables, and orchestrate the entire workflow in a reproducible, production-oriented setup.

The architecture follows modern analytics engineering best practices:

* Raw data ingestion
* Staging transformations (cleaning & standardization)
* Analytics-ready fact and dimension models (star schema)
* Orchestration with Airflow
* Transformations with dbt

---

## 2. Tech Stack

* **Apache Airflow** – workflow orchestration
* **dbt (Data Build Tool)** – transformations, tests, documentation
* **PostgreSQL** – data warehouse
* **Docker & Docker Compose** – containerized environment
* **Python** – Airflow DAGs

---

## 3. High-Level Architecture

```
        ┌────────────┐
        │   Source   │
        │ COVID Data |
        | Census Data│
        └─────┬──────┘
              │
              ▼
     ┌──────────────────┐
     │ Raw Schema (raw) │
     │  covid_data      |
     |  census_data     │
     └─────┬────────────┘
           │ dbt source
           ▼
 ┌──────────────────────┐
 │ Staging Schema       │
 │ stg_covid_cases      |
 |  stg_census          │
 └─────┬────────────────┘
       │ dbt ref
       ▼
 ┌──────────────────────┐
 │ Mart Schema (mart)   │
 │ Fact & Dimensions    |
 |                      |
 └──────────────────────┘

Airflow orchestrates ingestion and triggers dbt runs inside a dedicated dbt container.
```

---

## 4. Repository Structure

```
covid_etl_project/
│
├── dags/                     # Airflow DAGs
│   └── combined_etl_dag.py
|   └──tasks
|   │    └── census_tasks.py
|   │    └── covid_tasks.py
│
├── dbt/
│   └── covid_project/
│       ├── models/
│       │   ├── staging/
│       │   │   └── stg_covid_cases.sql
│       │   │   └── stg_census.sql
│       │   └── marts/         # Fact & dimension models
│       │   │   └── dimensions
│       │   │   |   └── dim_date.sql
│       │   │   |   └── dim_state.sql
│       │   │   └── facts
│       │   │   |   └── fct_covid_daily_metrics.sql
│       │   ├── staging/
│       │   │   └── src_covid.sql
│       ├── tests/
│       ├── macros/
│       ├── dbt_project.yml
│       └── profiles.yml
│
├── docker-compose.yml
└── README.md
```

---

## 5. Airflow DAG

The Airflow DAG orchestrates three main steps:

1. **Data ingestion of Covid data** into PostgreSQL (raw schema)
2. **Data ingestion of Census data** into PostgreSQL (raw schema)
3. **dbt transformations** 

The dbt transformations are executed inside the Airflow container. 
---

## 6. dbt Project Design

### 6.1 Sources

Raw data is declared as a dbt source:

```yaml
sources:
  - name: raw
    schema: raw
    tables:
      - name: covid_data
```

---

### 6.2 Staging Models

Staging models:

* Clean column names
* Cast data types
* Filter invalid records
* Apply minimal business logic

**Example: `stg_covid_cases.sql`**

```sql
{{ config(materialized='table') }}

SELECT
    date::date              AS case_date,
    states                  AS state_code,
    positive                AS positive_cases,
    negative                AS negative_cases,
    hospitalizedCurrently   AS currently_hospitalized,
    death                   AS total_deaths
FROM {{ source('raw', 'covid_data') }}
WHERE date IS NOT NULL
  AND states IS NOT NULL
```

Schema configuration is handled centrally in `dbt_project.yml`:

```yaml
models:
  covid_project:
    staging:
      +schema: staging
      +materialized: table
```

---

### 6.3 Mart Models (Phase 2)

The mart layer implements a **star schema**:

* **Fact table**: daily COVID metrics
* **Dimension tables**: date, state

Fact models are **incremental** and use composite keys:

```yaml
unique_key: ['date_key', 'state_key']
```

Incremental logic is date-based, assuming daily immutable snapshots with optional lookback windows for late-arriving data.

---

## 7. Incremental Strategy

Fact tables use dbt incremental models with:

* Composite `unique_key`
* Date-based filtering
* Optional lookback window for safety

Example:

```sql
{% if is_incremental() %}
WHERE case_date >= (
  SELECT max(date_key) - interval '1 day'
  FROM {{ this }}
)
{% endif %}
```

This ensures:

* Idempotent runs
* High performance
* Protection against small data corrections

---

## 8. Data Quality

dbt tests are used to validate data quality:

* `not_null` tests
* `unique` tests
* Relationship tests (fact ↔ dimensions)

Tests are executed automatically after `dbt run`.

---

## 9. Permissions & Containers

Key design decisions:

* Airflow runs as a **non-root user** (security best practice)
* dbt project initialized **inside the container** to avoid permission issues
* Volumes are mounted so changes are reflected immediately

---

## 10. How to Run the Project

```bash
docker-compose up -d --build
```

Access:

* Airflow UI: [http://localhost:8080](http://localhost:8080)
* PostgreSQL via psql or client

Trigger the DAG from Airflow to execute the full pipeline.

---

## 11. Current Status

✔ Raw ingestion implemented
✔ Staging model built and tested
✔ dbt executed via Airflow
✔ Incremental fact design started
✔ Permissions and containerization resolved

---

## 12. Key Learning Outcomes

This project demonstrates:

* Modern ELT architecture
* Analytics engineering best practices
* dbt incremental modeling
* Airflow–dbt integration
* Real-world containerized workflows
