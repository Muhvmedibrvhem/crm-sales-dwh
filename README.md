# CRM Sales Opportunities Data Warehouse

An end-to-end data warehouse built on the Maven Analytics *CRM Sales Opportunities* dataset.
The project designs a dimensional model (conceptual, logical and physical schemas) and implements the ETL with **Airflow + dbt + DuckDB**.

- **Author:** Mohamed Ibrahem
- **Program:** DEPI, Microsoft Data Engineer track (R5)
- **Date:** 2026-10-09
- **Dataset:** https://mavenanalytics.io/data-playground/crm-sales-opportunities

---

## 1. Overview

The source data describes the B2B sales pipeline of a hardware company: which sales agent worked which opportunity, for which account, on which product, and how far the deal got.

The goal is to turn four flat CSV files into a star schema that makes pipeline questions easy to answer, for example win rate by agent, revenue by product series, or average sales cycle by sector.

| Tool | Role | Version |
|------|------|---------|
| DuckDB | Analytical database (single file `crm.duckdb`) | 1.5.6 |
| dbt-core + dbt-duckdb | SQL transformations, tests and lineage | 1.12.5 / 1.11.0 |
| Apache Airflow | Orchestration of the load, build and test steps | 2.10.5 (Docker) |
| Python | Raw data loading script | 3.11 |

## 2. Source data

Five CSV files in `data/raw/`:

| File | Rows | Description |
|------|------|-------------|
| `accounts.csv` | 85 | Customer accounts: sector, year established, revenue, employees, office location, parent company |
| `products.csv` | 7 | Products: series and list price |
| `sales_teams.csv` | 35 | Sales agents with their manager and regional office |
| `sales_pipeline.csv` | 8,800 | One row per sales opportunity (the transactional table) |
| `data_dictionary.csv` | 21 | Field descriptions provided with the dataset |

### Profiling findings

These findings drove the design decisions in section 4.4.

| Deal stage | Opportunities | Missing account | Missing engage date | Missing close date |
|------------|---------------|-----------------|---------------------|--------------------|
| Won | 4,238 | 0 | 0 | 0 |
| Lost | 2,473 | 0 | 0 | 0 |
| Engaging | 1,589 | 1,088 | 0 | 1,589 |
| Prospecting | 500 | 337 | 500 | 500 |

- Open deals (Engaging, Prospecting) naturally have no close date, and many have no account yet. In total 1,425 opportunities have no account.
- The product name `GTXPro` in `sales_pipeline` has no exact match in `products`, so a plain join would silently drop those deals.

## 3. Architecture

```
 CSV files            DuckDB (crm.duckdb)
 data/raw   ──load──▶  raw      ──dbt──▶  staging  ──dbt──▶  marts
                       (as-is)           (views:            (tables:
                                         cleaning)          dimensions + fact)

 Orchestrated by Airflow DAG  crm_sales_etl:
     load_raw_data  ──▶  dbt_run  ──▶  dbt_test
```

| Layer | Schema | Materialization | Purpose |
|-------|--------|-----------------|---------|
| Raw | `raw` | table | Exact copy of the CSV files, no changes |
| Staging | `staging` | view | Trim text, add a normalized product key |
| Marts | `marts` | table | Final dimensions and the fact table |

## 4. Dimensional model

**Grain of the fact table:** one row per sales opportunity (`opportunity_id`).

### 4.1 Conceptual schema

The business entities and how they relate, without attributes or data types.

```
                    +-----------+
                    |  Account  |
                    +-----+-----+
                          | 1
                          | N
+-------------+ 1     N +-+-----------+ N     1 +-----------+
|    Date     +---------+   Sales     +---------+  Product  |
| engage/close|         | Opportunity |         +-----------+
+-------------+         +------+------+
                               | N
                               | 1
                        +------+------+
                        | Sales Agent |
                        | (manager,   |
                        |  office)    |
                        +-------------+
```

- An account can have many opportunities. An opportunity may not have an account yet.
- A product appears in many opportunities.
- A sales agent works many opportunities and belongs to one manager and one regional office.
- An opportunity has an engage date and a close date. Both can be missing while the deal is still open.

Diagram image: `docs/conceptual_schema.png`

### 4.2 Logical schema

Tables, attributes and keys, independent of the database engine.

**fact_sales_pipeline**

| Column | Type | Key | Notes |
|--------|------|-----|-------|
| opportunity_id | text | PK | Degenerate dimension from the source |
| agent_key | number | FK to dim_sales_agent | |
| product_key | number | FK to dim_product | |
| account_key | number | FK to dim_account | |
| engage_date_key | number | FK to dim_date | |
| close_date_key | number | FK to dim_date | |
| deal_stage | text | | Won, Lost, Engaging, Prospecting |
| close_value | number | | Measure: deal value recorded at close |
| sales_cycle_days | number | | Derived: days from engage to close, closed deals only |
| is_won | boolean | | Derived: true when deal_stage is Won |

**dim_sales_agent**: agent_key (PK), sales_agent, manager, regional_office

**dim_product**: product_key (PK), product, series, sales_price

**dim_account**: account_key (PK), account, sector, year_established, revenue, employees, office_location, subsidiary_of

**dim_date**: date_key (PK), full_date, day, month, month_name, quarter, year

Diagram image: `docs/logical_schema.png`

### 4.3 Physical schema

The implementation in DuckDB, schema `marts`.

| Table | Column | DuckDB type | Constraint |
|-------|--------|-------------|------------|
| dim_sales_agent | agent_key | INTEGER | PK |
| | sales_agent | VARCHAR | not null |
| | manager | VARCHAR | |
| | regional_office | VARCHAR | |
| dim_product | product_key | INTEGER | PK |
| | product | VARCHAR | not null |
| | series | VARCHAR | |
| | sales_price | BIGINT | |
| dim_account | account_key | INTEGER | PK |
| | account | VARCHAR | not null |
| | sector | VARCHAR | |
| | year_established | BIGINT | |
| | revenue | DOUBLE | |
| | employees | BIGINT | |
| | office_location | VARCHAR | |
| | subsidiary_of | VARCHAR | |
| dim_date | date_key | INTEGER | PK (format YYYYMMDD) |
| | full_date | DATE | |
| | day, month, quarter, year | INTEGER | |
| | month_name | VARCHAR | |
| fact_sales_pipeline | opportunity_id | VARCHAR | PK |
| | agent_key, product_key, account_key | INTEGER | FK, not null |
| | engage_date_key, close_date_key | INTEGER | FK, not null |
| | deal_stage | VARCHAR | not null |
| | close_value | BIGINT | |
| | sales_cycle_days | INTEGER | |
| | is_won | BOOLEAN | |

Keys and relationships are enforced by dbt tests (section 6), because dbt builds the tables with `CREATE TABLE AS` and does not declare database-level constraints.

Diagram image: `docs/physical_schema.png`

**Logical vs physical.** In a star schema the table structure is the same at both levels, because the logical design already decides which tables, columns and keys exist. The physical level adds everything the database needs to store the data:

| Aspect | Logical schema | Physical schema |
|--------|----------------|-----------------|
| Data types | Generic: `text`, `number`, `date`, `boolean` | DuckDB types: `VARCHAR`, `INTEGER`, `BIGINT`, `DOUBLE`, `DATE`, `BOOLEAN` |
| Nullability | Not specified | `NOT NULL` on keys and on `deal_stage`, `sales_agent`, `product`, `account` |
| Location | Entities only | Tables live in schema `marts` of `crm.duckdb`, built from the `staging` views over the `raw` tables |
| Storage | Not specified | Materialized as tables by dbt |
| Key generation | Surrogate keys exist | Generated with `row_number()` in dbt, with `-1` reserved for the `Unknown` row |
| Integrity | Relationships drawn between tables | Enforced by 15 dbt tests (unique, not null, relationships) |

### 4.4 Design decisions

- **Star schema.** One fact table surrounded by four dimensions keeps queries simple and fast. The sales team table is merged into the agent dimension because each agent has exactly one manager and office, so a separate dimension would add joins without adding information.
- **Surrogate keys.** Dimensions use integer keys generated in dbt instead of names, because names can change or repeat.
- **Unknown members.** Every dimension has a row with key `-1` named `Unknown`. Deals without an account, or without dates, point to it. This keeps all 8,800 opportunities in the fact table and avoids NULL foreign keys.
- **Role-playing date dimension.** `engage_date_key` and `close_date_key` both reference `dim_date`, so one date table plays two roles.
- **Degenerate dimensions.** `opportunity_id` and `deal_stage` stay in the fact table, since `deal_stage` has only four values.
- **Derived measures.** `sales_cycle_days` is calculated only for Won and Lost deals, because the duration of an open deal has no meaning. `is_won` makes win-rate calculations a simple average.
- **Product name normalization.** The staging layer adds `product_match_key` (uppercase, spaces removed) to both the products and pipeline models. The fact table joins on this key, so `GTXPro` is matched without hard-coding any name, while the displayed product name still comes from `dim_product`.

## 5. ETL pipeline

### 5.1 Raw load

`scripts/load_raw_data.py` reads every CSV in `data/raw/` and creates `raw.<file_name>` with `CREATE OR REPLACE TABLE`, so it can be re-run safely.

### 5.2 dbt models

```
dbt_project/models/
├─ staging/
│  ├─ _sources.yml            declares the raw tables
│  ├─ stg_accounts.sql
│  ├─ stg_products.sql
│  ├─ stg_sales_teams.sql
│  └─ stg_sales_pipeline.sql
└─ marts/
   ├─ dim_sales_agent.sql
   ├─ dim_product.sql
   ├─ dim_account.sql
   ├─ dim_date.sql
   ├─ fact_sales_pipeline.sql
   └─ _marts.yml              tests
```

`macros/generate_schema_name.sql` overrides dbt's default naming so schemas are named exactly `staging` and `marts` instead of `main_staging` and `main_marts`.

### 5.3 Airflow DAG

`dags/crm_etl_dag.py` defines the DAG `crm_sales_etl` with three sequential tasks:

| Task | Command | Purpose |
|------|---------|---------|
| load_raw_data | `python scripts/load_raw_data.py` | Reload the raw layer |
| dbt_run | `dbt run` | Build staging views and marts tables |
| dbt_test | `dbt test` | Validate the model |

The DAG has no schedule and is triggered manually. dbt runs in its own Python virtual environment inside the container, because Airflow and dbt have conflicting dependencies.

Screenshot of a successful run: `docs/airflow_dag.png`

## 6. Data quality

`dbt build` runs 9 models and 15 tests, all passing:

- `unique` and `not_null` on every dimension key and on `opportunity_id` (10 tests).
- `relationships` on all five foreign keys of the fact table, proving that each key resolves to a row in its dimension (5 tests).

## 7. Project structure

```
crm_dwh/
├─ data/raw/                    source CSV files
├─ scripts/
│  ├─ load_raw_data.py          raw load into DuckDB
│  └─ inspect_raw.py            profiling queries used in section 2
├─ dbt_project/                 dbt project (models, macros, profiles.yml)
├─ dags/crm_etl_dag.py          Airflow DAG
├─ docs/                        schema diagrams and screenshots
├─ Dockerfile
├─ docker-compose.yml
├─ .dockerignore
├─ requirements.txt
└─ README.md
```

## 8. How to run

### Option A: local (no Docker)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

python scripts/load_raw_data.py
cd dbt_project
dbt build
```

The script creates `crm.duckdb` in the project root. `dbt build` must run from inside `dbt_project`, because `profiles.yml` lives there and points to `../crm.duckdb`.

### Option B: Airflow in Docker

```powershell
docker compose up --build -d
docker compose logs airflow | Select-String "password"
```

1. Open http://localhost:8081 (the compose file maps host port 8081 to Airflow's 8080; change it in `docker-compose.yml` if needed).
2. Log in as `admin` with the password printed in the logs.
3. Enable the DAG `crm_sales_etl` and press Trigger.
4. The three tasks run in order and should all turn green.

Close any program that has `crm.duckdb` open before triggering the DAG, because DuckDB allows only one writer at a time.

### Example query

```sql
select
    p.series,
    count(*)                              as deals,
    avg(f.is_won::int)                    as win_rate,
    sum(f.close_value) filter (where f.is_won) as won_revenue
from marts.fact_sales_pipeline f
join marts.dim_product p on f.product_key = p.product_key
where f.deal_stage in ('Won', 'Lost')
group by p.series
order by won_revenue desc;
```

## 9. Limitations and future work

- Airflow runs in `standalone` mode with SQLite metadata and the sequential executor. This is a development setup, not a production one.
- DuckDB supports a single writer, so the pipeline steps run one after another.
- The raw load is a full refresh each time. A production version would load increments and keep history of deal-stage changes (a snapshot or a slowly changing dimension).
- Possible additions: a `dim_deal_stage` dimension, dbt documentation site (`dbt docs generate`), and a BI dashboard on top of the marts.
