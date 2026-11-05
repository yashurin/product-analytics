# product-analytics

Practice project for a small product analytics platform.

Raw e-commerce orders live in `data/raw/`. dbt transforms that data into analytics models. Apache Airflow orchestrates the pipeline runs. A FastAPI app serves the resulting metrics. DuckDB is the local warehouse. The same dbt models can target Snowflake.

## Run locally

Use Python 3.11 or newer. Run every command below from the project root. The orders source reads `data/raw/orders.csv` relative to the current directory, and `profiles.yml` writes `./analytics.duckdb` there.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
dbt build --profiles-dir .
uvicorn app.main:app --reload
```

`dbt build` creates the staging view, the daily fact table, and runs the tests in `models/schema.yml`. `uvicorn` serves the API at http://127.0.0.1:8000.

- `GET /health` returns `{"status": "ok"}`.
- `GET /metrics/daily` returns the latest `fct_daily_orders` row. On the sample data that is `2026-10-15`, with `order_count` 1, `total_revenue` `99.00`, and `avg_order_value` `99.00`.

`requirements.txt` lists `dbt-duckdb`, `fastapi`, `uvicorn`, `pandas`, and `duckdb`. `--profiles-dir .` forces dbt to use this folder's `profiles.yml`. The app opens `analytics.duckdb` read-only. Set `DUCKDB_PATH` to point it at another file.

The same `dbt build` also runs inside the `transform_orders` DAG. See [Airflow](#airflow).

## Layout

```
product-analytics/
├── README.md
├── requirements.txt
├── .gitignore
├── dbt_project.yml
├── profiles.yml
├── models/
│   ├── schema.yml
│   ├── staging/
│   │   └── stg_orders.sql
│   └── marts/
│       └── fct_daily_orders.sql
├── app/
│   └── main.py
├── data/
│   └── raw/
│       └── orders.csv
└── airflow/
    ├── Dockerfile
    ├── requirements.txt
    ├── packages.txt
    ├── docker-compose.override.yml
    ├── .astro/
    │   └── config.yaml
    ├── dags/
    │   └── transform_orders.py
    ├── include/
    │   └── check_daily_orders.py
    └── plugins/
```

`orders.csv` has 50 sample orders with `order_id`, `customer_id`, `product_id`, `order_date`, `quantity`, `unit_price`, and `status`. Each `product_id` keeps a stable unit price. Statuses include `completed`, `shipped`, `processing`, `cancelled`, and `returned`. Dates run from January through October 2026.

`stg_orders` casts those columns and adds `line_amount`. `fct_daily_orders` aggregates each day into `order_count`, `total_revenue`, and `avg_order_value`. Cancelled orders are left out of that fact.

## Airflow

Orchestration is an Astro project in `airflow/`. `airflow/.astro/config.yaml` is what the Astro CLI looks for, so you do not need `astro dev init` here. Run `astro dev init` only in an empty directory when you want the CLI to generate a new project. Running it inside `airflow/` fails because that directory is not empty.

`airflow/requirements.txt` lists `dbt-duckdb` and `duckdb`. The Dockerfile installs them into `/usr/local/airflow/dbt_venv`, separate from Airflow's own libraries. `BashOperator` comes from `apache-airflow-providers-standard`, which the Astro Runtime image already includes.

Start the local Airflow containers from the Astro project:

```bash
cd airflow
astro dev start
```

The first start builds the image. When the containers are up, open http://localhost:8080 and log in with `admin` / `admin`. Trigger the `transform_orders` DAG. It checks that `data/raw/orders.csv` is present, runs `dbt build`, then checks that `fct_daily_orders` has rows.

`docker-compose.override.yml` mounts the parent of `airflow/` into the scheduler at `/usr/local/airflow/product-analytics`. Local Astro runs tasks in the scheduler, and the mount is read-only, so the dbt task copies the project to `/tmp` before it builds. Start the CLI from `airflow/` so that mount resolves. If the file check cannot see the CSV, stop the project and start it again with the repo path set:

```bash
cd airflow
PRODUCT_ANALYTICS_HOST_DIR="$(cd .. && pwd)" astro dev start
```

## Snowflake

Leave `target: dev` in `profiles.yml` so a normal `dbt build` stays on DuckDB. Add a second output and select it with `--target snowflake`. Install the adapter in the same virtualenv (`pip install dbt-snowflake`) and keep the password in the environment.

```yaml
product_analytics:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ./analytics.duckdb
      threads: 4
      schema: main
    snowflake:
      type: snowflake
      account: <account>       # locator, for example xy12345.us-east-1
      user: <user>
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: TRANSFORMER
      warehouse: TRANSFORMING
      database: ANALYTICS
      schema: PRODUCT
      threads: 4
```

```bash
export SNOWFLAKE_PASSWORD=...
dbt build --target snowflake --profiles-dir .
```

Create these objects before the first Snowflake build. `TRANSFORMER` needs to read raw orders and create the models in `ANALYTICS.PRODUCT`.

```sql
CREATE WAREHOUSE TRANSFORMING
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE;

CREATE DATABASE ANALYTICS;
CREATE SCHEMA ANALYTICS.RAW;
CREATE SCHEMA ANALYTICS.PRODUCT;

CREATE ROLE TRANSFORMER;
GRANT USAGE ON WAREHOUSE TRANSFORMING TO ROLE TRANSFORMER;
GRANT USAGE ON DATABASE ANALYTICS TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA ANALYTICS.RAW TO ROLE TRANSFORMER;
GRANT USAGE, CREATE TABLE, CREATE VIEW ON SCHEMA ANALYTICS.PRODUCT TO ROLE TRANSFORMER;

CREATE TABLE ANALYTICS.RAW.ORDERS (
  order_id VARCHAR,
  customer_id VARCHAR,
  product_id VARCHAR,
  order_date DATE,
  quantity INTEGER,
  unit_price NUMBER(12, 2),
  status VARCHAR
);

GRANT SELECT ON TABLE ANALYTICS.RAW.ORDERS TO ROLE TRANSFORMER;
GRANT ROLE TRANSFORMER TO USER <user>;
```

Load `data/raw/orders.csv` into `ANALYTICS.RAW.ORDERS`. The DuckDB source in `models/schema.yml` uses `read_csv` through `external_location`, which Snowflake does not provide. For the Snowflake target, point that source at the table and remove `external_location`:

```yaml
sources:
  - name: raw
    database: ANALYTICS
    schema: RAW
    tables:
      - name: orders
```

The Airflow DAG does not use a warehouse connection today. `dbt build` runs in the dbt virtualenv and writes the local DuckDB file, and `check_daily_orders` reads that file. For Snowflake:

- Add `dbt-snowflake` to `airflow/requirements.txt` and run `astro dev restart` so the virtualenv picks it up.
- Create a local connection in `airflow/airflow_settings.yaml` (that file is gitignored):

```yaml
airflow:
  connections:
    - conn_id: snowflake_default
      conn_type: snowflake
      conn_login: <user>
      conn_password: <password>
      conn_schema: PRODUCT
      conn_extra:
        account: <account>
        warehouse: TRANSFORMING
        database: ANALYTICS
        role: TRANSFORMER
```

- On the existing `dbt_build` task, set `append_env=True` so the virtualenv stays on `PATH`, pass the connection password through, and add `--target snowflake` to the `dbt build` line:

```python
dbt_build = BashOperator(
    task_id="dbt_build",
    append_env=True,
    env={"SNOWFLAKE_PASSWORD": "{{ conn.snowflake_default.password }}"},
    bash_command=f"""
    set -euo pipefail
    test -s "{RAW_ORDERS}"
    rm -rf "{WORK_DIR}"
    mkdir -p "{WORK_DIR}"
    cp "{PROJECT_DIR}/dbt_project.yml" "{PROJECT_DIR}/profiles.yml" "{WORK_DIR}/"
    cp -R "{PROJECT_DIR}/models" "{PROJECT_DIR}/data" "{WORK_DIR}/"
    cd "{WORK_DIR}"
    "{DBT}" build --target snowflake --profiles-dir .
    """,
)
```

`check_daily_orders` still opens `analytics.duckdb`. A Snowflake run does not create that file. Drop that task, or replace it with a query against `ANALYTICS.PRODUCT.FCT_DAILY_ORDERS`. The FastAPI app keeps reading the local DuckDB file either way.
