"""Run the product-analytics dbt project.

1. Confirm the raw orders file is on the mounted project.
2. Run dbt build in a writable copy.
3. Confirm fct_daily_orders has rows.
"""

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag
from pendulum import datetime

PROJECT_DIR = "/usr/local/airflow/product-analytics"
WORK_DIR = "/tmp/product-analytics"
RAW_ORDERS = f"{PROJECT_DIR}/data/raw/orders.csv"
DBT = "/usr/local/airflow/dbt_venv/bin/dbt"
PYTHON = "/usr/local/airflow/dbt_venv/bin/python"
# include/ is mounted into the scheduler. A heredoc in the task command is not:
# the indented closing delimiter never matches, and Python then sees leading spaces.
CHECK_DAILY = "/usr/local/airflow/include/check_daily_orders.py"


@dag(
    dag_id="transform_orders",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["product-analytics"],
)
def transform_orders():
    check_raw_orders = BashOperator(
        task_id="check_raw_orders",
        bash_command=f"""
        set -euo pipefail
        if [ ! -s "{RAW_ORDERS}" ]; then
          echo "Raw orders file not found at {RAW_ORDERS}."
          echo "Mount the product-analytics repo on the scheduler at {PROJECT_DIR}."
          exit 1
        fi
        echo "Raw orders file is available: {RAW_ORDERS}"
        wc -l "{RAW_ORDERS}"
        """,
    )

    # The project mount is read-only. Copy it so dbt can write analytics.duckdb and target/.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"""
        set -euo pipefail
        test -s "{RAW_ORDERS}"
        rm -rf "{WORK_DIR}"
        mkdir -p "{WORK_DIR}"
        cp "{PROJECT_DIR}/dbt_project.yml" "{PROJECT_DIR}/profiles.yml" "{WORK_DIR}/"
        cp -R "{PROJECT_DIR}/models" "{PROJECT_DIR}/data" "{WORK_DIR}/"
        cd "{WORK_DIR}"
        "{DBT}" build --profiles-dir .
        """,
    )

    check_daily_orders = BashOperator(
        task_id="check_daily_orders",
        bash_command=(
            "set -euo pipefail\n"
            f'"{PYTHON}" "{CHECK_DAILY}" "{WORK_DIR}/analytics.duckdb"\n'
        ),
    )

    check_raw_orders >> dbt_build >> check_daily_orders


transform_orders()
