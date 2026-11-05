FROM astrocrpublic.azurecr.io/runtime:3.3-11-python-3.12-base

# dbt and Airflow pin different versions of the same libraries.
# Install dbt into its own virtualenv instead of the Airflow environment.
# Python 3.12 matches the wheels used by dbt-duckdb. Runtime 3.3 defaults to 3.14.
USER astro
COPY requirements.txt /tmp/dbt-requirements.txt
RUN python -m venv /usr/local/airflow/dbt_venv \
    && /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt
