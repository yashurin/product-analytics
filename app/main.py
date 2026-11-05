"""Serve product-analytics metrics from the local DuckDB warehouse.

`dbt build --profiles-dir .` writes `analytics.duckdb` at the project root.
This process only reads that file. Rebuild the models before expecting new numbers.

Run from the project root:

    uvicorn app.main:app --reload
"""

from __future__ import annotations

import os
from datetime import date
from decimal import Decimal
from pathlib import Path

import duckdb
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# profiles.yml stores the warehouse next to the dbt project, not next to this module.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DB_PATH = Path(os.environ.get("DUCKDB_PATH", PROJECT_ROOT / "analytics.duckdb"))

app = FastAPI(title="product-analytics", version="1.0.0")


class DailyMetrics(BaseModel):
    """The newest row in main.fct_daily_orders."""

    order_date: date
    order_count: int
    total_revenue: Decimal
    avg_order_value: Decimal


def connect() -> duckdb.DuckDBPyConnection:
    """Open the warehouse read-only.

    Read-only avoids DuckDB's single-writer lock, so dbt can rebuild the file
    while the API is running. A connection is not safe to share across
    requests, so each call opens its own.
    """
    if not DB_PATH.is_file():
        raise HTTPException(
            status_code=503,
            detail=(
                f"Warehouse not found at {DB_PATH}. "
                "Run `dbt build --profiles-dir .` from the project root."
            ),
        )
    return duckdb.connect(str(DB_PATH), read_only=True)


@app.get("/health")
def health() -> dict[str, str]:
    """Process is up. This does not open the warehouse."""
    return {"status": "ok"}


@app.get("/metrics/daily", response_model=DailyMetrics)
def latest_daily_metrics() -> DailyMetrics:
    """Return order count, revenue, and average order value for the latest day."""
    connection = connect()
    try:
        rows = connection.execute(
            """
            select
                order_date,
                order_count,
                total_revenue,
                avg_order_value
            from main.fct_daily_orders
            order by order_date desc
            limit 1
            """
        ).fetchall()
    except duckdb.CatalogException as exc:
        raise HTTPException(
            status_code=503,
            detail=(
                "Table main.fct_daily_orders is missing. "
                "Run `dbt build --profiles-dir .` from the project root."
            ),
        ) from exc
    finally:
        connection.close()

    if not rows:
        raise HTTPException(status_code=404, detail="fct_daily_orders has no rows.")

    order_date, order_count, total_revenue, avg_order_value = rows[0]
    return DailyMetrics(
        order_date=order_date,
        order_count=order_count,
        total_revenue=total_revenue,
        avg_order_value=avg_order_value,
    )
