"""Fail the task unless fct_daily_orders has at least one order.

Usage: python check_daily_orders.py /path/to/analytics.duckdb
"""

import sys

import duckdb


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: check_daily_orders.py ANALYTICS_DUCKDB")

    connection = duckdb.connect(sys.argv[1], read_only=True)
    try:
        days, orders = connection.execute(
            """
            select count(*), coalesce(sum(order_count), 0)
            from main.fct_daily_orders
            """
        ).fetchall()[0]
    finally:
        connection.close()

    print(f"fct_daily_orders days={days} orders={orders}")
    if days < 1 or orders < 1:
        raise SystemExit("fct_daily_orders is empty")


if __name__ == "__main__":
    main()
