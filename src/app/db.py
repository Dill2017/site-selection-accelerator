"""DBSQL connection helper using databricks-sdk Config()."""

from __future__ import annotations

import os

import pandas as pd
from databricks import sql as dbsql
from databricks.sdk.core import Config

WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")


def _get_connection() -> dbsql.client.Connection:
    cfg = Config()
    host = cfg.host
    # sql-connector expects bare hostname (no protocol)
    if host and host.startswith("https://"):
        host = host[len("https://"):]
    if host and host.endswith("/"):
        host = host.rstrip("/")

    return dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
    )


def execute_query(query: str) -> pd.DataFrame:
    """Run *query* on the SQL warehouse and return a DataFrame."""
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols)
