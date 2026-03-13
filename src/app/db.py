"""DBSQL connection helper using databricks-sdk Config().

Keeps a single long-lived connection to avoid the overhead of
TCP connect + authenticate on every query.
"""

from __future__ import annotations

import logging
import os
import threading

import pandas as pd
from databricks import sql as dbsql
from databricks.sdk.core import Config

log = logging.getLogger(__name__)

WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")

_conn: dbsql.client.Connection | None = None
_lock = threading.Lock()


def _create_connection() -> dbsql.client.Connection:
    if os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("IS_DATABRICKS_APP"):
        cfg = Config()
    else:
        cfg = Config(profile="DEFAULT")
    host = cfg.host
    if host and host.startswith("https://"):
        host = host[len("https://"):]
    if host and host.endswith("/"):
        host = host.rstrip("/")

    return dbsql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
        credentials_provider=lambda: cfg.authenticate,
    )


def _get_connection() -> dbsql.client.Connection:
    global _conn
    with _lock:
        if _conn is None or not _conn.open:
            log.info("Opening new DBSQL connection (warehouse=%s)", WAREHOUSE_ID)
            _conn = _create_connection()
        return _conn


def execute_query(query: str) -> pd.DataFrame:
    """Run *query* on the SQL warehouse and return a DataFrame.

    Reuses a cached connection; automatically reconnects on failure.
    """
    try:
        conn = _get_connection()
        return _run(conn, query)
    except Exception:
        global _conn
        with _lock:
            _conn = None
        conn = _get_connection()
        return _run(conn, query)


def _run(conn: dbsql.client.Connection, query: str) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(query)
        cols = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
