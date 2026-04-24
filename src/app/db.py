"""DBSQL query helper using the Databricks SDK Statement Execution API.

Uses the REST-based statement execution endpoint rather than the Thrift
connector, which is more reliable from Databricks App environments.

Results are fetched via EXTERNAL_LINKS disposition with Arrow IPC format,
which avoids the 25 MB inline byte limit and handles arbitrarily large
result sets.
"""

from __future__ import annotations

import io
import logging
import os
import threading
import time
from urllib.request import Request, urlopen

import pandas as pd
import pyarrow as pa
import pyarrow.ipc
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    Disposition,
    ExternalLink,
    Format,
    StatementParameterListItem,
    StatementState,
)

log = logging.getLogger(__name__)

_MAX_POLL_ITERATIONS = 150  # ~5 min at 2 s intervals

_client: WorkspaceClient | None = None
_client_lock = threading.Lock()


def _get_client() -> WorkspaceClient:
    """Return a cached WorkspaceClient (thread-safe double-checked locking).

    Strategy:
      1. If DATABRICKS_CONFIG_PROFILE is explicitly set, use that profile
         (local-dev scenario).
      2. Otherwise call WorkspaceClient() with no args — the SDK auto-detects
         the correct auth in every Databricks-managed environment (Apps, Jobs,
         Notebooks) as well as when DATABRICKS_HOST + PAT are set in env.
    """
    global _client
    if _client is not None:
        return _client

    with _client_lock:
        if _client is not None:
            return _client

        profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
        try:
            if profile:
                _client = WorkspaceClient(profile=profile)
            else:
                _client = WorkspaceClient()
        except Exception as exc:
            raise RuntimeError(
                f"Failed to initialise WorkspaceClient: {exc}. "
                "Ensure DATABRICKS_HOST and authentication are configured "
                "(or set DATABRICKS_CONFIG_PROFILE for local development)."
            ) from exc

        log.info("Initialised WorkspaceClient (warehouse=%s)", _validate_warehouse_id())
        return _client


def _validate_warehouse_id() -> str:
    wh_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
    if not wh_id:
        raise RuntimeError(
            "DATABRICKS_WAREHOUSE_ID is not set. "
            "Ensure the environment variable is configured in app.yml "
            "(via the sql-warehouse resource) or in your local .env file."
        )
    return wh_id


def _wait_for_statement(client: WorkspaceClient, statement_id: str):
    """Poll until the statement finishes executing (max ~5 min)."""
    for _ in range(_MAX_POLL_ITERATIONS):
        resp = client.statement_execution.get_statement(statement_id)
        state = resp.status.state
        if state in (StatementState.SUCCEEDED, StatementState.FAILED,
                     StatementState.CANCELED, StatementState.CLOSED):
            return resp
        log.debug("Statement %s state=%s, polling…", statement_id, state)
        time.sleep(2)

    raise RuntimeError(
        f"Statement {statement_id} did not complete within "
        f"{_MAX_POLL_ITERATIONS * 2}s — possible warehouse timeout."
    )


def _download_arrow_chunk(link: ExternalLink) -> pa.Table:
    """Download a single Arrow IPC chunk from a pre-signed external link."""
    req = Request(link.external_link)
    if link.http_headers:
        for key, value in link.http_headers.items():
            req.add_header(key, value)
    with urlopen(req) as resp:
        buf = resp.read()
    reader = pa.ipc.open_stream(io.BytesIO(buf))
    return reader.read_all()


def _collect_arrow_tables(client: WorkspaceClient, resp) -> pa.Table:
    """Fetch all Arrow chunks (first from the response, then paginated)."""
    tables: list[pa.Table] = []

    if resp.result and resp.result.external_links:
        for link in resp.result.external_links:
            tables.append(_download_arrow_chunk(link))

    total_chunks = resp.manifest.total_chunk_count or 1
    if total_chunks > 1:
        log.info("Fetching %d additional result chunks…", total_chunks - 1)
        for chunk_idx in range(1, total_chunks):
            chunk_resp = client.statement_execution.get_statement_result_chunk_n(
                statement_id=resp.statement_id,
                chunk_index=chunk_idx,
            )
            if chunk_resp.external_links:
                for link in chunk_resp.external_links:
                    tables.append(_download_arrow_chunk(link))

    if not tables:
        return pa.table({})
    return pa.concat_tables(tables)


def execute_query(
    query: str,
    *,
    params: list[StatementParameterListItem] | None = None,
    raise_on_truncation: bool = False,
) -> pd.DataFrame:
    """Run *query* on the SQL warehouse and return a DataFrame.

    Handles long-running queries by polling.  Uses EXTERNAL_LINKS
    disposition with ARROW_STREAM format so there is no byte-limit
    ceiling on result size.

    Parameters
    ----------
    params : list[StatementParameterListItem] | None
        Named parameters referenced as ``:param_name`` in the query.
    raise_on_truncation : bool
        If ``True``, raise when the manifest reports truncation (should
        not happen with EXTERNAL_LINKS but kept for safety).
    """
    wh_id = _validate_warehouse_id()
    client = _get_client()

    resp = client.statement_execution.execute_statement(
        statement=query,
        warehouse_id=wh_id,
        wait_timeout="50s",
        disposition=Disposition.EXTERNAL_LINKS,
        format=Format.ARROW_STREAM,
        parameters=params,
    )

    state = resp.status.state if resp.status else None

    if state in (StatementState.PENDING, StatementState.RUNNING):
        log.info("Query still running after initial wait, polling…")
        resp = _wait_for_statement(client, resp.statement_id)

    if resp.status and resp.status.state == StatementState.FAILED:
        msg = resp.status.error.message if resp.status.error else "Unknown SQL error"
        raise RuntimeError(f"SQL execution failed: {msg}")

    if resp.manifest is None or resp.result is None:
        return pd.DataFrame()

    arrow_table = _collect_arrow_tables(client, resp)
    total_rows = arrow_table.num_rows

    is_truncated = resp.manifest.truncated
    total_expected = resp.manifest.total_row_count
    log.info(
        "Query: %d rows fetched (expected=%s, truncated=%s)",
        total_rows, total_expected, is_truncated,
    )

    if is_truncated:
        log.warning("Result TRUNCATED: got %d of %s rows", total_rows, total_expected)
        if raise_on_truncation:
            raise RuntimeError(
                f"Query results truncated: received {total_rows} of "
                f"{total_expected} rows."
            )

    return arrow_table.to_pandas()
