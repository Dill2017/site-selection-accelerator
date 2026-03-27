"""DBSQL query helper using the Databricks SDK Statement Execution API.

Uses the REST-based statement execution endpoint rather than the Thrift
connector, which is more reliable from Databricks App environments.
"""

from __future__ import annotations

import logging
import os
import time

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

log = logging.getLogger(__name__)

WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID")

_MAX_POLL_ITERATIONS = 150  # ~5 min at 2 s intervals

_client: WorkspaceClient | None = None


def _get_client() -> WorkspaceClient:
    """Return a cached WorkspaceClient.

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

    log.info("Initialised WorkspaceClient (warehouse=%s)", WAREHOUSE_ID)
    return _client


def _validate_warehouse_id() -> str:
    if not WAREHOUSE_ID:
        raise RuntimeError(
            "DATABRICKS_WAREHOUSE_ID is not set. "
            "Ensure the environment variable is configured in app.yml "
            "(via the sql-warehouse resource) or in your local .env file."
        )
    return WAREHOUSE_ID


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


def _cast_columns(df: pd.DataFrame, col_schemas: list) -> pd.DataFrame:
    """Convert string columns to proper types based on the SQL schema."""
    for col_schema in col_schemas:
        col_name = col_schema.name
        type_text = (col_schema.type_text or "").upper()
        if col_name not in df.columns or df[col_name].empty:
            continue
        try:
            if "BIGINT" in type_text or "LONG" in type_text:
                df[col_name] = df[col_name].apply(
                    lambda v: int(v) if v is not None else None
                )
                df[col_name] = df[col_name].astype("Int64")
            elif "INT" in type_text:
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("Int64")
            elif "DOUBLE" in type_text or "FLOAT" in type_text or "DECIMAL" in type_text:
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
            elif "BOOLEAN" in type_text:
                df[col_name] = df[col_name].map({"true": True, "false": False, None: None})
        except Exception as e:
            log.warning("Type cast failed for column %s (%s): %s", col_name, type_text, e)
    return df


def execute_query(query: str) -> pd.DataFrame:
    """Run *query* on the SQL warehouse and return a DataFrame.

    Handles long-running queries by polling. Uses INLINE disposition
    with maximum byte limit. Fetches all chunks for paginated results.
    """
    wh_id = _validate_warehouse_id()
    client = _get_client()

    resp = client.statement_execution.execute_statement(
        statement=query,
        warehouse_id=wh_id,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        byte_limit=26214400,
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

    col_schemas = resp.manifest.schema.columns
    columns = [col.name for col in col_schemas]

    all_rows = list(resp.result.data_array or [])

    total_chunks = resp.manifest.total_chunk_count or 1
    if total_chunks > 1:
        log.info("Fetching %d additional result chunks…", total_chunks - 1)
        for chunk_idx in range(1, total_chunks):
            chunk = client.statement_execution.get_statement_result_chunk_n(
                statement_id=resp.statement_id,
                chunk_index=chunk_idx,
            )
            if chunk.data_array:
                all_rows.extend(chunk.data_array)

    is_truncated = resp.manifest.truncated
    total_expected = resp.manifest.total_row_count
    log.info(
        "Query: %d rows fetched (expected=%s, truncated=%s, chunks=%d)",
        len(all_rows), total_expected, is_truncated, total_chunks,
    )

    if is_truncated:
        log.warning("Result TRUNCATED: got %d of %s rows", len(all_rows), total_expected)

    df = pd.DataFrame(all_rows, columns=columns)
    df = _cast_columns(df, col_schemas)
    return df
