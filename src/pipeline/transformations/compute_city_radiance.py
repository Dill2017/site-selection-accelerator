"""Compute VIIRS nighttime radiance for a single city and MERGE into gold_radiance.

Designed to run as a serverless on-demand job triggered by the app when a user
queries a city that isn't already in the gold_radiance table.  Uses FUSE to
read the VIIRS GeoTIFF from a UC Volume (efficient windowed reads via rasterio).

Usage (DABs job with parameters):
    spark_python_task with parameters:
        <catalog> <schema> <warehouse_id> <country> <city> <resolution>
Or locally:
    python compute_city_radiance.py <catalog> <schema> <warehouse_id> GB London 9
"""

from __future__ import annotations

import logging
import os
import sys
import time

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# SQL helper (same as batch ETL — inlined to avoid import issues in tasks)
# ---------------------------------------------------------------------------

def _execute_sql(
    client: WorkspaceClient,
    warehouse_id: str,
    query: str,
) -> pd.DataFrame:
    resp = client.statement_execution.execute_statement(
        statement=query,
        warehouse_id=warehouse_id,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        byte_limit=26214400,
    )

    state = resp.status.state if resp.status else None
    if state in (StatementState.PENDING, StatementState.RUNNING):
        log.info("Query still running, polling…")
        while True:
            resp = client.statement_execution.get_statement(resp.statement_id)
            if resp.status.state in (
                StatementState.SUCCEEDED, StatementState.FAILED,
                StatementState.CANCELED, StatementState.CLOSED,
            ):
                break
            time.sleep(3)

    if resp.status and resp.status.state == StatementState.FAILED:
        msg = resp.status.error.message if resp.status.error else "Unknown"
        raise RuntimeError(f"SQL failed: {msg}\nQuery: {query[:200]}")

    if resp.manifest is None or resp.result is None:
        return pd.DataFrame()

    col_schemas = resp.manifest.schema.columns
    columns = [col.name for col in col_schemas]
    all_rows = list(resp.result.data_array or [])

    total_chunks = resp.manifest.total_chunk_count or 1
    if total_chunks > 1:
        for chunk_idx in range(1, total_chunks):
            chunk = client.statement_execution.get_statement_result_chunk_n(
                statement_id=resp.statement_id,
                chunk_index=chunk_idx,
            )
            if chunk.data_array:
                all_rows.extend(chunk.data_array)

    df = pd.DataFrame(all_rows, columns=columns)
    for col_schema in col_schemas:
        col_name = col_schema.name
        type_text = (col_schema.type_text or "").upper()
        if col_name not in df.columns or df[col_name].empty:
            continue
        if "BIGINT" in type_text or "LONG" in type_text:
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("Int64")
        elif "DOUBLE" in type_text or "FLOAT" in type_text or "DECIMAL" in type_text:
            df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# Volume / VIIRS helpers
# ---------------------------------------------------------------------------

def _find_viirs_tif(client: WorkspaceClient, volume_path: str) -> str | None:
    try:
        for entry in client.files.list_directory_contents(volume_path):
            if entry.path and entry.path.lower().endswith(".tif"):
                return entry.path
    except Exception as e:
        log.warning("Could not list Volume %s: %s", volume_path, e)
    return None


def _get_city_h3_cells(
    client: WorkspaceClient,
    warehouse_id: str,
    geom_wkt: str,
    resolution: int,
) -> set[int]:
    query = f"""
        SELECT explode(h3_polyfillash3('{geom_wkt}', {resolution})) AS h3_cell
    """
    df = _execute_sql(client, warehouse_id, query)
    if df.empty:
        return set()
    return set(df["h3_cell"].astype("int64").tolist())


# ---------------------------------------------------------------------------
# Raster → H3 computation
# ---------------------------------------------------------------------------

def _compute_radiance_h3(
    viirs_path: str,
    city_row: dict,
    resolution: int = 9,
) -> pd.DataFrame:
    import numpy as np
    import rasterio
    from rasterio.windows import from_bounds
    from h3ronpy.pandas.raster import raster_to_dataframe

    xmin = float(city_row["bbox_xmin"])
    xmax = float(city_row["bbox_xmax"])
    ymin = float(city_row["bbox_ymin"])
    ymax = float(city_row["bbox_ymax"])

    with rasterio.open(viirs_path) as src:
        window = from_bounds(xmin, ymin, xmax, ymax, transform=src.transform)
        data = src.read(1, window=window)
        win_transform = src.window_transform(window)

    rows_px, cols_px = data.shape
    if rows_px == 0 or cols_px == 0:
        log.warning("Empty raster window for bbox [%s,%s,%s,%s]", xmin, ymin, xmax, ymax)
        return pd.DataFrame(columns=["h3_cell", "radiance"])

    data_clean = np.nan_to_num(data.astype(np.float64), nan=0.0)

    df = raster_to_dataframe(
        data_clean,
        win_transform,
        h3_resolution=resolution,
        nodata_value=0.0,
        compact=False,
    )

    df["cell"] = df["cell"].astype("int64")
    df = df.rename(columns={"cell": "h3_cell", "value": "radiance"})
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(
    catalog: str,
    schema: str,
    warehouse_id: str,
    country: str,
    city: str,
    resolution: int = 9,
    viirs_volume_name: str = "viirs_nighttime_lights",
) -> str:
    """Compute radiance for *one* city and MERGE into gold_radiance."""
    profile = os.environ.get("DATABRICKS_CONFIG_PROFILE")
    client = WorkspaceClient(profile=profile) if profile else WorkspaceClient()

    volume_path = f"/Volumes/{catalog}/{schema}/{viirs_volume_name}"
    viirs_volume_path = _find_viirs_tif(client, volume_path)

    if viirs_volume_path is None:
        print(f"[VIIRS] GeoTIFF not found in {volume_path} — skipping.")
        return "SKIPPED"

    fuse_path = f"/Volumes/{catalog}/{schema}/{viirs_volume_name}/{viirs_volume_path.split('/')[-1]}"
    print(f"[VIIRS] Using tile via FUSE: {fuse_path}")

    # Fetch city metadata
    city_df = _execute_sql(client, warehouse_id, f"""
        SELECT country, city_name, geom_wkt,
               bbox_xmin, bbox_xmax, bbox_ymin, bbox_ymax
        FROM {catalog}.{schema}.gold_cities
        WHERE country = '{country}' AND city_name = '{city}'
        LIMIT 1
    """)

    if city_df.empty:
        print(f"[VIIRS] City not found in gold_cities: {city}, {country}")
        return "SKIPPED"

    city_row = city_df.iloc[0].to_dict()
    t0 = time.time()

    # Compute radiance from raster
    radiance_df = _compute_radiance_h3(fuse_path, city_row, resolution)
    if radiance_df.empty:
        print(f"[VIIRS] No radiance data for {city}, {country}")
        return "SKIPPED"

    # Filter to city polygon H3 cells
    city_cells = _get_city_h3_cells(
        client, warehouse_id, city_row["geom_wkt"], resolution,
    )
    if city_cells:
        before = len(radiance_df)
        radiance_df = radiance_df[radiance_df["h3_cell"].isin(city_cells)].reset_index(drop=True)
        print(f"[VIIRS] Polygon filter: {before} → {len(radiance_df)} cells")

    if radiance_df.empty:
        print(f"[VIIRS] No cells after polygon filter for {city}, {country}")
        return "SKIPPED"

    radiance_df["country"] = country
    radiance_df["city_name"] = city
    radiance_df = radiance_df[["country", "city_name", "h3_cell", "radiance"]]

    elapsed = time.time() - t0
    print(f"[VIIRS] ✓ {city}, {country}: {len(radiance_df)} H3 cells ({elapsed:.1f}s)")

    # MERGE into gold_radiance (idempotent — safe for concurrent writes)
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    table_name = f"{catalog}.{schema}.gold_radiance"

    sdf = spark.createDataFrame(radiance_df)
    sdf.createOrReplaceTempView("new_radiance")

    spark.sql(f"""
        MERGE INTO {table_name} t
        USING new_radiance s
        ON t.country = s.country
           AND t.city_name = s.city_name
           AND t.h3_cell = s.h3_cell
        WHEN MATCHED THEN UPDATE SET radiance = s.radiance
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"[VIIRS] Merged {len(radiance_df)} rows into {table_name}")
    log.info("Merged radiance for %s, %s: %d cells", city, country, len(radiance_df))

    return table_name


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    try:
        from databricks.sdk.runtime import dbutils  # type: ignore[import]
        catalog = dbutils.widgets.get("catalog")
        schema = dbutils.widgets.get("schema")
        warehouse_id = dbutils.widgets.get("warehouse_id")
        country = dbutils.widgets.get("country")
        city = dbutils.widgets.get("city")
        resolution = int(dbutils.widgets.get("resolution"))
        viirs_volume_name = dbutils.widgets.get("viirs_volume_name")
    except Exception:
        if len(sys.argv) >= 7:
            catalog, schema, warehouse_id = sys.argv[1], sys.argv[2], sys.argv[3]
            country, city = sys.argv[4], sys.argv[5]
            resolution = int(sys.argv[6])
            viirs_volume_name = sys.argv[7] if len(sys.argv) >= 8 else "viirs_nighttime_lights"
        else:
            print("Usage: compute_city_radiance.py <catalog> <schema> <warehouse_id> <country> <city> <resolution> [viirs_volume_name]")
            sys.exit(1)

    result = main(catalog, schema, warehouse_id, country, city, resolution, viirs_volume_name)
    print(f"CITY_RADIANCE_RESULT={result}")
