# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Compute City Radiance (On-Demand)
# MAGIC Compute VIIRS nighttime radiance for a single city and MERGE into gold_radiance.
# MAGIC
# MAGIC Designed to run as a serverless on-demand job triggered by the app when a user
# MAGIC queries a city that isn't already in the gold_radiance table. Uses FUSE to
# MAGIC read the VIIRS GeoTIFF from a UC Volume (efficient windowed reads via rasterio).

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("country", "")
dbutils.widgets.text("city", "")
dbutils.widgets.text("resolution", "9")
dbutils.widgets.text("viirs_volume_name", "viirs_nighttime_lights")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
warehouse_id = dbutils.widgets.get("warehouse_id")
country = dbutils.widgets.get("country")
city = dbutils.widgets.get("city")
resolution = int(dbutils.widgets.get("resolution"))
viirs_volume_name = dbutils.widgets.get("viirs_volume_name")

print(f"Catalog:      {catalog}")
print(f"Schema:       {schema}")
print(f"Warehouse ID: {warehouse_id}")
print(f"Country:      {country}")
print(f"City:         {city}")
print(f"Resolution:   {resolution}")
print(f"VIIRS Volume: {viirs_volume_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports & SQL Helper

# COMMAND ----------

from __future__ import annotations

import io
import logging
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
    StatementState,
)

log = logging.getLogger(__name__)


def _download_arrow_chunk(link: ExternalLink) -> pa.Table:
    req = Request(link.external_link)
    if link.http_headers:
        for key, value in link.http_headers.items():
            req.add_header(key, value)
    with urlopen(req) as resp:
        buf = resp.read()
    return pa.ipc.open_stream(io.BytesIO(buf)).read_all()


def _execute_sql(
    client: WorkspaceClient,
    warehouse_id: str,
    query: str,
) -> pd.DataFrame:
    resp = client.statement_execution.execute_statement(
        statement=query,
        warehouse_id=warehouse_id,
        wait_timeout="50s",
        disposition=Disposition.EXTERNAL_LINKS,
        format=Format.ARROW_STREAM,
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

    tables: list[pa.Table] = []
    if resp.result.external_links:
        for link in resp.result.external_links:
            tables.append(_download_arrow_chunk(link))

    total_chunks = resp.manifest.total_chunk_count or 1
    if total_chunks > 1:
        for chunk_idx in range(1, total_chunks):
            chunk_resp = client.statement_execution.get_statement_result_chunk_n(
                statement_id=resp.statement_id,
                chunk_index=chunk_idx,
            )
            if chunk_resp.external_links:
                for link in chunk_resp.external_links:
                    tables.append(_download_arrow_chunk(link))

    if not tables:
        return pd.DataFrame()
    return pa.concat_tables(tables).to_pandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume / Raster Helpers

# COMMAND ----------

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


def _compute_radiance_h3(
    viirs_path: str,
    city_row: dict,
    resolution: int = 9,
) -> pd.DataFrame:
    """Read VIIRS raster for a city bbox and return mean radiance per H3 cell."""
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main

# COMMAND ----------

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
    client = WorkspaceClient()

    volume_path = f"/Volumes/{catalog}/{schema}/{viirs_volume_name}"
    viirs_volume_path = _find_viirs_tif(client, volume_path)

    if viirs_volume_path is None:
        print(f"[VIIRS] GeoTIFF not found in {volume_path} — skipping.")
        return "SKIPPED"

    fuse_path = f"/Volumes/{catalog}/{schema}/{viirs_volume_name}/{viirs_volume_path.split('/')[-1]}"
    print(f"[VIIRS] Using tile via FUSE: {fuse_path}")

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

    radiance_df = _compute_radiance_h3(fuse_path, city_row, resolution)
    if radiance_df.empty:
        print(f"[VIIRS] No radiance data for {city}, {country}")
        return "SKIPPED"

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute

# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

result = main(catalog, schema, warehouse_id, country, city, resolution, viirs_volume_name)
print(f"CITY_RADIANCE_RESULT={result}")
