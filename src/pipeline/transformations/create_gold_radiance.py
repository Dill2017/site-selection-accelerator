# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Gold Radiance
# MAGIC Pre-compute VIIRS nighttime radiance per H3 cell for training cities.
# MAGIC
# MAGIC Reads the VIIRS GeoTIFF from a UC Volume, clips to each training city's
# MAGIC bounding box, assigns pixels to H3 cells via Databricks SQL, and writes
# MAGIC the aggregated result as a gold_radiance Delta table.
# MAGIC
# MAGIC If the VIIRS file is not present in the Volume the task exits
# MAGIC successfully with a warning — radiance is an optional enrichment.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.text("viirs_volume_name", "viirs_nighttime_lights")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
warehouse_id = dbutils.widgets.get("warehouse_id")
viirs_volume_name = dbutils.widgets.get("viirs_volume_name")

print(f"Catalog:           {catalog}")
print(f"Schema:            {schema}")
print(f"Warehouse ID:      {warehouse_id}")
print(f"VIIRS Volume Name: {viirs_volume_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports & Configuration

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

H3_RESOLUTION = 9

TRAINING_CITIES: list[tuple[str, str]] = [
    ("RU", "Москва"),
    ("GB", "London"),
    ("IT", "Roma"),
    ("US", "New York"),
    ("DE", "Berlin"),
    ("FI", "Helsinki"),
    ("NO", "Oslo"),
    ("US", "Chicago"),
    ("PL", "Warszawa"),
    ("ES", "Madrid"),
    ("CZ", "Praha"),
    ("LT", "Vilnius"),
    ("KZ", "Астана"),
    ("AT", "Wien"),
    ("BY", "Мінск"),
    ("LV", "Rīga"),
    ("RS", "Београд"),
    ("SK", "Bratislava"),
    ("US", "San Francisco"),
    ("PL", "Kraków"),
    ("PL", "Gdańsk"),
    ("PL", "Wrocław"),
    ("PL", "Łódź"),
    ("HR", "Zagreb"),
    ("SE", "Stockholm"),
    ("PL", "Poznań"),
    ("IS", "Reykjavík"),
    ("SI", "Ljubljana"),
    ("NL", "Amsterdam"),
    ("EE", "Tallinn"),
    ("BG", "София"),
    ("IE", "Dublin"),
    ("FR", "Paris"),
    ("PT", "Lisboa"),
    ("LU", "Luxembourg"),
    ("CH", "Bern"),
    ("BE", "Bruxelles - Brussel"),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Execution Helper

# COMMAND ----------

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
    """Execute a SQL statement and return a DataFrame."""
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
# MAGIC ## City Processing & Raster Helpers

# COMMAND ----------

def _find_viirs_tif(client: WorkspaceClient, volume_path: str) -> str | None:
    """Find the first .tif file in the Volume using the SDK Files API."""
    try:
        for entry in client.files.list_directory_contents(volume_path):
            if entry.path and entry.path.lower().endswith(".tif"):
                return entry.path
    except Exception as e:
        log.warning("Could not list Volume %s: %s", volume_path, e)
    return None


def _get_city_rows(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
) -> pd.DataFrame:
    """Fetch bounding-box and polygon data for training cities."""
    placeholders = ", ".join(
        f"('{country}', '{city}')" for country, city in TRAINING_CITIES
    )
    query = f"""
        SELECT country, city_name, geom_wkt,
               bbox_xmin, bbox_xmax, bbox_ymin, bbox_ymax
        FROM {catalog}.{schema}.gold_cities
        WHERE (country, city_name) IN ({placeholders})
    """
    return _execute_sql(client, warehouse_id, query)


def _get_city_h3_cells(
    client: WorkspaceClient,
    warehouse_id: str,
    geom_wkt: str,
    resolution: int,
) -> set[int]:
    """Return the set of H3 cell IDs that fill a city polygon."""
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


def _compute_radiance_for_city(
    viirs_path: str,
    city_row: dict,
    resolution: int,
    client: WorkspaceClient,
    warehouse_id: str,
) -> pd.DataFrame:
    """Read VIIRS raster for one city, filter to city polygon H3 cells."""
    radiance_df = _compute_radiance_h3(viirs_path, city_row, resolution)
    if radiance_df.empty:
        return radiance_df

    city_cells = _get_city_h3_cells(
        client, warehouse_id, city_row["geom_wkt"], resolution,
    )
    if not city_cells:
        return pd.DataFrame(columns=["h3_cell", "radiance"])

    filtered = radiance_df[radiance_df["h3_cell"].isin(city_cells)].reset_index(drop=True)
    log.info("  Polygon filter: %d → %d cells", len(radiance_df), len(filtered))
    return filtered

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Pipeline

# COMMAND ----------

def main(
    catalog: str,
    schema: str,
    warehouse_id: str,
    viirs_volume_name: str = "viirs_nighttime_lights",
) -> str:
    """Compute gold_radiance for all training cities."""
    client = WorkspaceClient()

    volume_path = f"/Volumes/{catalog}/{schema}/{viirs_volume_name}"
    viirs_volume_path = _find_viirs_tif(client, volume_path)

    if viirs_volume_path is None:
        log.warning(
            "VIIRS GeoTIFF not found in %s — skipping radiance computation. "
            "This is expected if the VIIRS data has not been downloaded yet. "
            "See README.md for instructions.",
            volume_path,
        )
        return "SKIPPED"

    fuse_path = f"/Volumes/{catalog}/{schema}/{viirs_volume_name}/{viirs_volume_path.split('/')[-1]}"
    print(f"[VIIRS] Using tile via FUSE: {fuse_path}")

    city_rows = _get_city_rows(client, warehouse_id, catalog, schema)
    total_cities = len(city_rows)
    print(f"[VIIRS] Found {total_cities} / {len(TRAINING_CITIES)} training cities in gold_cities")

    if city_rows.empty:
        print("[VIIRS] No training cities found in gold_cities — skipping.")
        return "SKIPPED"

    all_results: list[pd.DataFrame] = []
    for idx, (_, row) in enumerate(city_rows.iterrows(), 1):
        country = row["country"]
        city_name = row["city_name"]
        t_city = time.time()
        print(f"[VIIRS] [{idx}/{total_cities}] Processing {city_name}, {country}…")
        try:
            result = _compute_radiance_for_city(
                fuse_path, row.to_dict(), H3_RESOLUTION, client, warehouse_id,
            )
            elapsed = time.time() - t_city
            if not result.empty:
                result["country"] = country
                result["city_name"] = city_name
                all_results.append(result)
                print(f"[VIIRS] [{idx}/{total_cities}] ✓ {city_name}: {len(result)} H3 cells ({elapsed:.1f}s)")
            else:
                print(f"[VIIRS] [{idx}/{total_cities}] ⚠ {city_name}: no radiance data ({elapsed:.1f}s)")
        except Exception as e:
            print(f"[VIIRS] [{idx}/{total_cities}] ✗ {city_name}: FAILED — {e}")
            log.error("Failed for %s, %s: %s", city_name, country, e)

    if not all_results:
        log.warning("No radiance data produced for any city — skipping table write.")
        return "SKIPPED"

    combined = pd.concat(all_results, ignore_index=True)
    combined = combined[["country", "city_name", "h3_cell", "radiance"]]
    log.info("Total radiance records: %d across %d cities", len(combined), len(all_results))

    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    sdf = spark.createDataFrame(combined)
    table_name = f"{catalog}.{schema}.gold_radiance"
    sdf.write.mode("overwrite").saveAsTable(table_name)
    print(f"[VIIRS] Wrote {len(combined)} rows to {table_name}")
    log.info("Wrote gold_radiance table: %s", table_name)

    return f"{catalog}.{schema}.gold_radiance"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute

# COMMAND ----------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

result = main(catalog, schema, warehouse_id, viirs_volume_name)
print(f"GOLD_RADIANCE_RESULT={result}")
