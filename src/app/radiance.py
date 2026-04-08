"""VIIRS nighttime radiance → H3 cell aggregation.

Reads a VIIRS annual composite GeoTIFF (from a UC Volume), clips to a
city bounding box using rasterio, then uses h3ronpy to directly convert
raster pixels into H3 cells with mean radiance per cell.

Volume access uses the Databricks SDK Files API (UC-compatible) rather
than FUSE mounts, so it works on any cluster access mode.

Data source: Earth Observation Group (EOG), Payne Institute for Public Policy.
License: CC BY 4.0.
Citation:
    Elvidge, C.D, Zhizhin, M., Ghosh T., Hsu FC, Taneja J.
    "Annual time series of global VIIRS nighttime lights derived from
    monthly averages: 2012 to 2019". Remote Sensing 2021, 13(5), p.922
"""

from __future__ import annotations

import logging
import tempfile

import numpy as np
import pandas as pd
from databricks.sdk import WorkspaceClient

log = logging.getLogger(__name__)


def _find_viirs_tif(client: WorkspaceClient, volume_path: str) -> str | None:
    """Find the first .tif file in the Volume using the SDK Files API."""
    try:
        for entry in client.files.list_directory_contents(volume_path):
            if entry.path and entry.path.lower().endswith(".tif"):
                return entry.path
    except Exception as e:
        log.warning("Could not list Volume %s: %s", volume_path, e)
    return None


def _download_viirs_to_temp(client: WorkspaceClient, volume_file_path: str) -> str:
    """Download a VIIRS .tif from the Volume to a local temp file."""
    log.info("Downloading VIIRS tile from Volume: %s", volume_file_path)
    resp = client.files.download(volume_file_path)
    tmp = tempfile.NamedTemporaryFile(suffix=".tif", delete=False)
    tmp.write(resp.contents.read())
    tmp.close()
    log.info("Downloaded to %s", tmp.name)
    return tmp.name


def compute_radiance_h3(
    viirs_path: str,
    city_row: dict,
    resolution: int = 9,
) -> pd.DataFrame:
    """Read VIIRS raster for a city bbox and return mean radiance per H3 cell.

    Uses rasterio to clip the GeoTIFF to the city bounding box, then
    h3ronpy.pandas.raster.raster_to_dataframe to directly convert raster
    pixels into H3 cells with aggregated values.

    Parameters
    ----------
    viirs_path : str
        Local filesystem path to the VIIRS GeoTIFF file (downloaded from
        the Volume via _download_viirs_to_temp).
    city_row : dict
        Row from gold_cities containing at minimum:
        bbox_xmin, bbox_xmax, bbox_ymin, bbox_ymax.
    resolution : int
        H3 resolution for cell assignment.

    Returns
    -------
    pd.DataFrame
        Columns: h3_cell (int64), radiance (float64).
    """
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

    log.info(
        "Raster→H3: %d cells at res %d (bbox %.2f,%.2f → %.2f,%.2f)",
        len(df), resolution, xmin, ymin, xmax, ymax,
    )
    return df
