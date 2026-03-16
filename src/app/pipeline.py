"""DBSQL geospatial query pipeline.

Reads from pre-processed gold tables (gold_cities, gold_places) that
contain flattened coordinates, pre-joined polygons, and denested
categories.  All heavy geospatial pre-processing runs once in the
SDP pipeline; the app only does lightweight H3 tessellation and
POI-to-cell assignment at query time.
"""

from __future__ import annotations

import h3 as _h3
import pandas as pd

from config import GOLD_BUILDINGS_TABLE, GOLD_CITIES_TABLE, GOLD_PLACES_TABLE
from db import execute_query


# ── Lookup helpers (populate dropdowns) ─────────────────────────────────────


def get_countries() -> list[str]:
    """Return country codes that have at least one city in the gold table."""
    query = f"""
        SELECT DISTINCT country
        FROM {GOLD_CITIES_TABLE}
        ORDER BY country
    """
    return execute_query(query)["country"].tolist()


def get_cities(country: str) -> list[str]:
    """Return city/town names for a given country code."""
    query = f"""
        SELECT DISTINCT city_name
        FROM {GOLD_CITIES_TABLE}
        WHERE country = '{country}'
        ORDER BY city_name
    """
    return execute_query(query)["city_name"].tolist()


# ── Core pipeline queries ───────────────────────────────────────────────────


def get_city_polygon(country: str, city: str) -> dict:
    """Return city geometry metadata from the gold table.

    Returns dict with keys: geom_wkt, has_polygon.
    """
    query = f"""
        SELECT geom_wkt, has_polygon
        FROM {GOLD_CITIES_TABLE}
        WHERE country = '{country}' AND city_name = '{city}'
        LIMIT 1
    """
    df = execute_query(query)
    if df.empty:
        raise ValueError(f"City not found: {city}, {country}")
    return df.iloc[0].to_dict()


def tessellate_city(country: str, city: str, resolution: int) -> pd.DataFrame:
    """H3-tessellate a city polygon and return cell IDs with centre coordinates.

    Uses the real polygon boundary from gold_cities (geom_wkt) so that
    H3 cells follow the actual city shape rather than a bounding box.

    Returns DataFrame with columns: h3_cell, center_lat, center_lon
    """
    query = f"""
        WITH city_poly AS (
            SELECT geom_wkt
            FROM {GOLD_CITIES_TABLE}
            WHERE country = '{country}' AND city_name = '{city}'
            LIMIT 1
        ),
        cells AS (
            SELECT explode(h3_polyfillash3(geom_wkt, {resolution})) AS h3_cell
            FROM city_poly
        )
        SELECT
            h3_cell,
            CAST(h3_centerasgeojson(h3_cell):coordinates[1] AS DOUBLE) AS center_lat,
            CAST(h3_centerasgeojson(h3_cell):coordinates[0] AS DOUBLE) AS center_lon
        FROM cells
    """
    return execute_query(query)


def get_pois_with_h3(
    country: str,
    city: str,
    resolution: int,
    categories: list[str],
) -> pd.DataFrame:
    """Extract POIs inside the city polygon, assign each to an H3 cell.

    Uses a bbox pre-filter for scan efficiency, then restricts to H3
    cells produced by h3_polyfillash3 on the actual city polygon.  This
    ensures coverage follows the real city boundary while keeping the
    query fast.

    Returns DataFrame with columns:
        poi_id, category, lon, lat, address, h3_cell
    """
    cat_list = ", ".join(f"'{c}'" for c in categories)
    query = f"""
        WITH city AS (
            SELECT geom_wkt,
                   bbox_xmin, bbox_xmax, bbox_ymin, bbox_ymax
            FROM {GOLD_CITIES_TABLE}
            WHERE country = '{country}' AND city_name = '{city}'
            LIMIT 1
        ),
        city_h3 AS (
            SELECT explode(h3_polyfillash3(geom_wkt, {resolution})) AS h3_cell
            FROM city
        ),
        bbox_pois AS (
            SELECT
                p.poi_id,
                p.category,
                p.lon,
                p.lat,
                p.address,
                h3_longlatash3(p.lon, p.lat, {resolution}) AS h3_cell
            FROM {GOLD_PLACES_TABLE} p
            CROSS JOIN city c
            WHERE p.lon BETWEEN c.bbox_xmin AND c.bbox_xmax
              AND p.lat BETWEEN c.bbox_ymin AND c.bbox_ymax
              AND p.category IN ({cat_list})
        )
        SELECT b.*
        FROM bbox_pois b
        INNER JOIN city_h3 ch ON b.h3_cell = ch.h3_cell
    """
    return execute_query(query)


def get_buildings_with_h3(
    country: str,
    city: str,
    resolution: int,
) -> pd.DataFrame:
    """Extract buildings inside the city polygon, assign each to an H3 cell.

    Uses the same bbox pre-filter + polygon-cell join pattern as
    get_pois_with_h3.  Returns one row per building with both its
    building_category and height_bin so the caller can expand into
    feature rows.

    Returns DataFrame with columns:
        building_id, building_category, height_bin, lon, lat, h3_cell
    """
    query = f"""
        WITH city AS (
            SELECT geom_wkt,
                   bbox_xmin, bbox_xmax, bbox_ymin, bbox_ymax
            FROM {GOLD_CITIES_TABLE}
            WHERE country = '{country}' AND city_name = '{city}'
            LIMIT 1
        ),
        city_h3 AS (
            SELECT explode(h3_polyfillash3(geom_wkt, {resolution})) AS h3_cell
            FROM city
        ),
        bbox_bldg AS (
            SELECT
                b.building_id,
                b.building_category,
                b.height_bin,
                b.lon,
                b.lat,
                b.h3_cell
            FROM {GOLD_BUILDINGS_TABLE} b
            CROSS JOIN city c
            WHERE b.lon BETWEEN c.bbox_xmin AND c.bbox_xmax
              AND b.lat BETWEEN c.bbox_ymin AND c.bbox_ymax
        )
        SELECT bb.*
        FROM bbox_bldg bb
        INNER JOIN city_h3 ch ON bb.h3_cell = ch.h3_cell
    """
    return execute_query(query)


def get_buildings_around_points(
    locations: list[dict],
    resolution: int,
    k_ring: int = 2,
) -> pd.DataFrame:
    """Fetch buildings around arbitrary lat/lon points.

    Mirrors get_pois_around_points but queries the buildings table.

    Returns DataFrame with columns:
        building_id, building_category, height_bin, lon, lat, h3_cell
    """
    h3_cells_df = tessellate_points(locations, resolution, k_ring)
    if h3_cells_df.empty:
        return pd.DataFrame(
            columns=["building_id", "building_category", "height_bin",
                     "lon", "lat", "h3_cell"]
        )

    cell_set = set(h3_cells_df["h3_cell"].tolist())

    seen_centers: set[str] = set()
    bbox_clauses: list[str] = []
    for loc in locations:
        center_hex = _h3.latlng_to_cell(loc["lat"], loc["lon"], resolution)
        if center_hex in seen_centers:
            continue
        seen_centers.add(center_hex)

        disk = _h3.grid_disk(center_hex, k_ring)
        lats, lons = zip(*[_h3.cell_to_latlng(h) for h in disk])
        pad = 0.005
        bbox_clauses.append(
            f"(bbox_xmin >= {min(lons) - pad} AND bbox_xmax <= {max(lons) + pad} "
            f"AND bbox_ymin >= {min(lats) - pad} AND bbox_ymax <= {max(lats) + pad})"
        )

    bbox_filter = " OR ".join(bbox_clauses)
    query = f"""
        SELECT
            building_id,
            building_category,
            height_bin,
            lon,
            lat,
            h3_cell
        FROM {GOLD_BUILDINGS_TABLE}
        WHERE ({bbox_filter})
    """
    bldg_df = execute_query(query)
    bldg_df = bldg_df[bldg_df["h3_cell"].isin(cell_set)]
    return bldg_df


def build_count_vectors(pois_df: pd.DataFrame) -> pd.DataFrame:
    """Pivot POI data into a count-vector matrix (H3 cell x category).

    Returns a DataFrame indexed by h3_cell with one column per category,
    values are integer counts.
    """
    counts = (
        pois_df.groupby(["h3_cell", "category"])
        .size()
        .reset_index(name="count")
    )
    pivot = counts.pivot_table(
        index="h3_cell",
        columns="category",
        values="count",
        fill_value=0,
    )
    pivot.columns.name = None
    return pivot


def get_nearest_address_per_cell(pois_df: pd.DataFrame) -> dict[int, str]:
    """Pick a representative address for each H3 cell (first non-null)."""
    addr = (
        pois_df.dropna(subset=["address"])
        .drop_duplicates(subset=["h3_cell"])
        .set_index("h3_cell")["address"]
    )
    return addr.to_dict()


# ── Cross-city helpers (brand locations outside the target city) ─────────────


def tessellate_points(
    locations: list[dict],
    resolution: int,
    k_ring: int = 2,
) -> pd.DataFrame:
    """H3-tessellate neighborhoods around arbitrary lat/lon points.

    For each location creates a disk of H3 cells (center + k-ring neighbors)
    so the embedding model has enough spatial context.

    Returns DataFrame with columns: h3_cell, center_lat, center_lon
    """
    all_cells: set[str] = set()
    for loc in locations:
        center_hex = _h3.latlng_to_cell(loc["lat"], loc["lon"], resolution)
        all_cells.update(_h3.grid_disk(center_hex, k_ring))

    rows = []
    for hex_str in all_cells:
        lat, lng = _h3.cell_to_latlng(hex_str)
        rows.append({
            "h3_cell": _h3.str_to_int(hex_str),
            "center_lat": lat,
            "center_lon": lng,
        })
    return pd.DataFrame(rows)


def get_pois_around_points(
    locations: list[dict],
    resolution: int,
    categories: list[str],
    k_ring: int = 2,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch H3 cells and POIs around a list of lat/lon points.

    Builds a tight bounding box per unique brand-cell neighborhood and
    issues a single SQL query with OR-ed bbox filters for efficiency.

    Returns
    -------
    h3_cells_df : DataFrame (h3_cell, center_lat, center_lon)
    pois_df : DataFrame (poi_id, category, lon, lat, address, h3_cell)
    """
    h3_cells_df = tessellate_points(locations, resolution, k_ring)
    if h3_cells_df.empty:
        empty_pois = pd.DataFrame(
            columns=["poi_id", "category", "lon", "lat", "address", "h3_cell"]
        )
        return h3_cells_df, empty_pois

    cell_set = set(h3_cells_df["h3_cell"].tolist())

    cat_list = ", ".join(f"'{c}'" for c in categories)

    seen_centers: set[str] = set()
    bbox_clauses: list[str] = []
    for loc in locations:
        center_hex = _h3.latlng_to_cell(loc["lat"], loc["lon"], resolution)
        if center_hex in seen_centers:
            continue
        seen_centers.add(center_hex)

        disk = _h3.grid_disk(center_hex, k_ring)
        lats, lons = zip(*[_h3.cell_to_latlng(h) for h in disk])
        pad = 0.005
        bbox_clauses.append(
            f"(bbox_xmin >= {min(lons) - pad} AND bbox_xmax <= {max(lons) + pad} "
            f"AND bbox_ymin >= {min(lats) - pad} AND bbox_ymax <= {max(lats) + pad})"
        )

    bbox_filter = " OR ".join(bbox_clauses)
    query = f"""
        SELECT
            poi_id,
            category,
            lon,
            lat,
            address,
            h3_longlatash3(lon, lat, {resolution}) AS h3_cell
        FROM {GOLD_PLACES_TABLE}
        WHERE ({bbox_filter})
          AND category IN ({cat_list})
    """
    pois_df = execute_query(query)
    pois_df = pois_df[pois_df["h3_cell"].isin(cell_set)]
    return h3_cells_df, pois_df
