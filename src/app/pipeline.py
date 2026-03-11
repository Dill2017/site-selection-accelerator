"""DBSQL geospatial query pipeline.

All heavy spatial work — city polygon retrieval, H3 tessellation,
POI extraction with H3 cell assignment, and count-vector aggregation —
runs server-side on Databricks SQL using H3 and ST_* functions.
"""

from __future__ import annotations

import pandas as pd

from config import DIVISION_AREA_TABLE, DIVISION_TABLE, PLACES_TABLE
from db import execute_query


# ── Lookup helpers (populate dropdowns) ─────────────────────────────────────


def get_countries() -> list[str]:
    """Return ISO-2 country codes that have at least one city in the divisions catalog."""
    query = f"""
        SELECT DISTINCT country
        FROM {DIVISION_TABLE}
        WHERE subtype = 'locality'
          AND class IN ('city', 'town')
          AND country IS NOT NULL
        ORDER BY country
    """
    return execute_query(query)["country"].tolist()


def get_cities(country: str) -> list[str]:
    """Return city/town names for a given country code."""
    query = f"""
        SELECT DISTINCT names.primary AS city_name
        FROM {DIVISION_TABLE}
        WHERE subtype = 'locality'
          AND class IN ('city', 'town')
          AND country = '{country}'
          AND names.primary IS NOT NULL
        ORDER BY city_name
    """
    return execute_query(query)["city_name"].tolist()


# ── City polygon helpers ────────────────────────────────────────────────────

# Offset in degrees used to build a fallback bbox when no polygon exists.
# ~0.15 deg latitude ≈ 17 km; good default for city-scale analysis.
_BBOX_OFFSET_DEG = 0.15


def _get_city_center(country: str, city: str) -> tuple[float, float]:
    """Return (lon, lat) of the city point from the division catalog."""
    query = f"""
        SELECT
            bbox.xmin AS lon,
            bbox.ymin AS lat
        FROM {DIVISION_TABLE}
        WHERE subtype = 'locality'
          AND class IN ('city', 'town')
          AND country = '{country}'
          AND names.primary = '{city}'
        LIMIT 1
    """
    df = execute_query(query)
    if df.empty:
        raise ValueError(f"City not found: {city}, {country}")
    row = df.iloc[0]
    return float(row["lon"]), float(row["lat"])


def _city_has_polygon(country: str, city: str) -> bool:
    """Check whether division_area contains a polygon for this city."""
    query = f"""
        SELECT COUNT(*) AS cnt
        FROM {DIVISION_AREA_TABLE} da
        JOIN {DIVISION_TABLE} d ON da.division_id = d.id
        WHERE d.subtype = 'locality'
          AND d.class IN ('city', 'town')
          AND d.country = '{country}'
          AND d.names.primary = '{city}'
    """
    return int(execute_query(query).iloc[0]["cnt"]) > 0


def _polygon_wkt_cte(country: str, city: str) -> str:
    """Return a SQL CTE that produces a single geom_wkt column.

    Uses the real polygon from division_area if available, otherwise
    builds a rectangular polygon from the city centre point.
    """
    if _city_has_polygon(country, city):
        return f"""
            city_poly AS (
                SELECT ST_AsText(ST_GeomFromWKB(da.geom)) AS geom_wkt
                FROM {DIVISION_AREA_TABLE} da
                JOIN {DIVISION_TABLE} d ON da.division_id = d.id
                WHERE d.subtype = 'locality'
                  AND d.class IN ('city', 'town')
                  AND d.country = '{country}'
                  AND d.names.primary = '{city}'
                LIMIT 1
            )"""

    lon, lat = _get_city_center(country, city)
    off = _BBOX_OFFSET_DEG
    wkt = (
        f"POLYGON(("
        f"{lon - off} {lat - off}, "
        f"{lon + off} {lat - off}, "
        f"{lon + off} {lat + off}, "
        f"{lon - off} {lat + off}, "
        f"{lon - off} {lat - off}"
        f"))"
    )
    return f"city_poly AS (SELECT '{wkt}' AS geom_wkt)"


# ── Core pipeline queries ───────────────────────────────────────────────────


def get_city_bbox(country: str, city: str) -> dict:
    """Return the bounding-box used to pre-filter the places table.

    Returns dict with keys xmin, xmax, ymin, ymax.
    """
    if _city_has_polygon(country, city):
        query = f"""
            SELECT
                ST_XMin(ST_GeomFromWKB(da.geom)) AS xmin,
                ST_XMax(ST_GeomFromWKB(da.geom)) AS xmax,
                ST_YMin(ST_GeomFromWKB(da.geom)) AS ymin,
                ST_YMax(ST_GeomFromWKB(da.geom)) AS ymax
            FROM {DIVISION_AREA_TABLE} da
            JOIN {DIVISION_TABLE} d ON da.division_id = d.id
            WHERE d.subtype = 'locality'
              AND d.class IN ('city', 'town')
              AND d.country = '{country}'
              AND d.names.primary = '{city}'
            LIMIT 1
        """
        df = execute_query(query)
        return df.iloc[0].to_dict()

    lon, lat = _get_city_center(country, city)
    off = _BBOX_OFFSET_DEG
    return {
        "xmin": lon - off,
        "xmax": lon + off,
        "ymin": lat - off,
        "ymax": lat + off,
    }


def tessellate_city(country: str, city: str, resolution: int) -> pd.DataFrame:
    """H3-tessellate a city polygon and return cell IDs with centre coordinates.

    Returns DataFrame with columns: h3_cell, center_lat, center_lon
    """
    poly_cte = _polygon_wkt_cte(country, city)

    query = f"""
        WITH {poly_cte},
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
    """Extract POIs inside the city bbox, assign each to an H3 cell.

    Returns DataFrame with columns:
        poi_id, category, lon, lat, address, h3_cell
    """
    bbox = get_city_bbox(country, city)
    cat_list = ", ".join(f"'{c}'" for c in categories)

    query = f"""
        SELECT
            p.id                                     AS poi_id,
            p.categories.primary                     AS category,
            ST_X(ST_GeomFromWKB(p.geom))             AS lon,
            ST_Y(ST_GeomFromWKB(p.geom))             AS lat,
            p.addresses[0].freeform                  AS address,
            h3_longlatash3(
                ST_X(ST_GeomFromWKB(p.geom)),
                ST_Y(ST_GeomFromWKB(p.geom)),
                {resolution}
            ) AS h3_cell
        FROM {PLACES_TABLE} p
        WHERE p.categories.primary IN ({cat_list})
          AND p.bbox.xmin >= {bbox['xmin']}
          AND p.bbox.xmax <= {bbox['xmax']}
          AND p.bbox.ymin >= {bbox['ymin']}
          AND p.bbox.ymax <= {bbox['ymax']}
    """
    return execute_query(query)


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
