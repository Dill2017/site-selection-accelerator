"""Persist analysis results to Delta tables via DBSQL Statement Execution API.

Uses the same db.execute_query helper as the rest of the app to write
analysis artifacts into the catalog/schema configured in config.py.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone

import h3
import pandas as pd

from config import (
    ANALYSES_TABLE,
    ANALYSIS_BRAND_PROFILES_TABLE,
    ANALYSIS_COMPETITORS_TABLE,
    ANALYSIS_FINGERPRINTS_TABLE,
    ANALYSIS_HEXAGONS_TABLE,
    ALL_BUILDING_CATEGORIES,
    ALL_FEATURE_GROUPS,
)
from db import execute_query

log = logging.getLogger(__name__)

_tables_ensured = False


def _sql_str(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def _sql_val(value) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return _sql_str(str(value))


def ensure_analysis_tables() -> None:
    """Create analysis tables if they don't already exist."""
    global _tables_ensured
    if _tables_ensured:
        return

    ddl_statements = [
        f"""CREATE TABLE IF NOT EXISTS {ANALYSES_TABLE} (
            analysis_id STRING NOT NULL, session_id STRING NOT NULL,
            brand_input_mode STRING, brand_input_value STRING,
            country STRING, city STRING, h3_resolution INT,
            categories STRING, enable_competition BOOLEAN,
            beta DOUBLE, include_buildings BOOLEAN,
            city_polygon_geojson STRING,
            center_lat DOUBLE, center_lon DOUBLE,
            created_at TIMESTAMP, created_by STRING
        ) USING DELTA""",
        f"""CREATE TABLE IF NOT EXISTS {ANALYSIS_BRAND_PROFILES_TABLE} (
            analysis_id STRING NOT NULL, category STRING,
            avg_count DOUBLE, pct_within_type DOUBLE,
            feature_type STRING, group_name STRING
        ) USING DELTA""",
        f"""CREATE TABLE IF NOT EXISTS {ANALYSIS_HEXAGONS_TABLE} (
            analysis_id STRING NOT NULL, h3_cell BIGINT, hex_id STRING,
            similarity DOUBLE, opportunity_score DOUBLE,
            is_brand_cell BOOLEAN, lat DOUBLE, lon DOUBLE,
            address STRING, poi_count INT, competitor_count INT,
            top_competitors STRING
        ) USING DELTA""",
        f"""CREATE TABLE IF NOT EXISTS {ANALYSIS_FINGERPRINTS_TABLE} (
            analysis_id STRING NOT NULL, hex_id STRING,
            category STRING, group_name STRING, feature_type STRING,
            this_location DOUBLE, brand_average DOUBLE,
            this_location_pct DOUBLE, brand_average_pct DOUBLE,
            explanation_summary STRING
        ) USING DELTA""",
        f"""CREATE TABLE IF NOT EXISTS {ANALYSIS_COMPETITORS_TABLE} (
            analysis_id STRING NOT NULL, hex_id STRING,
            poi_name STRING, category STRING,
            brand STRING, address STRING
        ) USING DELTA""",
    ]

    for ddl in ddl_statements:
        try:
            execute_query(ddl)
        except Exception as e:
            log.warning("DDL execution warning: %s", e)

    _tables_ensured = True
    log.info("Analysis tables ensured")


def _h3_int_to_hex(cell_id: int) -> str:
    if cell_id < 0:
        cell_id = cell_id + (1 << 64)
    return h3.int_to_str(cell_id)


def persist_analysis(
    session_id: str,
    request_data: dict,
    pipeline_result,
    city_polygon_geojson: dict | None,
    center_lat: float,
    center_lon: float,
    user_identity: str = "",
    top_n_fingerprints: int = 20,
) -> dict:
    """Persist all analysis artifacts to Delta and return metadata.

    Parameters
    ----------
    session_id : str
        In-memory cache session id.
    request_data : dict
        The original AnalyzeRequest fields.
    pipeline_result : PipelineResult
        The cached pipeline result.
    city_polygon_geojson : dict | None
        City polygon GeoJSON for the analyses table.
    center_lat, center_lon : float
        Map center coordinates.
    user_identity : str
        User who triggered the analysis.
    top_n_fingerprints : int
        How many top hexagons to compute fingerprints for.

    Returns
    -------
    dict with analysis_id and list of tables written.
    """
    ensure_analysis_tables()

    analysis_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    _persist_analyses_row(analysis_id, session_id, request_data,
                          city_polygon_geojson, center_lat, center_lon,
                          now, user_identity)

    _persist_brand_profile(analysis_id, pipeline_result)
    _persist_hexagons(analysis_id, pipeline_result)
    _persist_competitors(analysis_id, pipeline_result)
    _persist_fingerprints(analysis_id, pipeline_result, top_n_fingerprints)

    log.info("Persisted analysis %s for session %s", analysis_id, session_id)
    return {
        "analysis_id": analysis_id,
        "tables_written": [
            ANALYSES_TABLE,
            ANALYSIS_BRAND_PROFILES_TABLE,
            ANALYSIS_HEXAGONS_TABLE,
            ANALYSIS_FINGERPRINTS_TABLE,
            ANALYSIS_COMPETITORS_TABLE,
        ],
    }


def _persist_analyses_row(
    analysis_id: str, session_id: str, req: dict,
    city_polygon_geojson: dict | None,
    center_lat: float, center_lon: float,
    created_at: str, created_by: str,
) -> None:
    poly_json = json.dumps(city_polygon_geojson) if city_polygon_geojson else None
    cats_json = json.dumps(req.get("categories", []))

    sql = f"""INSERT INTO {ANALYSES_TABLE} VALUES (
        {_sql_str(analysis_id)}, {_sql_str(session_id)},
        {_sql_str(req.get('brand_input_mode'))},
        {_sql_str(req.get('brand_input_value'))},
        {_sql_str(req.get('country'))}, {_sql_str(req.get('city'))},
        {_sql_val(req.get('resolution', 9))},
        {_sql_str(cats_json)},
        {_sql_val(req.get('enable_competition', True))},
        {_sql_val(req.get('beta', 1.0))},
        {_sql_val(req.get('include_buildings', True))},
        {_sql_str(poly_json)},
        {_sql_val(center_lat)}, {_sql_val(center_lon)},
        TIMESTAMP '{created_at}',
        {_sql_str(created_by)}
    )"""
    execute_query(sql)


def _persist_brand_profile(analysis_id: str, pr) -> None:
    brand_avg = pr.brand_avg
    avg_nonzero = brand_avg[brand_avg > 0]
    if avg_nonzero.empty:
        return

    bldg_set = set(ALL_BUILDING_CATEGORIES)
    group_lookup = {}
    for grp, cats in ALL_FEATURE_GROUPS.items():
        for c in cats:
            group_lookup[c] = grp

    rows: list[str] = []
    poi_total = sum(v for c, v in avg_nonzero.items() if c not in bldg_set)
    bldg_total = sum(v for c, v in avg_nonzero.items() if c in bldg_set)

    for cat, avg_count in avg_nonzero.items():
        ft = "Building" if cat in bldg_set else "POI"
        ft_total = bldg_total if cat in bldg_set else poi_total
        pct = round(avg_count / ft_total * 100, 1) if ft_total > 0 else 0.0
        grp = group_lookup.get(cat, "Other")
        rows.append(
            f"({_sql_str(analysis_id)}, {_sql_str(cat)}, "
            f"{_sql_val(round(float(avg_count), 2))}, {_sql_val(pct)}, "
            f"{_sql_str(ft)}, {_sql_str(grp)})"
        )

    if rows:
        batch_size = 100
        for i in range(0, len(rows), batch_size):
            values = ", ".join(rows[i:i + batch_size])
            execute_query(
                f"INSERT INTO {ANALYSIS_BRAND_PROFILES_TABLE} VALUES {values}"
            )


def _persist_hexagons(analysis_id: str, pr) -> None:
    scored = pr.scored
    has_competition = "opportunity_score" in scored.columns

    rows: list[str] = []
    for _, row in scored.iterrows():
        cell = int(row["h3_cell"])
        hex_id = _h3_int_to_hex(cell)
        opp = float(row["opportunity_score"]) if has_competition and pd.notna(row.get("opportunity_score")) else None
        addr = pr.address_lookup.get(cell, "")

        rows.append(
            f"({_sql_str(analysis_id)}, {_sql_val(cell)}, {_sql_str(hex_id)}, "
            f"{_sql_val(round(float(row['similarity']), 4))}, "
            f"{_sql_val(round(opp, 4) if opp is not None else None)}, "
            f"{_sql_val(bool(row['is_brand_cell']))}, "
            f"{_sql_val(float(h3.cell_to_latlng(_h3_int_to_hex(cell))[0]))}, "
            f"{_sql_val(float(h3.cell_to_latlng(_h3_int_to_hex(cell))[1]))}, "
            f"{_sql_str(addr)}, "
            f"{_sql_val(int(row.get('poi_density', 0)))}, "
            f"{_sql_val(int(row.get('competitor_count', 0)) if has_competition else 0)}, "
            f"{_sql_str(str(row.get('top_competitors', '')) if has_competition else '')})"
        )

    if rows:
        batch_size = 200
        for i in range(0, len(rows), batch_size):
            values = ", ".join(rows[i:i + batch_size])
            execute_query(
                f"INSERT INTO {ANALYSIS_HEXAGONS_TABLE} VALUES {values}"
            )


def _persist_competitors(analysis_id: str, pr) -> None:
    if pr.competitor_pois is None or pr.competitor_pois.empty:
        return

    rows: list[str] = []
    for _, cr in pr.competitor_pois.iterrows():
        hex_id = str(cr.get("h3_hex", ""))
        rows.append(
            f"({_sql_str(analysis_id)}, {_sql_str(hex_id)}, "
            f"{_sql_str(str(cr.get('poi_primary_name', '')))}, "
            f"{_sql_str(str(cr.get('basic_category', cr.get('poi_primary_category', ''))))}, "
            f"{_sql_str(str(cr.get('brand_name_primary', '') or ''))}, "
            f"{_sql_str(str(cr.get('address_line', '') or ''))})"
        )

    if rows:
        batch_size = 200
        for i in range(0, len(rows), batch_size):
            values = ", ".join(rows[i:i + batch_size])
            execute_query(
                f"INSERT INTO {ANALYSIS_COMPETITORS_TABLE} VALUES {values}"
            )


def _persist_fingerprints(analysis_id: str, pr, top_n: int) -> None:
    """Compute and persist fingerprints for the top N hexagons by similarity."""
    from explainability import build_fingerprint_df, summarise_fingerprint

    scored = pr.scored
    top_cells = scored.nlargest(top_n, "similarity")

    rows: list[str] = []
    for _, hex_row in top_cells.iterrows():
        cell = int(hex_row["h3_cell"])
        hex_id = _h3_int_to_hex(cell)

        fp_df = build_fingerprint_df(cell, pr.count_vectors, pr.brand_avg)
        if fp_df.empty:
            continue

        try:
            summary = summarise_fingerprint(fp_df)
        except Exception as e:
            log.warning("Fingerprint LLM failed for %s: %s", hex_id, e)
            summary = ""

        non_zero = fp_df[
            (fp_df["This Location"] > 0) | (fp_df["Brand Average"] > 0)
        ]
        for _, fp_row in non_zero.iterrows():
            rows.append(
                f"({_sql_str(analysis_id)}, {_sql_str(hex_id)}, "
                f"{_sql_str(fp_row['category_raw'])}, "
                f"{_sql_str(fp_row['Group'])}, "
                f"{_sql_str(fp_row['Feature Type'])}, "
                f"{_sql_val(float(fp_row['This Location']))}, "
                f"{_sql_val(float(fp_row['Brand Average']))}, "
                f"{_sql_val(float(fp_row['This Location (%)']))}, "
                f"{_sql_val(float(fp_row['Brand Average (%)']))}, "
                f"{_sql_str(summary)})"
            )

    if rows:
        batch_size = 200
        for i in range(0, len(rows), batch_size):
            values = ", ".join(rows[i:i + batch_size])
            execute_query(
                f"INSERT INTO {ANALYSIS_FINGERPRINTS_TABLE} VALUES {values}"
            )


def list_analyses(limit: int = 20) -> list[dict]:
    """Return recent analyses from the registry table."""
    try:
        df = execute_query(
            f"SELECT analysis_id, brand_input_value, city, country, created_at "
            f"FROM {ANALYSES_TABLE} "
            f"ORDER BY created_at DESC LIMIT {limit}"
        )
        if df.empty:
            return []
        return df.to_dict(orient="records")
    except Exception as e:
        log.warning("Could not list analyses: %s", e)
        return []
