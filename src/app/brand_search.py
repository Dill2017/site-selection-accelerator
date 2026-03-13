"""Brand discovery and competition analysis via Genie Space.

Uses a Databricks Genie Space backed by gold_places_enriched and
gold_cities tables.  Genie generates SQL from natural language,
using h3_polyfillash3 to convert city polygons into H3 cells for
fast spatial filtering (no expensive ST_CONTAINS JOINs).

If GENIE_SPACE_ID is not set in env or app_config, the app will
auto-provision one on first use.

For competition analysis, direct SQL on gold_places_enriched is
used since the parameters (H3 cells, categories) are already structured.
"""

from __future__ import annotations

import datetime
import json
import logging
import os

import h3
import pandas as pd
from databricks.sdk import WorkspaceClient

import config as cfg

log = logging.getLogger(__name__)


def h3_int_to_hex(val: int) -> str:
    """Convert any H3 integer to hex string, handling signed Databricks BIGINTs."""
    if val < 0:
        val = val + (1 << 64)
    return h3.int_to_str(val)


_ws_client: WorkspaceClient | None = None


def _get_workspace_client() -> WorkspaceClient:
    """Return a cached WorkspaceClient.

    On Databricks Apps the default Config() uses the service principal.
    Locally we use the DEFAULT profile (PAT) to avoid databricks-cli
    subprocess segfaults inside Streamlit.
    """
    global _ws_client
    if _ws_client is not None:
        return _ws_client

    if os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("IS_DATABRICKS_APP"):
        _ws_client = WorkspaceClient()
    else:
        _ws_client = WorkspaceClient(profile="DEFAULT")

    return _ws_client


# ── Genie Space resolution ───────────────────────────────────────────────────

_GENIE_DISPLAY_NAME = "Site Selection - Brand & Competition Explorer"


def _ensure_genie_space() -> str:
    """Return the GENIE_SPACE_ID, reading from config or searching by name.

    Resolution order:
    1. cfg.GENIE_SPACE_ID (env var or app_config table)
    2. Find existing space by display name via SDK
    """
    if cfg.GENIE_SPACE_ID:
        return cfg.GENIE_SPACE_ID

    w = _get_workspace_client()
    try:
        resp = w.genie.list_spaces()
        if resp and resp.spaces:
            for space in resp.spaces:
                if space.title == _GENIE_DISPLAY_NAME:
                    log.info("Found existing Genie Space: %s", space.space_id)
                    cfg.GENIE_SPACE_ID = space.space_id
                    return space.space_id
    except Exception as e:
        log.warning("Could not list Genie Spaces: %s", e)

    raise ValueError(
        "GENIE_SPACE_ID is not set and no matching space found. "
        "Run setup_genie_space.py or set the env var / app_config row."
    )


# ── Genie helpers ────────────────────────────────────────────────────────────


def _ask_genie(question: str, timeout_seconds: int = 120) -> pd.DataFrame:
    """Send a question to the Genie Space and return the result as a DataFrame.

    Genie generates the SQL; we then execute it via the DBSQL connector
    (from db.py) to get full, reliable results.
    """
    import streamlit as st
    from db import execute_query

    space_id = _ensure_genie_space()
    w = _get_workspace_client()

    log.info("Calling Genie space_id=%s, SDK has genie=%s", space_id, hasattr(w, 'genie'))

    try:
        msg = w.genie.start_conversation_and_wait(
            space_id=space_id,
            content=question,
            timeout=datetime.timedelta(seconds=timeout_seconds),
        )
    except Exception as e:
        log.error("Genie conversation failed: %s", e)
        st.error(f"Genie API error: {type(e).__name__}: {e}")
        return pd.DataFrame()

    if not msg.attachments:
        log.warning("Genie returned no attachments for: %s", question[:80])
        st.warning("Genie returned no SQL — it may need more instructions.")
        return pd.DataFrame()

    for attachment in msg.attachments:
        if attachment.query is not None:
            sql = attachment.query.query
            if not sql:
                continue
            log.info("Genie SQL for '%s': %s", question[:60], sql[:200])
            try:
                df = execute_query(sql)
                log.info("Genie → DBSQL returned %d rows", len(df))
                return df
            except Exception as e:
                log.error("Failed to execute Genie SQL: %s", e)
                st.error(f"Genie SQL execution failed: {e}")
                return pd.DataFrame()

    log.warning("Genie attachments had no query for: %s", question[:80])
    return pd.DataFrame()


# ── Brand discovery ──────────────────────────────────────────────────────────


def discover_brand_locations(
    query: str,
    resolution: int,
    country: str,
    city: str,
) -> tuple[list[dict], list[int], pd.DataFrame]:
    """Find a brand's existing locations via Genie Space.

    Genie uses h3_polyfillash3 to convert the city polygon into H3
    cells and filters POIs by cell membership for fast spatial queries.
    H3 cells are returned as hex strings.

    Returns
    -------
    brand_locations : list of {lat, lon} dicts
    brand_cells : list of H3 cell IDs (BIGINT)
    brand_pois : DataFrame of matched POIs
    """
    question = (
        f"Find all {query} locations within the city boundary of {city}, "
        f"{country}. Use h3_polyfillash3 on the gold_cities polygon to get "
        f"the H3 cells covering the city, then filter gold_places_enriched "
        f"where h3_longlatash3(lon, lat, {resolution}) is in that set. "
        f"Return poi_id, poi_primary_name, basic_category, "
        f"brand_name_primary, lon, lat, "
        f"h3_h3tostring(h3_longlatash3(lon, lat, {resolution})) as h3_cell"
    )

    brand_pois = _ask_genie(question)

    if brand_pois.empty:
        return [], [], brand_pois

    for col in ("lon", "lat"):
        if col in brand_pois.columns:
            brand_pois[col] = pd.to_numeric(brand_pois[col], errors="coerce")

    brand_pois = brand_pois.dropna(subset=["lon", "lat"])

    locations: list[dict] = []
    cells: list[int] = []
    for _, row in brand_pois.iterrows():
        locations.append({"lat": float(row["lat"]), "lon": float(row["lon"])})
        h3_val = row.get("h3_cell")
        if h3_val is not None and str(h3_val).strip():
            try:
                cells.append(h3.str_to_int(str(h3_val)))
            except Exception:
                pass

    return locations, cells, brand_pois


# ── Infer categories for lat/lon input mode ──────────────────────────────────


def infer_location_categories(
    locations: list[dict],
    resolution: int,
    country: str,
    city: str,
) -> pd.DataFrame:
    """Reverse-lookup POIs at the given coordinates to infer brand categories.

    Queries gold_places_enriched for POIs in the same H3 cells as the
    input locations, filtered by the city polygon.
    """
    from db import execute_query

    h3_hexes = set()
    for loc in locations:
        hex_str = h3.latlng_to_cell(loc["lat"], loc["lon"], resolution)
        h3_hexes.add(hex_str)

    if not h3_hexes:
        return pd.DataFrame()

    h3_list = ", ".join(f"'{h}'" for h in h3_hexes)

    query = f"""
    WITH city_h3 AS (
        SELECT explode(h3_polyfillash3(
            geom_wkt, {resolution}
        )) AS h3_cell
        FROM {cfg.GOLD_CITIES_TABLE}
        WHERE country = '{country}' AND city_name = '{city}'
    )
    SELECT p.poi_id, p.poi_primary_name, p.basic_category,
           p.poi_primary_category, p.brand_name_primary,
           p.lon, p.lat
    FROM {cfg.GOLD_PLACES_ENRICHED} p
    WHERE h3_longlatash3(p.lon, p.lat, {resolution})
          IN (SELECT h3_cell FROM city_h3)
      AND h3_h3tostring(h3_longlatash3(p.lon, p.lat, {resolution})) IN ({h3_list})
      AND p.lon IS NOT NULL AND p.lat IS NOT NULL
    """

    try:
        return execute_query(query)
    except Exception as e:
        log.error("Location category inference failed: %s", e)
        return pd.DataFrame()


# ── Category filtering ───────────────────────────────────────────────────────


def _filter_categories(
    brand_query: str,
    brand_pois: pd.DataFrame,
    min_pct: float = 0.05,
) -> set[str]:
    """Two-stage filter: frequency first, then LLM with industry context."""
    counts: dict[str, int] = {}
    for col in ("basic_category", "poi_primary_category"):
        if col in brand_pois.columns:
            for cat in brand_pois[col].dropna():
                counts[cat] = counts.get(cat, 0) + 1

    if not counts:
        return set()

    total = len(brand_pois)
    threshold = max(total * min_pct, 1)
    above_threshold = {cat for cat, n in counts.items() if n >= threshold}

    if len(above_threshold) <= 1:
        return above_threshold

    sorted_cats = sorted(counts.items(), key=lambda x: -x[1])
    dominant = [cat for cat, _ in sorted_cats[:3]]

    return _llm_industry_filter(brand_query, dominant, above_threshold)


def _llm_industry_filter(
    brand_query: str,
    dominant_categories: list[str],
    all_categories: set[str],
) -> set[str]:
    """Ask the LLM which categories are in the same industry vertical."""
    dominant_str = ", ".join(dominant_categories)
    cat_list = ", ".join(sorted(all_categories))

    prompt = f"""The brand "{brand_query}" primarily operates in these categories: {dominant_str}

Here are ALL categories found near this brand's locations:
{cat_list}

Return ONLY the categories that a COMPETITOR of "{brand_query}" would have.
A competitor is a business in the SAME industry vertical that serves the
same type of customer for the same type of need.

For example:
- If the brand is a cafe, competitors are other cafes, coffee shops, bakeries — NOT hair salons, gyms, or distributors.
- If the brand is a gym, competitors are other gyms, fitness centres — NOT restaurants or pharmacies.
- If the brand is a hotel, competitors are other hotels, lodgings, B&Bs — NOT convenience stores, supermarkets, or cafes.

Return a JSON array of matching category strings from the list.
Example: ["cafe", "coffee_shop", "bakery"]

Competitors:"""

    try:
        w = _get_workspace_client()
        response = w.serving_endpoints.query(
            name="databricks-claude-opus-4-6",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
            temperature=0.1,
        )
        raw = response.choices[0].message.content.strip()
        start = raw.find("[")
        end = raw.rfind("]") + 1
        if start >= 0 and end > start:
            filtered = json.loads(raw[start:end])
            result = {c for c in filtered if c in all_categories}
            if result:
                log.info(
                    "LLM industry filter for '%s' (dominant: %s): kept %s from %s",
                    brand_query, dominant_categories, result, all_categories,
                )
                return result
    except Exception as e:
        log.warning("LLM industry filter failed: %s — using dominant categories", e)

    return set(dominant_categories)


# ── Competition analysis ─────────────────────────────────────────────────────


def find_competitors_in_similar_cells(
    scored: pd.DataFrame,
    brand_pois: pd.DataFrame | None = None,
    brand_query: str = "",
    min_similarity: float = 0.5,
    country: str = "",
    city: str = "",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Find competitors in high-similarity cells via direct SQL on
    gold_places_enriched with polygon-based city filtering.

    Returns
    -------
    comp_per_cell : DataFrame with h3_hex, competitor_count, top_competitors
    competitor_pois : DataFrame of individual competitor POIs
    """
    from db import execute_query

    empty_agg = pd.DataFrame(
        columns=["h3_hex", "competitor_count", "top_competitors"]
    )
    empty_pois = pd.DataFrame()

    if brand_pois is None or brand_pois.empty:
        log.warning("No brand POIs, cannot search for competitors")
        return empty_agg, empty_pois

    brand_categories = _filter_categories(brand_query, brand_pois, min_pct=0.05)
    if not brand_categories:
        log.warning("No brand categories found, cannot search for competitors")
        return empty_agg, empty_pois

    brand_query_lower = brand_query.lower().strip() if brand_query else ""
    exact_brand_names: set[str] = set()
    if "brand_name_primary" in brand_pois.columns:
        exact_brand_names = set(
            brand_pois["brand_name_primary"].dropna().str.lower().unique()
        )

    is_brand = scored.get("is_brand_cell", False)
    candidate_cells = scored[
        (~is_brand) & (scored["similarity"] >= min_similarity)
    ]
    if candidate_cells.empty:
        return empty_agg, empty_pois

    h3_ints = candidate_cells["h3_cell"].tolist()
    h3_hexes = [h3_int_to_hex(c) for c in h3_ints]
    h3_list = ", ".join(f"'{h}'" for h in h3_hexes)
    cat_list = ", ".join(f"'{c}'" for c in brand_categories)

    query = f"""
    SELECT p.poi_id AS id,
           h3_h3tostring(h3_longlatash3(p.lon, p.lat, 9)) AS h3,
           p.poi_primary_name, p.basic_category,
           p.poi_primary_category, p.brand_name_primary,
           p.address_line, p.locality, p.region, p.country
    FROM {cfg.GOLD_PLACES_ENRICHED} p
    WHERE h3_h3tostring(h3_longlatash3(p.lon, p.lat, 9)) IN ({h3_list})
      AND (p.basic_category IN ({cat_list}) OR p.poi_primary_category IN ({cat_list}))
      AND p.lon IS NOT NULL AND p.lat IS NOT NULL
    """

    log.info(
        "Querying competitors: %d cells, %d categories: %s",
        len(h3_hexes), len(brand_categories), brand_categories,
    )

    try:
        competitors = execute_query(query)
    except Exception as e:
        log.error("Competitor query failed: %s", e)
        return empty_agg, empty_pois

    if competitors.empty:
        return empty_agg, empty_pois

    def _is_brand(row):
        name = str(row.get("poi_primary_name", "")).lower()
        brand = str(row.get("brand_name_primary", "")).lower()
        if brand and brand in exact_brand_names:
            return True
        if brand_query_lower and brand_query_lower in name:
            return True
        return False

    competitors = competitors[~competitors.apply(_is_brand, axis=1)].copy()

    if competitors.empty:
        return empty_agg, empty_pois

    competitors["h3_hex"] = competitors["h3"].astype(str)

    coords = competitors["h3_hex"].apply(
        lambda x: pd.Series(h3.cell_to_latlng(x), index=["lat", "lon"])
    )
    competitors = pd.concat(
        [competitors.reset_index(drop=True), coords], axis=1
    )

    global_popularity = competitors["poi_primary_name"].dropna().value_counts()

    def _top3_with_counts(names: pd.Series) -> str:
        cell_counts = names.dropna().value_counts()
        ranked = cell_counts.to_frame("cell_n")
        ranked["global_n"] = ranked.index.map(
            lambda n: global_popularity.get(n, 0)
        )
        ranked = ranked.sort_values(
            ["cell_n", "global_n"], ascending=[False, False]
        )
        return ", ".join(
            f"{name} ({row.cell_n})"
            for name, row in ranked.head(3).iterrows()
        )

    comp_per_cell = (
        competitors.groupby("h3_hex")
        .agg(
            competitor_count=("id", "size"),
            top_competitors=("poi_primary_name", _top3_with_counts),
        )
        .reset_index()
    )

    return comp_per_cell, competitors
