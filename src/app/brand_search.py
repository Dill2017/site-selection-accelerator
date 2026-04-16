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
import re
import time
import unicodedata

import h3
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import ChatMessage, ChatMessageRole

import config as cfg

log = logging.getLogger(__name__)


def _clean_name(value: object) -> str:
    """Return a normalized name string, treating null-like values as empty."""
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _primary_shop_name(row: pd.Series) -> str:
    """Use brand_name_primary when present, otherwise fall back to poi_primary_name."""
    brand = _clean_name(row.get("brand_name_primary"))
    if brand:
        return brand
    return _clean_name(row.get("poi_primary_name"))


def h3_int_to_hex(val: int) -> str:
    """Convert any H3 integer to hex string, handling signed Databricks BIGINTs."""
    if val < 0:
        val = val + (1 << 64)
    return h3.int_to_str(val)


_ws_client: WorkspaceClient | None = None


def _get_workspace_client() -> WorkspaceClient:
    """Return a cached WorkspaceClient.

    On Databricks Apps the default Config() uses the service principal.
    Locally we use the DEFAULT profile (PAT) for authentication.
    """
    global _ws_client
    if _ws_client is not None:
        return _ws_client

    if os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("IS_DATABRICKS_APP"):
        _ws_client = WorkspaceClient()
    else:
        _ws_client = WorkspaceClient(profile="DEFAULT")

    return _ws_client


def _sql_escape(value: str) -> str:
    return value.replace("'", "''")


def _normalize_for_match(value: str) -> str:
    text = unicodedata.normalize("NFKD", value)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    return re.sub(r"[^a-z0-9]", "", text)


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
        return pd.DataFrame()

    if not msg.attachments:
        log.warning("Genie returned no attachments for: %s", question[:80])
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
    """Find brand POI locations via Genie Space.

    The query is sent directly to Genie as a brand/ILIKE search.

    Returns
    -------
    brand_locations : list of {lat, lon} dicts
    brand_cells : list of H3 cell IDs (BIGINT)
    brand_pois : DataFrame of matched POIs
    """
    _RETURN_COLS = (
        "poi_id, poi_primary_name, basic_category, "
        "brand_name_primary, address_line, lon, lat, "
        f"h3_h3tostring(h3_longlatash3(lon, lat, {resolution})) as h3_cell"
    )
    _SPATIAL_HINT = (
        "Use h3_polyfillash3 on the gold_cities polygon to get "
        "the H3 cells covering the city, then filter gold_places_enriched "
        f"where h3_longlatash3(lon, lat, {resolution}) is in that set. "
        f"Return {_RETURN_COLS}"
    )

    question = (
        f"Find all {query} locations within the city boundary "
        f"of {city}, {country}. Filter where brand_name_primary ILIKE "
        f"'%{query}%' OR poi_primary_name ILIKE '%{query}%'. "
        f"{_SPATIAL_HINT}"
    )
    log.info("Brand query '%s'", query)
    t0 = time.perf_counter()
    brand_pois = _ask_genie(question)
    log.info("Brand Genie lookup latency: %.2fs", time.perf_counter() - t0)

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
    restrict_to_target_city: bool = True,
) -> pd.DataFrame:
    """Infer source categories from nearest POI(s) to each input point.

    For each user-provided coordinate we pick the nearest POI row
    (exact row if coordinates match exactly, otherwise closest by
    lon/lat distance). This avoids using all POIs in the whole H3 cell,
    which can be noisy for malls/commercial hotspots.
    """
    from db import execute_query

    if not locations:
        return pd.DataFrame()

    # Keep arguments for compatibility; in nearest-point mode we intentionally
    # do not constrain to target city/country for source category inference.
    _ = (country, city, restrict_to_target_city)

    rows: list[pd.DataFrame] = []
    for loc in locations:
        lat = float(loc["lat"])
        lon = float(loc["lon"])
        source_addr = str(loc.get("source", "")).strip()
        # For geocoded free-text addresses, users often pass
        # "address_line, locality[, ...]". The table stores just address_line.
        # Match on parsed street line first to avoid false nearest-point fallback.
        source_parts = [p.strip() for p in source_addr.split(",") if p.strip()]
        source_line = source_parts[0] if source_parts else source_addr

        # Address mode: anchor to rows that actually share the address text.
        if source_addr:
            escaped = _sql_escape(source_line)
            norm_source = _normalize_for_match(source_line)
            addr_query = f"""
            SELECT p.poi_id, p.poi_primary_name, p.basic_category,
                   p.poi_primary_category, p.brand_name_primary,
                   p.address_line, p.lon, p.lat,
                   h3_h3tostring(h3_longlatash3(p.lon, p.lat, {resolution})) AS h3_cell
            FROM {cfg.GOLD_PLACES_ENRICHED} p
            WHERE p.lon IS NOT NULL AND p.lat IS NOT NULL
              AND lower(trim(p.address_line)) = lower(trim('{escaped}'))
            """
            try:
                addr_df = execute_query(addr_query)
                if addr_df.empty and norm_source:
                    # Fallback for formatting/diacritic differences in address text.
                    addr_query_fuzzy = f"""
                    SELECT p.poi_id, p.poi_primary_name, p.basic_category,
                           p.poi_primary_category, p.brand_name_primary,
                           p.address_line, p.lon, p.lat,
                           h3_h3tostring(h3_longlatash3(p.lon, p.lat, {resolution})) AS h3_cell
                    FROM {cfg.GOLD_PLACES_ENRICHED} p
                    WHERE p.lon IS NOT NULL AND p.lat IS NOT NULL
                      AND regexp_replace(
                            translate(lower(coalesce(p.address_line, '')),
                                      'áàäâãéèëêíìïîóòöôõúùüûñç',
                                      'aaaaaeeeeiiiiooooouuuunc'),
                            '[^a-z0-9]', ''
                          ) LIKE '%{_sql_escape(norm_source)}%'
                    """
                    addr_df = execute_query(addr_query_fuzzy)
                if not addr_df.empty:
                    # Address mode should keep all matched rows for that address.
                    # Only use nearest-point fallback when address matching fails.
                    rows.append(addr_df)
                    continue
            except Exception as e:
                log.warning("Address-based source lookup failed for '%s': %s", source_addr, e)

        query = f"""
        SELECT p.poi_id, p.poi_primary_name, p.basic_category,
               p.poi_primary_category, p.brand_name_primary,
               p.lon, p.lat,
               h3_h3tostring(h3_longlatash3(p.lon, p.lat, {resolution})) AS h3_cell
        FROM {cfg.GOLD_PLACES_ENRICHED} p
        WHERE p.lon IS NOT NULL AND p.lat IS NOT NULL
        ORDER BY POWER(p.lon - ({lon}), 2) + POWER(p.lat - ({lat}), 2)
        LIMIT 1
        """
        try:
            df = execute_query(query)
            if not df.empty:
                rows.append(df)
        except Exception as e:
            log.error("Nearest POI lookup failed for (%s,%s): %s", lat, lon, e)

    try:
        if not rows:
            return pd.DataFrame()
        merged = pd.concat(rows, ignore_index=True)
        if "poi_id" in merged.columns:
            merged = merged.drop_duplicates(subset=["poi_id"])
        return merged
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
    for _, row in brand_pois.iterrows():
        # Count one canonical category per POI row to avoid double-counting
        # when basic_category and poi_primary_category are identical.
        cat = _clean_name(row.get("basic_category")) or _clean_name(
            row.get("poi_primary_category")
        )
        if cat:
            counts[cat] = counts.get(cat, 0) + 1

    if not counts:
        return set()

    total = len(brand_pois)
    # For small/mixed anchor sets (common with lat/lon + address inputs),
    # avoid treating every single one-off category as a competitor category.
    threshold = max(total * min_pct, 2)
    above_threshold = {cat for cat, n in counts.items() if n >= threshold}

    # If nothing passes threshold, keep only the most frequent category(ies),
    # which approximates "average composition by dominant category".
    if not above_threshold:
        max_n = max(counts.values())
        return {cat for cat, n in counts.items() if n == max_n}

    if len(above_threshold) <= 1:
        return above_threshold

    sorted_cats = sorted(counts.items(), key=lambda x: -x[1])
    dominant = [cat for cat, _ in sorted_cats[:3]]

    # Address/lat-lon/map-selection modes have no explicit brand name, so
    # avoid brand-vertical LLM filtering and keep deterministic top categories.
    if not brand_query.strip():
        return set(dominant)

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
            messages=[ChatMessage(role=ChatMessageRole.USER, content=prompt)],
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
    resolution: int = 9,
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
    for _, brand_row in brand_pois.iterrows():
        name = _primary_shop_name(brand_row).lower()
        if name:
            exact_brand_names.add(name)

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
           h3_h3tostring(h3_longlatash3(p.lon, p.lat, {resolution})) AS h3,
           p.poi_primary_name, p.basic_category,
           p.poi_primary_category, p.brand_name_primary,
           p.address_line, p.locality, p.region, p.country
    FROM {cfg.GOLD_PLACES_ENRICHED} p
    WHERE h3_h3tostring(h3_longlatash3(p.lon, p.lat, {resolution})) IN ({h3_list})
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

    competitors["shop_name"] = competitors.apply(_primary_shop_name, axis=1)

    def _is_brand(row):
        name = _clean_name(row.get("shop_name")).lower()
        if name and name in exact_brand_names:
            return True
        if brand_query_lower and brand_query_lower in name:
            return True
        return False

    competitors = competitors[~competitors.apply(_is_brand, axis=1)].copy()
    competitors = competitors[competitors["shop_name"].str.len() > 0]

    if competitors.empty:
        return empty_agg, empty_pois

    competitors["h3_hex"] = competitors["h3"].astype(str)

    coords = competitors["h3_hex"].apply(
        lambda x: pd.Series(h3.cell_to_latlng(x), index=["lat", "lon"])
    )
    competitors = pd.concat(
        [competitors.reset_index(drop=True), coords], axis=1
    )

    global_popularity = competitors["shop_name"].dropna().value_counts()

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
            top_competitors=("shop_name", _top3_with_counts),
        )
        .reset_index()
    )

    return comp_per_cell, competitors
