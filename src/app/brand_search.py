"""Brand discovery and competition analysis via text-feature embeddings.

Uses the existing Vector Search index on beatrice_liew.geospatial.site_embeddings
which embeds concatenated POI text features:
    name: X | category: Y | brand: Z | address: ... | locality: ... | country: ...

The index returns POIs with H3 cell assignments, allowing us to:
1. Discover a brand's existing locations from a free-text query
2. Identify competitor POIs and aggregate them per H3 cell
"""

from __future__ import annotations

import json
import logging
import os

import h3
import pandas as pd
from databricks.sdk import WorkspaceClient

from config import (
    BRAND_THRESHOLD,
    COMPETITOR_THRESHOLD,
    VS_COLUMNS,
    VS_INDEX_NAME,
)

log = logging.getLogger(__name__)


def h3_int_to_hex(val: int) -> str:
    """Convert any H3 integer to hex string, handling signed Databricks BIGINTs."""
    if val < 0:
        val = val + (1 << 64)
    return h3.int_to_str(val)

_ws_client: WorkspaceClient | None = None


def _get_workspace_client() -> WorkspaceClient:
    """Return a WorkspaceClient, caching after first successful init.

    On Databricks Apps, DATABRICKS_RUNTIME_VERSION is set and the default
    Config() works via service principal. Locally, we use the DEFAULT
    profile (PAT) to avoid the databricks-cli subprocess which segfaults
    inside Streamlit's multi-threaded environment.
    """
    global _ws_client
    if _ws_client is not None:
        return _ws_client

    if os.environ.get("DATABRICKS_RUNTIME_VERSION") or os.environ.get("IS_DATABRICKS_APP"):
        _ws_client = WorkspaceClient()
    else:
        _ws_client = WorkspaceClient(profile="DEFAULT")

    return _ws_client


def search_pois(
    query: str,
    num_results: int = 200,
    filters: dict | None = None,
) -> pd.DataFrame:
    """Search for POIs matching a query via Vector Search.

    Returns a DataFrame with the columns defined in ``VS_COLUMNS`` plus a
    ``score`` column from the index's similarity ranking.
    """
    w = _get_workspace_client()
    kwargs: dict = dict(
        index_name=VS_INDEX_NAME,
        query_text=query,
        columns=VS_COLUMNS,
        num_results=num_results,
    )
    if filters:
        kwargs["filters_json"] = json.dumps(filters)

    results = w.vector_search_indexes.query_index(**kwargs)
    columns = [col.name for col in results.manifest.columns]
    rows: list[dict] = []
    if results.result and results.result.data_array:
        for row in results.result.data_array:
            rows.append(dict(zip(columns, row)))

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    if "score" not in df.columns:
        df["score"] = 1.0 - (df.reset_index().index / len(df))

    return df


def _detect_category_intent(query: str) -> str | None:
    """Ask the LLM what business category the user is searching for.

    Returns a single category string suitable for use as a Vector Search
    filter on ``basic_category``, or *None* if detection fails.
    """
    prompt = f"""A user is searching for business locations with this query: "{query}"

What single business category best describes the type of place they are looking for?
Pick from common POI categories such as: hotel, cafe, coffee_shop, restaurant,
fast_food_restaurant, bar, bakery, gym, pharmacy, convenience_store, supermarket,
clothing_store, bank, hospital, car_rental, gas_station, shopping_mall, park,
department_store, hair_salon, beauty_salon, school, dentist, movie_theater,
furniture_store, professional_services, real_estate, education, lodging,
accommodation, pub, nightclub.

Return ONLY the single category string, nothing else. If unsure, return "unknown".
Example: hotel"""

    try:
        w = _get_workspace_client()
        response = w.serving_endpoints.query(
            name="databricks-claude-opus-4-6",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=30,
            temperature=0.0,
        )
        cat = response.choices[0].message.content.strip().lower().strip('"\'.')
        if cat and cat != "unknown":
            log.info("Intent detection for '%s': category = '%s'", query, cat)
            return cat
    except Exception as e:
        log.warning("Intent detection failed: %s", e)

    return None


def _refine_brand_pois(query: str, brand_pois: pd.DataFrame) -> pd.DataFrame:
    """Narrow brand POIs to the intended business type.

    Vector Search matches on shared words (e.g. "Premier Inn" also returns
    "Premier" convenience stores).  We refine in two stages:
    1. If brand_name_primary is available, prefer POIs whose brand name
       closely matches the query (≥3 hits).
    2. Otherwise, keep only POIs whose category matches the dominant
       category from the top-scoring results.
    """
    q_lower = query.lower().strip()

    # Stage 1: brand-name match (e.g. "premier inn" ⊂ brand_name_primary)
    if "brand_name_primary" in brand_pois.columns:
        def _brand_matches(val):
            if pd.isna(val):
                return False
            val_lower = str(val).lower()
            return q_lower in val_lower
        name_match = brand_pois[brand_pois["brand_name_primary"].apply(_brand_matches)]
        if len(name_match) >= 3:
            log.info(
                "Brand-name refinement for '%s': %d → %d POIs",
                query, len(brand_pois), len(name_match),
            )
            return name_match.copy()

    # Stage 2: dominant-category filter from highest-scoring results
    cat_col = None
    for col in ("basic_category", "poi_primary_category"):
        if col in brand_pois.columns:
            cat_col = col
            break
    if cat_col:
        top_hits = brand_pois.nlargest(min(20, len(brand_pois)), "score")
        dominant = top_hits[cat_col].mode()
        if not dominant.empty:
            core_cat = dominant.iloc[0]
            cat_match = brand_pois[brand_pois[cat_col] == core_cat]
            if len(cat_match) >= 3:
                log.info(
                    "Category refinement for '%s': keeping '%s' (%d → %d POIs)",
                    query, core_cat, len(brand_pois), len(cat_match),
                )
                return cat_match.copy()

    return brand_pois


def discover_brand_locations(
    query: str,
    resolution: int,
    country_filter: str | None = None,
    city_filter: str | None = None,
    threshold: float | None = None,
) -> tuple[list[dict], list[int], pd.DataFrame, pd.DataFrame]:
    """Find a brand's existing locations from a text query.

    Returns
    -------
    brand_locations : list of {lat, lon} dicts (feeds into existing pipeline)
    brand_cells : list of H3 cell IDs (BIGINT)
    brand_pois : DataFrame of the matched brand POIs (above threshold)
    all_results : DataFrame of ALL raw search results (for debugging)
    """
    if threshold is None:
        threshold = BRAND_THRESHOLD

    filters = {}
    if country_filter:
        filters["country"] = country_filter
    if city_filter:
        filters["locality"] = city_filter

    intent_cat = _detect_category_intent(query)

    if intent_cat:
        filtered_filters = {**filters, "basic_category": intent_cat}
        results = search_pois(query, num_results=200, filters=filtered_filters or None)
        if len(results) < 5:
            log.info(
                "Category filter '%s' returned only %d results — retrying without it",
                intent_cat, len(results),
            )
            results = search_pois(query, num_results=200, filters=filters or None)
    else:
        results = search_pois(query, num_results=200, filters=filters or None)

    if results.empty:
        return [], [], results, results

    brand_pois = results[results["score"] >= threshold].copy()
    if brand_pois.empty:
        return [], [], brand_pois, results

    brand_pois = _refine_brand_pois(query, brand_pois)

    locations: list[dict] = []
    cells: list[int] = []
    for _, row in brand_pois.iterrows():
        h3_hex = str(row["h3"])
        try:
            lat, lon = h3.cell_to_latlng(h3_hex)
        except Exception:
            log.warning("Invalid H3 cell '%s', skipping", h3_hex)
            continue
        locations.append({"lat": lat, "lon": lon})
        cells.append(h3.str_to_int(h3_hex))

    return locations, cells, brand_pois, results


def _filter_categories(
    brand_query: str,
    brand_pois: pd.DataFrame,
    min_pct: float = 0.05,
) -> set[str]:
    """Two-stage filter: frequency first, then LLM with industry context.

    1. Count category frequency across brand POIs.
    2. Identify the dominant categories (top by count).
    3. Ask the LLM which of the remaining categories belong to the
       same retail / consumer vertical as the dominant ones.
    """
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


def find_competitors_in_similar_cells(
    scored: pd.DataFrame,
    brand_pois: pd.DataFrame | None = None,
    brand_query: str = "",
    min_similarity: float = 0.5,
    country_filter: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Find competitors by looking at what businesses exist in cells with
    high similarity to the brand.

    This is data-driven: instead of guessing competitors, we look at
    the actual POIs in neighborhoods that match the brand's profile.

    Parameters
    ----------
    min_similarity : cells with similarity >= this value are searched.
        Default 0.5 matches the point where the heatmap turns red.

    Returns
    -------
    comp_per_cell : DataFrame with h3_hex, competitor_count, top_competitors
    competitor_pois : DataFrame of individual competitor POIs (for map layer)
    """
    from db import execute_query
    from config import ENRICHED_TABLE

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

    # Build exclusion sets — exact brand names + substring for the query
    brand_query_lower = brand_query.lower().strip() if brand_query else ""
    exact_brand_names: set[str] = set()
    if brand_pois is not None and "brand_name_primary" in brand_pois.columns:
        exact_brand_names = set(brand_pois["brand_name_primary"].dropna().str.lower().unique())

    # Take all cells above the similarity threshold (excluding brand cells)
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
    SELECT id, h3, poi_primary_name, basic_category,
           poi_primary_category, brand_name_primary,
           address_line, locality, region, country
    FROM {ENRICHED_TABLE}
    WHERE h3 IN ({h3_list})
      AND (basic_category IN ({cat_list}) OR poi_primary_category IN ({cat_list}))
    """
    if country_filter:
        query += f"\n      AND country = '{country_filter}'"

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

    # Exclude the brand itself: exact match on brand field, substring on query
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

    # Pre-compute global popularity (total locations across all cells)
    global_popularity = competitors["poi_primary_name"].dropna().value_counts()

    def _top3_with_counts(names: pd.Series) -> str:
        cell_counts = names.dropna().value_counts()
        # Sort by cell count first, then by global popularity as tiebreaker
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


