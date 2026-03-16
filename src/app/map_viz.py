"""pydeck map builder — CARTO basemap with H3 opportunity heatmap."""

from __future__ import annotations

import json
from typing import Optional

import h3
import numpy as np
import pandas as pd
import pydeck as pdk
from shapely import wkt

from explainability import tooltip_snippet


CARTO_BASEMAP = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"


def _h3_int_to_hex(cell_id: int) -> str:
    if cell_id < 0:
        cell_id = cell_id + (1 << 64)
    return h3.int_to_str(cell_id)


def _h3_center(cell_id: int) -> tuple[float, float]:
    hex_str = _h3_int_to_hex(cell_id)
    lat, lon = h3.cell_to_latlng(hex_str)
    return lat, lon


def _score_to_rgba(score: float) -> list[int]:
    """Map score 0..1 to a blue→green→red colour ramp with transparency."""
    r = int(np.clip(score * 2, 0, 1) * 255)
    g = int(np.clip(2 - score * 2, 0, 1) * 150)
    b = int((1 - score) * 200)
    return [r, g, b, 140]


def _wkt_to_geojson(wkt_str: str) -> dict | None:
    """Convert a WKT polygon string to a GeoJSON Feature."""
    try:
        geom = wkt.loads(wkt_str)
        return json.loads(json.dumps({
            "type": "Feature",
            "geometry": geom.__geo_interface__,
            "properties": {},
        }))
    except Exception:
        return None


def build_map(
    scored_df: pd.DataFrame,
    brand_locations: list[dict],
    h3_cells_df: pd.DataFrame,
    address_lookup: dict[int, str],
    top_n: int = 20,
    count_vectors: Optional[pd.DataFrame] = None,
    brand_avg: Optional[pd.Series] = None,
    competitor_pois: Optional[pd.DataFrame] = None,
    city_polygon_wkt: Optional[str] = None,
) -> pdk.Deck:
    """Build a pydeck Deck with H3 heatmap, brand dots, and top opportunities.

    Parameters
    ----------
    scored_df : DataFrame with h3_cell, similarity, is_brand_cell
        (and optionally opportunity_score, competitor_count, top_competitors).
    brand_locations : list of dicts with lat, lon.
    h3_cells_df : DataFrame with h3_cell, center_lat, center_lon.
    address_lookup : dict mapping h3_cell -> nearest address string.
    top_n : number of top opportunities to highlight.
    count_vectors : optional POI count matrix for tooltip explainability.
    brand_avg : optional brand average counts for tooltip comparison.
    competitor_pois : unused, kept for API compatibility.
    city_polygon_wkt : optional WKT string of the real city polygon boundary.
    """
    has_competition = "opportunity_score" in scored_df.columns
    # ── 1. H3 heatmap layer — coloured by similarity ───────────────────────
    whitespace = scored_df[~scored_df["is_brand_cell"]].copy()
    whitespace["hex_id"] = whitespace["h3_cell"].apply(_h3_int_to_hex)
    whitespace["color"] = whitespace["similarity"].apply(_score_to_rgba)
    whitespace["sim_pct"] = (whitespace["similarity"] * 100).round(1)

    if has_competition:
        whitespace["opp_pct"] = (whitespace["opportunity_score"] * 100).round(1)
        whitespace["competitor_count"] = whitespace["competitor_count"].fillna(0).astype(int)
        whitespace["top_competitors"] = whitespace["top_competitors"].fillna("")
    else:
        whitespace["opp_pct"] = whitespace["sim_pct"]
        whitespace["competitor_count"] = 0
        whitespace["top_competitors"] = ""

    if "poi_density" in whitespace.columns:
        whitespace["poi_count"] = whitespace["poi_density"].fillna(0).astype(int)
    else:
        whitespace["poi_count"] = 0

    whitespace["address"] = whitespace["h3_cell"].map(address_lookup).fillna("")

    if count_vectors is not None and brand_avg is not None:
        whitespace["cat_detail"] = whitespace["h3_cell"].apply(
            lambda cid: tooltip_snippet(cid, count_vectors, brand_avg)
        )
    else:
        whitespace["cat_detail"] = ""

    whitespace["brand_count"] = ""

    h3_layer = pdk.Layer(
        "H3HexagonLayer",
        data=whitespace,
        get_hexagon="hex_id",
        get_fill_color="color",
        get_line_color=[255, 255, 255, 60],
        line_width_min_pixels=1,
        extruded=False,
        pickable=True,
        opacity=0.7,
    )

    # ── 2. Brand locations — snapped to H3 centres, colour by density ──────
    resolution = h3.get_resolution(
        _h3_int_to_hex(scored_df["h3_cell"].iloc[0])
    )
    cell_counts: dict[str, int] = {}
    for loc in brand_locations:
        hex_id = h3.latlng_to_cell(loc["lat"], loc["lon"], resolution)
        cell_counts[hex_id] = cell_counts.get(hex_id, 0) + 1

    max_count = max(cell_counts.values()) if cell_counts else 1
    brand_rows: list[dict] = []
    for hex_id, count in cell_counts.items():
        clat, clon = h3.cell_to_latlng(hex_id)
        t = (count - 1) / max(max_count - 1, 1)
        lightness = int(255 - t * 200)
        brand_rows.append({
            "lat": clat,
            "lon": clon,
            "brand_count": count,
            "hex_id": hex_id,
            "color": [30, 50, lightness, 220],
            "radius": 120,
        })
    brand_df = pd.DataFrame(brand_rows) if brand_rows else pd.DataFrame(
        columns=["lat", "lon", "brand_count", "hex_id", "color", "radius"]
    )
    for col in ("address", "opp_pct", "sim_pct", "poi_count",
                "competitor_count", "top_competitors", "cat_detail"):
        brand_df[col] = ""

    brand_layer = pdk.Layer(
        "ScatterplotLayer",
        data=brand_df,
        get_position=["lon", "lat"],
        get_fill_color="color",
        get_radius="radius",
        pickable=True,
    )

    # ── 3. Top opportunities (green dots) — top 5% by opp score ─────────────
    opp_col = "opportunity_score" if has_competition else "similarity"
    sort_cols = [opp_col]
    if "poi_density" in whitespace.columns:
        sort_cols.append("poi_density")
    n_top = max(1, int(len(whitespace) * 0.02))
    top_opps = whitespace.sort_values(sort_cols, ascending=False).head(n_top).copy()
    centres = top_opps["h3_cell"].apply(
        lambda c: pd.Series(_h3_center(c), index=["lat", "lon"])
    )
    top_opps = pd.concat([top_opps.reset_index(drop=True), centres], axis=1)
    top_opps["color"] = [[0, 200, 80, 220]] * len(top_opps)
    top_opps["radius"] = 100

    top_layer = pdk.Layer(
        "ScatterplotLayer",
        data=top_opps,
        get_position=["lon", "lat"],
        get_fill_color="color",
        get_radius="radius",
        pickable=False,
    )

    layers = [h3_layer, brand_layer, top_layer]

    if city_polygon_wkt:
        feature = _wkt_to_geojson(city_polygon_wkt)
        if feature:
            boundary_layer = pdk.Layer(
                "GeoJsonLayer",
                data={"type": "FeatureCollection", "features": [feature]},
                get_line_color=[80, 80, 80, 200],
                get_fill_color=[0, 0, 0, 0],
                line_width_min_pixels=2,
                pickable=False,
                stroked=True,
                filled=False,
            )
            layers.insert(0, boundary_layer)

    # ── View state ──────────────────────────────────────────────────────────
    center_lat = h3_cells_df["center_lat"].mean()
    center_lon = h3_cells_df["center_lon"].mean()

    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=11,
        pitch=0,
    )

    # ── Tooltip ─────────────────────────────────────────────────────────────
    # Each row is a <div> that collapses to display:none when value is empty
    # via the :empty pseudo-selector on an inner <span>.
    def _row(label: str, field: str, suffix: str = "") -> str:
        val = f"{{{field}}}{suffix}"
        return (
            f'<div class="tt-row" data-field="{field}">'
            f"<b>{label}:</b> <span>{val}</span></div>"
        )

    tooltip_html = (
        "<style>"
        ".tt-row span:empty { display: none; }"
        ".tt-row:has(span:empty) { display: none; }"
        "</style>"
    )
    tooltip_html += _row("H3 Cell", "hex_id")
    tooltip_html += _row("Brand Locations", "brand_count")
    tooltip_html += _row("Address", "address")
    tooltip_html += _row("Opportunity Score", "opp_pct", "%")
    tooltip_html += _row("Similarity", "sim_pct", "%")
    tooltip_html += _row("POI Count", "poi_count")
    if has_competition:
        tooltip_html += _row("Competitors", "competitor_count")
        tooltip_html += _row("Top 3 Competitors", "top_competitors")
    tooltip_html += (
        "<hr style='margin:4px 0;border-color:#555'/>"
        "<b>POI Mix</b> (this cell / brand avg):<br/>"
        "{cat_detail}"
    )

    tooltip = {
        "html": tooltip_html,
        "style": {
            "backgroundColor": "#1a1a2e",
            "color": "white",
            "fontSize": "13px",
            "padding": "8px",
        },
    }

    return pdk.Deck(
        layers=layers,
        initial_view_state=view_state,
        map_style=CARTO_BASEMAP,
        tooltip=tooltip,
    )
