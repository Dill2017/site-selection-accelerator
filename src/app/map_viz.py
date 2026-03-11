"""pydeck map builder — CARTO basemap with H3 similarity heatmap."""

from __future__ import annotations

import h3
import numpy as np
import pandas as pd
import pydeck as pdk


CARTO_BASEMAP = "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json"


def _h3_int_to_hex(cell_id: int) -> str:
    return h3.int_to_str(cell_id)


def _h3_center(cell_id: int) -> tuple[float, float]:
    hex_str = h3.int_to_str(cell_id)
    lat, lon = h3.cell_to_latlng(hex_str)
    return lat, lon


def _similarity_to_rgba(score: float) -> list[int]:
    """Map similarity 0..1 to a blue→green→red colour ramp with transparency."""
    r = int(np.clip(score * 2, 0, 1) * 255)
    g = int(np.clip(2 - score * 2, 0, 1) * 150)
    b = int((1 - score) * 200)
    return [r, g, b, 140]


def build_map(
    scored_df: pd.DataFrame,
    brand_locations: list[dict],
    h3_cells_df: pd.DataFrame,
    address_lookup: dict[int, str],
    top_n: int = 20,
) -> pdk.Deck:
    """Build a pydeck Deck with three layers.

    Parameters
    ----------
    scored_df : DataFrame from ``compute_similarity`` with columns
        h3_cell, similarity, is_brand_cell.
    brand_locations : list of dicts with lat, lon.
    h3_cells_df : DataFrame with h3_cell, center_lat, center_lon.
    address_lookup : dict mapping h3_cell → nearest address string.
    top_n : number of top opportunities to highlight.
    """
    # ── 1. H3 heatmap layer (exclude brand cells) ──────────────────────────
    whitespace = scored_df[~scored_df["is_brand_cell"]].copy()
    whitespace["hex_id"] = whitespace["h3_cell"].apply(_h3_int_to_hex)
    whitespace["color"] = whitespace["similarity"].apply(_similarity_to_rgba)
    whitespace["address"] = whitespace["h3_cell"].map(address_lookup).fillna("—")
    whitespace["sim_pct"] = (whitespace["similarity"] * 100).round(1)

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

    # ── 2. Brand locations (blue dots) ──────────────────────────────────────
    brand_df = pd.DataFrame(brand_locations)
    brand_df["color"] = [[30, 100, 240, 220]] * len(brand_df)
    brand_df["radius"] = 120

    brand_layer = pdk.Layer(
        "ScatterplotLayer",
        data=brand_df,
        get_position=["lon", "lat"],
        get_fill_color="color",
        get_radius="radius",
        pickable=True,
    )

    # ── 3. Top opportunity centres (green dots) ─────────────────────────────
    top_opps = whitespace.head(top_n).copy()
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
        pickable=True,
    )

    # ── View state (center on the data) ─────────────────────────────────────
    center_lat = h3_cells_df["center_lat"].mean()
    center_lon = h3_cells_df["center_lon"].mean()

    view_state = pdk.ViewState(
        latitude=center_lat,
        longitude=center_lon,
        zoom=11,
        pitch=0,
    )

    tooltip = {
        "html": (
            "<b>Similarity:</b> {sim_pct}%<br/>"
            "<b>Address:</b> {address}<br/>"
            "<b>H3 Cell:</b> {hex_id}"
        ),
        "style": {
            "backgroundColor": "#1a1a2e",
            "color": "white",
            "fontSize": "13px",
            "padding": "8px",
        },
    }

    return pdk.Deck(
        layers=[h3_layer, brand_layer, top_layer],
        initial_view_state=view_state,
        map_style=CARTO_BASEMAP,
        tooltip=tooltip,
    )
