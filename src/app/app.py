"""Site Selection Accelerator — Brand Site Matching.

Streamlit application that finds whitespace expansion opportunities
for retail brands using DBSQL geospatial functions, SRAI Hex2Vec
embeddings, and cosine-similarity scoring.
"""

from __future__ import annotations

import altair as alt
import h3
import numpy as np
import pandas as pd
import streamlit as st
from geopy.geocoders import Nominatim

from config import CATEGORY_GROUPS, DEFAULT_H3_RESOLUTION, H3_RESOLUTIONS
from embeddings import run_embedding_pipeline
from explainability import build_brand_profile, explain_opportunity, summarise_explanation
from map_viz import build_map
from pipeline import (
    build_count_vectors,
    get_cities,
    get_countries,
    get_nearest_address_per_cell,
    get_pois_around_points,
    get_pois_with_h3,
    tessellate_city,
)
from similarity import compute_similarity, get_top_opportunities

# ── Page configuration ──────────────────────────────────────────────────────

st.set_page_config(
    page_title="Site Selection Accelerator",
    page_icon="📍",
    layout="wide",
)

st.title("Brand Site Matching")
st.markdown(
    "Find **whitespace expansion opportunities** by comparing your brand's "
    "location profile against the geospatial makeup of target cities. "
    "Brand locations can be in **any city** — the tool learns what "
    "neighbourhoods your brand thrives in and finds similar areas in the "
    "target market."
)

# ── Sidebar inputs ──────────────────────────────────────────────────────────

with st.sidebar:
    st.header("Configuration")

    # -- H3 Resolution -------------------------------------------------------
    resolution = st.selectbox(
        "H3 Resolution",
        options=H3_RESOLUTIONS,
        index=H3_RESOLUTIONS.index(DEFAULT_H3_RESOLUTION),
        help="Higher resolution = smaller hexagons, more detail, longer compute.",
    )

    # -- Country / City cascading selects ------------------------------------
    st.subheader("Target Market")

    @st.cache_data(show_spinner="Loading countries…")
    def _countries():
        return get_countries()

    country = st.selectbox("Country", options=_countries())

    @st.cache_data(show_spinner="Loading cities…")
    def _cities(c: str):
        return get_cities(c)

    city = st.selectbox("City", options=_cities(country) if country else [])

    # -- POI Category multi-select -------------------------------------------
    st.subheader("POI Categories")
    selected_cats: list[str] = []
    for group, cats in CATEGORY_GROUPS.items():
        with st.expander(group, expanded=True):
            chosen = st.multiselect(
                f"Select {group} categories",
                options=cats,
                default=cats,
                label_visibility="collapsed",
                key=f"cat_{group}",
            )
            selected_cats.extend(chosen)

    # -- Brand locations input -----------------------------------------------
    st.subheader("Your Brand Locations")
    input_mode = st.radio(
        "Input mode",
        ["Latitude / Longitude", "Addresses"],
        horizontal=True,
    )

    locations_text = st.text_area(
        "Enter one location per line"
        + (" (lat, lon)" if input_mode == "Latitude / Longitude" else " (full address)"),
        height=150,
        placeholder=(
            "51.5074, -0.1278\n51.5194, -0.1270"
            if input_mode == "Latitude / Longitude"
            else "10 Downing Street, London\n221B Baker Street, London"
        ),
    )

    run_button = st.button("🔍 Find Opportunities", type="primary", use_container_width=True)

# ── Helpers ─────────────────────────────────────────────────────────────────


def _parse_locations(text: str, mode: str) -> list[dict]:
    """Parse the user's location text into a list of {lat, lon} dicts."""
    locs: list[dict] = []
    geocoder = Nominatim(user_agent="site-selection-accelerator") if mode == "Addresses" else None

    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if mode == "Latitude / Longitude":
            parts = line.split(",")
            if len(parts) != 2:
                st.warning(f"Skipping invalid line: {line}")
                continue
            lat, lon = float(parts[0].strip()), float(parts[1].strip())
            locs.append({"lat": lat, "lon": lon})
        else:
            result = geocoder.geocode(line, timeout=10)
            if result is None:
                st.warning(f"Could not geocode: {line}")
                continue
            locs.append({"lat": result.latitude, "lon": result.longitude})
    return locs


def _h3_center_for_table(cell_id: int) -> tuple[float, float]:
    import h3 as _h3

    hex_str = _h3.int_to_str(cell_id)
    return _h3.cell_to_latlng(hex_str)


# ── Main execution ──────────────────────────────────────────────────────────

if run_button:
    # -- Validate inputs -----------------------------------------------------
    if not country or not city:
        st.error("Please select a target country and city.")
        st.stop()
    if not selected_cats:
        st.error("Please select at least one POI category.")
        st.stop()
    if not locations_text.strip():
        st.error("Please enter at least one brand location.")
        st.stop()

    brand_locations = _parse_locations(locations_text, input_mode)
    if not brand_locations:
        st.error("No valid brand locations were parsed. Please check your input.")
        st.stop()

    # -- Pipeline execution --------------------------------------------------
    progress = st.progress(0, text="Tessellating city with H3…")

    city_h3_cells_df = tessellate_city(country, city, resolution)
    n_cells = len(city_h3_cells_df)
    progress.progress(15, text=f"City tessellated into {n_cells:,} H3 cells.")

    # -- Detect brand locations outside the target city -----------------------
    city_cell_set = set(city_h3_cells_df["h3_cell"].tolist())
    brand_outside: list[dict] = []
    for loc in brand_locations:
        hex_str = h3.latlng_to_cell(loc["lat"], loc["lon"], resolution)
        if h3.str_to_int(hex_str) not in city_cell_set:
            brand_outside.append(loc)

    # -- Expand analysis to include brand-location neighbourhoods -------------
    if brand_outside:
        n_out = len(brand_outside)
        progress.progress(
            18,
            text=f"{n_out} brand location(s) outside target city — "
            f"fetching neighbourhood context…",
        )
        brand_ctx_cells, brand_ctx_pois = get_pois_around_points(
            brand_outside, resolution, selected_cats, k_ring=2
        )
        new_cells = brand_ctx_cells[
            ~brand_ctx_cells["h3_cell"].isin(city_cell_set)
        ]
        h3_cells_df = pd.concat(
            [city_h3_cells_df, new_cells], ignore_index=True
        )
    else:
        h3_cells_df = city_h3_cells_df

    progress.progress(20, text="Querying POIs…")
    city_pois_df = get_pois_with_h3(country, city, resolution, selected_cats)

    if brand_outside and not brand_ctx_pois.empty:
        pois_df = pd.concat(
            [city_pois_df, brand_ctx_pois], ignore_index=True
        ).drop_duplicates(subset=["poi_id"])
    else:
        pois_df = city_pois_df

    n_pois = len(pois_df)
    progress.progress(40, text=f"Found {n_pois:,} POIs in {len(selected_cats)} categories.")

    if pois_df.empty:
        st.warning("No POIs found for the selected categories in this city.")
        st.stop()

    progress.progress(45, text="Building count vectors…")
    count_vectors = build_count_vectors(pois_df)

    progress.progress(50, text="Training Hex2Vec embeddings (this may take a minute)…")
    embeddings = run_embedding_pipeline(h3_cells_df, pois_df, selected_cats)

    progress.progress(80, text="Computing similarity scores…")
    scored, brand_cells_in_emb = compute_similarity(
        embeddings, brand_locations, resolution
    )

    # -- Build explainability data -------------------------------------------
    brand_profile = build_brand_profile(count_vectors, brand_cells_in_emb)
    brand_avg = brand_profile["avg"]

    # -- Keep only target-city cells as expansion opportunities ---------------
    scored = scored[scored["h3_cell"].isin(city_cell_set)].reset_index(drop=True)

    s_min, s_max = scored["similarity"].min(), scored["similarity"].max()
    if s_max - s_min > 0:
        scored["similarity"] = (scored["similarity"] - s_min) / (s_max - s_min)
    else:
        scored["similarity"] = np.zeros(len(scored))

    top_opps = get_top_opportunities(scored, top_n=20)

    address_lookup = get_nearest_address_per_cell(pois_df)

    progress.progress(95, text="Building map…")
    deck = build_map(
        scored,
        brand_locations,
        city_h3_cells_df,
        address_lookup,
        count_vectors=count_vectors,
        brand_avg=brand_avg,
    )

    progress.progress(100, text="Done!")

    # -- Display results -----------------------------------------------------

    # ── Brand Location Profile ──────────────────────────────────────────────
    st.subheader("Brand Location Profile")
    st.caption(
        "Average POI category counts across your brand's existing locations. "
        "This is the baseline the similarity scores are compared against."
    )

    avg_nonzero = brand_avg[brand_avg > 0].sort_values(ascending=False)
    if not avg_nonzero.empty:
        avg_df = avg_nonzero.reset_index()
        avg_df.columns = ["Category", "Avg Count"]
        avg_df["Category"] = avg_df["Category"].str.replace("_", " ").str.title()

        group_lookup = {}
        for grp, cats in CATEGORY_GROUPS.items():
            for c in cats:
                group_lookup[c.replace("_", " ").title()] = grp
        avg_df["Group"] = avg_df["Category"].map(group_lookup).fillna("Other")

        avg_chart = (
            alt.Chart(avg_df)
            .mark_bar()
            .encode(
                x=alt.X("Avg Count:Q", title="Average POI Count"),
                y=alt.Y("Category:N", sort="-x", title=None),
                color=alt.Color(
                    "Group:N",
                    title="Category Group",
                    legend=alt.Legend(orient="bottom"),
                ),
                tooltip=["Category", "Avg Count", "Group"],
            )
            .properties(height=max(len(avg_df) * 22, 200))
        )
        st.altair_chart(avg_chart, use_container_width=True)
    else:
        st.info("No POI data found for the brand location cells.")

    with st.expander("Individual Brand Location Breakdown"):
        brand_cells_df = brand_profile["cells"]
        if not brand_cells_df.empty:
            brand_cells_display = brand_cells_df.copy()
            brand_cells_display.index = brand_cells_display.index.map(
                lambda c: address_lookup.get(c, h3.int_to_str(c))
            )
            non_zero_cols = brand_cells_display.columns[
                brand_cells_display.sum(axis=0) > 0
            ]
            heatmap_data = brand_cells_display[non_zero_cols]
            heatmap_data.columns = [
                c.replace("_", " ").title() for c in heatmap_data.columns
            ]

            melted = heatmap_data.reset_index().melt(
                id_vars="index", var_name="Category", value_name="Count"
            )
            melted.columns = ["Location", "Category", "Count"]

            heatmap = (
                alt.Chart(melted)
                .mark_rect()
                .encode(
                    x=alt.X("Category:N", title=None, axis=alt.Axis(labelAngle=-45)),
                    y=alt.Y("Location:N", title=None),
                    color=alt.Color(
                        "Count:Q",
                        scale=alt.Scale(scheme="blues"),
                        title="POI Count",
                    ),
                    tooltip=["Location", "Category", "Count"],
                )
                .properties(
                    height=max(len(heatmap_data) * 30, 100),
                )
            )
            st.altair_chart(heatmap, use_container_width=True)
        else:
            st.info("No count data available for brand cells.")

    # ── Whitespace Opportunity Map ──────────────────────────────────────────
    st.subheader("Whitespace Opportunity Map")

    col_legend, _ = st.columns([1, 2])
    with col_legend:
        st.markdown(
            "🔵 **Existing locations** &nbsp;|&nbsp; "
            "🟢 **Top opportunities** &nbsp;|&nbsp; "
            "🟥→🟦 **Similarity heatmap** (red = high, blue = low)"
        )
    st.caption("Hover over a hexagon to see its similarity score and top POI category comparison.")

    st.pydeck_chart(deck)

    # ── Top 20 Whitespace Opportunities ─────────────────────────────────────
    st.subheader("Top 20 Whitespace Opportunities")
    st.caption("Select a row to see a detailed category breakdown below the table.")

    display = top_opps.copy()
    display["address"] = display["h3_cell"].map(address_lookup).fillna("—")
    display["similarity_pct"] = (display["similarity"] * 100).round(1).astype(str) + "%"

    centres = display["h3_cell"].apply(_h3_center_for_table)
    display["latitude"] = centres.apply(lambda x: round(x[0], 5))
    display["longitude"] = centres.apply(lambda x: round(x[1], 5))

    selection = st.dataframe(
        display[["address", "similarity_pct", "latitude", "longitude"]].rename(
            columns={"similarity_pct": "similarity"}
        ),
        use_container_width=True,
        hide_index=True,
        on_select="rerun",
        selection_mode="single-row",
    )

    # ── Detail panel for selected opportunity ───────────────────────────────
    selected_rows = selection.selection.rows if selection.selection else []
    if selected_rows:
        sel_idx = selected_rows[0]
        sel_row = display.iloc[sel_idx]
        sel_cell = sel_row["h3_cell"]
        sel_addr = sel_row["address"]
        sel_sim = sel_row["similarity_pct"]

        exp = explain_opportunity(sel_cell, count_vectors, brand_avg)

        st.markdown(f"### {sel_addr}")
        st.markdown(f"**Similarity:** {sel_sim} &nbsp;|&nbsp; **{summarise_explanation(exp)}**")

        cell_counts = exp["counts"]
        comparison_cats = cell_counts.index[
            (cell_counts > 0) | (brand_avg.reindex(cell_counts.index, fill_value=0) > 0)
        ]
        if len(comparison_cats) > 0:
            comp_df = pd.DataFrame({
                "Category": [c.replace("_", " ").title() for c in comparison_cats],
                "This Location": [int(cell_counts[c]) for c in comparison_cats],
                "Brand Average": [round(brand_avg.get(c, 0), 1) for c in comparison_cats],
            })

            comp_melted = comp_df.melt(
                id_vars="Category", var_name="Source", value_name="Count"
            )

            comparison_chart = (
                alt.Chart(comp_melted)
                .mark_bar(opacity=0.85)
                .encode(
                    x=alt.X("Count:Q", title="POI Count"),
                    y=alt.Y("Category:N", sort="-x", title=None),
                    color=alt.Color(
                        "Source:N",
                        scale=alt.Scale(
                            domain=["This Location", "Brand Average"],
                            range=["#2ecc71", "#3498db"],
                        ),
                        title=None,
                        legend=alt.Legend(orient="top"),
                    ),
                    xOffset="Source:N",
                    tooltip=["Category", "Source", "Count"],
                )
                .properties(height=max(len(comp_df) * 28, 150))
            )
            st.altair_chart(comparison_chart, use_container_width=True)
        else:
            st.info("No POI categories present in this cell or the brand profile.")
