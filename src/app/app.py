"""Site Selection Accelerator — Brand Site Matching.

Streamlit application that finds whitespace expansion opportunities
for retail brands using DBSQL geospatial functions, SRAI Hex2Vec
embeddings, and cosine-similarity scoring.
"""

from __future__ import annotations

import streamlit as st
from geopy.geocoders import Nominatim

from config import CATEGORY_GROUPS, DEFAULT_H3_RESOLUTION, H3_RESOLUTIONS
from embeddings import run_embedding_pipeline
from map_viz import build_map
from pipeline import (
    build_count_vectors,
    get_cities,
    get_countries,
    get_nearest_address_per_cell,
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
    "location profile against the geospatial makeup of target cities."
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

    h3_cells_df = tessellate_city(country, city, resolution)
    n_cells = len(h3_cells_df)
    progress.progress(15, text=f"City tessellated into {n_cells:,} H3 cells.")

    progress.progress(20, text="Extracting POIs from Overture Maps…")
    pois_df = get_pois_with_h3(country, city, resolution, selected_cats)
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
    scored = compute_similarity(embeddings, brand_locations, resolution)
    top_opps = get_top_opportunities(scored, top_n=20)

    address_lookup = get_nearest_address_per_cell(pois_df)

    progress.progress(95, text="Building map…")
    deck = build_map(scored, brand_locations, h3_cells_df, address_lookup)

    progress.progress(100, text="Done!")

    # -- Display results -----------------------------------------------------
    st.subheader("Whitespace Opportunity Map")

    col_legend, _ = st.columns([1, 2])
    with col_legend:
        st.markdown(
            "🔵 **Existing locations** &nbsp;|&nbsp; "
            "🟢 **Top opportunities** &nbsp;|&nbsp; "
            "🟥→🟦 **Similarity heatmap** (red = high, blue = low)"
        )

    st.pydeck_chart(deck)

    st.subheader("Top 20 Whitespace Opportunities")
    display = top_opps.copy()
    display["address"] = display["h3_cell"].map(address_lookup).fillna("—")
    display["similarity"] = (display["similarity"] * 100).round(1).astype(str) + "%"

    centres = display["h3_cell"].apply(_h3_center_for_table)
    display["latitude"] = centres.apply(lambda x: x[0])
    display["longitude"] = centres.apply(lambda x: x[1])

    st.dataframe(
        display[["address", "similarity", "latitude", "longitude"]],
        use_container_width=True,
        hide_index=True,
    )
