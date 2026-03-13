"""Site Selection Accelerator — Brand Site Matching.

Streamlit application that finds whitespace expansion opportunities
for retail brands using DBSQL geospatial functions, SRAI Hex2Vec
embeddings, and cosine-similarity scoring.
"""

from __future__ import annotations

from dotenv import load_dotenv
load_dotenv()

import altair as alt
import h3
import numpy as np
import pandas as pd
import streamlit as st
from geopy.geocoders import Nominatim

from brand_search import (
    discover_brand_locations,
    find_competitors_in_similar_cells,
    infer_location_categories,
)
from config import CATEGORY_GROUPS, DEFAULT_H3_RESOLUTION, H3_RESOLUTIONS
from embeddings import run_embedding_pipeline
from explainability import (
    build_brand_profile,
    build_fingerprint_df,
    explain_competition,
    explain_opportunity,
    summarise_explanation,
)
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
from similarity import compute_opportunity_score, compute_similarity, get_top_opportunities

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

    _country_list = _countries()
    _default_country_idx = _country_list.index("GB") if "GB" in _country_list else 0
    country = st.selectbox("Country", options=_country_list, index=_default_country_idx)

    @st.cache_data(show_spinner="Loading cities…")
    def _cities(c: str):
        return get_cities(c)

    _city_list = _cities(country) if country else []
    _default_city_idx = _city_list.index("London") if "London" in _city_list else 0
    city = st.selectbox("City", options=_city_list, index=_default_city_idx)

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
        ["Brand Name", "Latitude / Longitude", "Addresses"],
        horizontal=True,
    )

    brand_query: str | None = None
    locations_text: str = ""

    if input_mode == "Brand Name":
        brand_query = st.text_input(
            "Brand or business type",
            placeholder="Starbucks, premium coffee chain, etc.",
        )
    else:
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

    # -- Competition analysis -----------------------------------------------
    st.subheader("Competition Analysis")
    enable_competition = st.checkbox("Enable competition penalty", value=True)
    if enable_competition:
        beta = st.slider(
            "Competition sensitivity (β)",
            min_value=0.0,
            max_value=1.0,
            value=1.0,
            step=0.1,
            help="0 = ignore competition, 1 = heavily penalise saturated areas",
        )
    else:
        beta = 0.0

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

    if input_mode == "Brand Name":
        if not brand_query or not brand_query.strip():
            st.error("Please enter a brand name or business type.")
            st.stop()
    elif not locations_text.strip():
        st.error("Please enter at least one brand location.")
        st.stop()

    # -- Resolve brand locations -------------------------------------------
    brand_pois_df = None
    if input_mode == "Brand Name":
        with st.spinner(f"Searching for '{brand_query}' locations via Genie…"):
            brand_locations, _, brand_pois_df = discover_brand_locations(
                brand_query, resolution, country=country, city=city,
            )

        if not brand_locations:
            st.warning(
                f"No locations found for '{brand_query}' in {city}, {country}. "
                "Try a different brand name or check that the Genie Space is configured."
            )
            st.stop()
        st.info(f"Found {len(brand_locations)} '{brand_query}' location(s) in {city}, {country}")

        with st.expander("🔍 Brand Search Debug", expanded=False):
            if brand_pois_df is not None and not brand_pois_df.empty:
                st.caption(f"**Genie results** ({len(brand_pois_df)} rows)")
                st.dataframe(brand_pois_df, use_container_width=True)

        if brand_pois_df is not None and not brand_pois_df.empty:
            cats = brand_pois_df.get("basic_category", pd.Series()).dropna().unique()[:5]
            if len(cats) > 0:
                st.caption(f"Brand categories: **{', '.join(cats)}**")
    else:
        brand_locations = _parse_locations(locations_text, input_mode)
        if not brand_locations:
            st.error("No valid brand locations were parsed. Please check your input.")
            st.stop()

        if enable_competition and beta > 0:
            with st.spinner("Inferring brand categories from nearby POIs…"):
                brand_pois_df = infer_location_categories(
                    brand_locations, resolution, country, city,
                )
            if brand_pois_df is not None and not brand_pois_df.empty:
                cats = brand_pois_df.get("basic_category", pd.Series()).dropna().unique()[:5]
                if len(cats) > 0:
                    st.caption(f"Inferred categories: **{', '.join(cats)}**")

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

    # -- Competition analysis (all input modes) ──────────────────────────────
    competitor_pois = None
    if enable_competition and beta > 0 and brand_pois_df is not None and not brand_pois_df.empty:
        progress.progress(85, text="Finding competitors in similar areas…")

        competition, competitor_pois = find_competitors_in_similar_cells(
            scored,
            brand_pois=brand_pois_df,
            brand_query=brand_query or "",
            min_similarity=0.5,
            country=country,
            city=city,
        )

        if competitor_pois is not None and not competitor_pois.empty:
            scored = compute_opportunity_score(scored, competition, beta=beta)

            # Debug: show the merged table so we can verify h3_hex matching
            with st.expander(
                f"🔍 DEBUG: Merged scored + competition ({len(scored)} rows, "
                f"{(scored['competitor_count'] > 0).sum()} with competitors)",
                expanded=True,
            ):
                debug_cols = [
                    c for c in [
                        "h3_cell", "h3_hex", "similarity",
                        "competitor_count", "top_competitors",
                        "competition_score", "opportunity_score",
                        "is_brand_cell",
                    ] if c in scored.columns
                ]
                has_comp = scored[scored["competitor_count"] > 0]
                st.caption(f"Rows with competitor_count > 0: {len(has_comp)}")
                if not has_comp.empty:
                    st.dataframe(
                        has_comp[debug_cols]
                        .sort_values("competitor_count", ascending=False)
                        .head(30),
                        use_container_width=True,
                        hide_index=True,
                    )
                else:
                    st.warning("No rows have competitor_count > 0 after merge!")
                    st.caption("Competition h3_hex sample:")
                    st.write(competition["h3_hex"].head(10).tolist())
                    st.caption("Scored h3_hex sample:")
                    if "h3_hex" in scored.columns:
                        st.write(scored["h3_hex"].head(10).tolist())
                    else:
                        st.write("h3_hex column missing from scored!")

        if not competition.empty and competitor_pois is not None and not competitor_pois.empty:
            with st.expander(
                f"📊 Competition per H3 cell ({len(competition)} cells)",
                expanded=True,
            ):
                st.dataframe(
                    competition.sort_values("competitor_count", ascending=False).head(30),
                    use_container_width=True,
                    hide_index=True,
                )

    poi_totals = count_vectors.sum(axis=1).rename("poi_density")
    scored = scored.merge(poi_totals, left_on="h3_cell", right_index=True, how="left")
    scored["poi_density"] = scored["poi_density"].fillna(0).astype(int)

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
        competitor_pois=competitor_pois,
    )

    progress.progress(100, text="Done!")

    st.session_state["results"] = {
        "count_vectors": count_vectors,
        "brand_avg": brand_avg,
        "brand_profile": brand_profile,
        "scored": scored,
        "top_opps": top_opps,
        "deck": deck,
        "address_lookup": address_lookup,
        "competitor_pois": competitor_pois,
        "brand_locations": brand_locations,
        "city_h3_cells_df": city_h3_cells_df,
        "pois_df": pois_df,
    }

# ── Display results (from session state, survives reruns) ───────────────────

if "results" not in st.session_state:
    st.stop()

_r = st.session_state["results"]
count_vectors = _r["count_vectors"]
brand_avg = _r["brand_avg"]
brand_profile = _r["brand_profile"]
scored = _r["scored"]
top_opps = _r["top_opps"]
deck = _r["deck"]
address_lookup = _r["address_lookup"]
competitor_pois = _r["competitor_pois"]

if True:
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

            heatmap_data.index.name = "Location"
            melted = heatmap_data.reset_index().melt(
                id_vars="Location", var_name="Category", value_name="Count"
            )

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
        if competitor_pois is None or competitor_pois.empty:
            st.markdown(
                "🔵 **Existing locations** &nbsp;|&nbsp; "
                "🟢 **Top opportunities** &nbsp;|&nbsp; "
                "🟥→🟦 **Similarity heatmap** (red = high, blue = low)"
            )
    has_competition = "opportunity_score" in scored.columns

    if has_competition:
        st.caption(
            "Hover over a hexagon to see opportunity score, similarity, "
            "competitor count, and top POI category comparison."
        )
    else:
        st.caption("Hover over a hexagon to see its similarity score and top POI category comparison.")

    if competitor_pois is not None and not competitor_pois.empty:
        st.markdown(
            "🔵 **Existing locations** &nbsp;|&nbsp; "
            "🟢 **Top opportunities** &nbsp;|&nbsp; "
            "🟥→🟦 **Score heatmap** (red = high, blue = low)"
        )

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

    table_cols = ["address", "similarity_pct", "latitude", "longitude"]
    rename_map = {"similarity_pct": "similarity"}

    if has_competition:
        display["opportunity_pct"] = (
            (display["opportunity_score"] * 100).round(1).astype(str) + "%"
        )
        display["competitors"] = display["competitor_count"].astype(int)
        table_cols = [
            "address", "opportunity_pct", "similarity_pct",
            "competitors", "latitude", "longitude",
        ]
        rename_map = {
            "opportunity_pct": "opportunity",
            "similarity_pct": "similarity",
            "competitors": "competitors",
        }

    selection = st.dataframe(
        display[table_cols].rename(columns=rename_map),
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

        # Score summary line
        score_parts = [f"**Vibe Match:** {sel_sim}"]
        if has_competition:
            comp_info = explain_competition(sel_cell, scored)
            if comp_info:
                opp_pct = f"{comp_info['opportunity_score'] * 100:.1f}%"
                score_parts.insert(0, f"**Opportunity:** {opp_pct}")
                score_parts.append(
                    f"**Competitors:** {comp_info['competitor_count']}"
                )
                if comp_info["top_competitors"]:
                    score_parts.append(
                        f"**Nearby:** {comp_info['top_competitors']}"
                    )
        score_parts.append(f"**{summarise_explanation(exp)}**")
        st.markdown(" &nbsp;|&nbsp; ".join(score_parts))

        try:
            fingerprint = build_fingerprint_df(sel_cell, count_vectors, brand_avg)
        except Exception as e:
            st.error(f"Error building fingerprint: {e}")
            fingerprint = pd.DataFrame()

        if fingerprint.empty:
            st.info("No POI categories present in this cell or the brand profile.")
        else:
            st.markdown("#### Category Fingerprint")
            st.caption(
                "Compare the full POI category distribution of this location "
                "against the brand average. Similar shapes indicate a strong vibe match."
            )

            col_chart, col_metric = st.columns([1, 1])
            with col_chart:
                chart_style = st.radio(
                    "Chart style",
                    ["Line", "Bar"],
                    horizontal=True,
                    key="fp_chart_style",
                )
            with col_metric:
                metric = st.radio(
                    "Metric",
                    ["Counts", "% of Total"],
                    horizontal=True,
                    key="fp_metric",
                )

            if metric == "% of Total":
                val_col = "Value (%)"
                y_title = "% of Total POIs"
                fp_plot = fingerprint.rename(columns={
                    "This Location (%)": "This Location",
                    "Brand Average (%)": "Brand Average",
                })[["Category", "Group", "This Location", "Brand Average"]]
            else:
                val_col = "Value"
                y_title = "POI Count"
                fp_plot = fingerprint[["Category", "Group", "This Location", "Brand Average"]]

            cat_order = fp_plot["Category"].tolist()

            fp_melted = fp_plot.melt(
                id_vars=["Category", "Group"],
                var_name="Source",
                value_name=val_col,
            )

            color_scale = alt.Scale(
                domain=["This Location", "Brand Average"],
                range=["#2ecc71", "#3498db"],
            )

            if chart_style == "Line":
                base = alt.Chart(fp_melted).encode(
                    x=alt.X(
                        "Category:N",
                        sort=cat_order,
                        title=None,
                        axis=alt.Axis(labelAngle=-45),
                    ),
                    y=alt.Y(f"{val_col}:Q", title=y_title),
                    color=alt.Color(
                        "Source:N",
                        scale=color_scale,
                        title=None,
                        legend=alt.Legend(orient="top"),
                    ),
                    tooltip=["Category", "Group", "Source", f"{val_col}:Q"],
                )
                fp_chart = (
                    (base.mark_line(interpolate="monotone") + base.mark_point(size=30))
                    .properties(height=350)
                )
            else:
                fp_chart = (
                    alt.Chart(fp_melted)
                    .mark_bar(opacity=0.85)
                    .encode(
                        x=alt.X(
                            "Category:N",
                            sort=cat_order,
                            title=None,
                            axis=alt.Axis(labelAngle=-45),
                        ),
                        y=alt.Y(f"{val_col}:Q", title=y_title),
                        color=alt.Color(
                            "Source:N",
                            scale=color_scale,
                            title=None,
                            legend=alt.Legend(orient="top"),
                        ),
                        xOffset="Source:N",
                        tooltip=["Category", "Group", "Source", f"{val_col}:Q"],
                    )
                    .properties(height=350)
                )

            try:
                st.altair_chart(fp_chart, use_container_width=True)
            except Exception as e:
                st.error(f"Error rendering fingerprint chart: {e}")
