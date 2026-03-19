from __future__ import annotations

import json
import logging

import h3
import numpy as np
import pandas as pd
from fastapi import HTTPException, Query
from fastapi.responses import StreamingResponse

from .core import create_router
from .models import (
    AnalyzeRequest,
    AnalyzeResultOut,
    AppConfigOut,
    BrandLocationData,
    BrandPOIRow,
    BrandProfileOut,
    CategoryAvgItem,
    CategoryGroup,
    CellBreakdownRow,
    CompetitionInfo,
    CompetitorPOI,
    FingerprintRow,
    GenieDebugOut,
    HexagonData,
    HexagonDetailOut,
    VersionOut,
)
from . import cache
from .cache import PipelineResult

log = logging.getLogger(__name__)

router = create_router()


# -- Helpers ------------------------------------------------------------------

def _h3_int_to_hex(cell_id: int) -> str:
    if cell_id < 0:
        cell_id = cell_id + (1 << 64)
    return h3.int_to_str(cell_id)


def _h3_center(cell_id: int) -> tuple[float, float]:
    hex_str = _h3_int_to_hex(cell_id)
    return h3.cell_to_latlng(hex_str)


def _score_to_rgba(score: float) -> list[int]:
    r = int(np.clip(score * 2, 0, 1) * 255)
    g = int(np.clip(2 - score * 2, 0, 1) * 150)
    b = int((1 - score) * 200)
    return [r, g, b, 140]


def _get_result(session_id: str) -> PipelineResult:
    result = cache.get(session_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Session not found or expired")
    return result


# -- Version ------------------------------------------------------------------

@router.get("/version", response_model=VersionOut, operation_id="version")
async def version():
    return VersionOut.from_metadata()


# -- Config -------------------------------------------------------------------

@router.get("/config", response_model=AppConfigOut, operation_id="getConfig")
async def get_config():
    from config import (
        CATEGORY_GROUPS,
        BUILDING_CATEGORY_GROUPS,
        H3_RESOLUTIONS,
        DEFAULT_H3_RESOLUTION,
    )
    return AppConfigOut(
        h3_resolutions=H3_RESOLUTIONS,
        default_resolution=DEFAULT_H3_RESOLUTION,
        category_groups=[
            CategoryGroup(name=k, categories=v) for k, v in CATEGORY_GROUPS.items()
        ],
        building_category_groups=[
            CategoryGroup(name=k, categories=v) for k, v in BUILDING_CATEGORY_GROUPS.items()
        ],
    )


# -- Countries / Cities -------------------------------------------------------

@router.get("/countries", response_model=list[str], operation_id="listCountries")
async def list_countries():
    from pipeline import get_countries
    return get_countries()


@router.get("/cities", response_model=list[str], operation_id="listCities")
async def list_cities(country: str = Query(...)):
    from pipeline import get_cities
    return get_cities(country)


# -- Analyze (SSE) ------------------------------------------------------------

@router.post("/analyze", operation_id="analyze")
async def analyze(req: AnalyzeRequest) -> StreamingResponse:
    """Run the full site-selection pipeline, streaming progress via SSE.

    Final event contains the session_id for retrieving results.
    """
    def event_stream():
        try:
            yield _sse({"type": "progress", "step": "starting", "pct": 0})

            from pipeline import (
                build_count_vectors,
                get_buildings_around_points,
                get_buildings_with_h3,
                get_city_polygon,
                get_nearest_address_per_cell,
                get_pois_around_points,
                get_pois_with_h3,
                tessellate_city,
            )
            from embeddings import load_hex2vec, normalise_buildings, run_embedding_pipeline
            from similarity import compute_opportunity_score, compute_similarity
            from explainability import build_brand_profile, tooltip_snippet
            from brand_search import (
                discover_brand_locations,
                find_competitors_in_similar_cells,
                infer_location_categories,
            )
            from config import HEX2VEC_VOLUME_PATH, ALL_BUILDING_CATEGORIES

            # Load pretrained model
            yield _sse({"type": "progress", "step": "loading_model", "pct": 5})
            try:
                pretrained, pretrained_meta = load_hex2vec(HEX2VEC_VOLUME_PATH)
            except Exception:
                pretrained, pretrained_meta = None, None

            # Resolve brand locations
            yield _sse({"type": "progress", "step": "resolving_brand", "pct": 10})
            brand_pois_df = None
            if req.brand_input.mode == "brand_name":
                brand_locations, _, brand_pois_df = discover_brand_locations(
                    req.brand_input.value, req.resolution,
                    country=req.country, city=req.city,
                )
                if not brand_locations:
                    yield _sse({"type": "error", "message": f"No locations found for '{req.brand_input.value}'"})
                    return
            elif req.brand_input.mode == "map_selection":
                if not req.brand_input.geojson:
                    yield _sse({"type": "error", "message": "No map features provided"})
                    return
                brand_locations = _parse_map_selection(req.brand_input.geojson, req.resolution)
                if not brand_locations:
                    yield _sse({"type": "error", "message": "No valid locations from map selection"})
                    return
                if req.enable_competition and req.beta > 0:
                    brand_pois_df = infer_location_categories(
                        brand_locations, req.resolution, req.country, req.city,
                    )
            else:
                brand_locations = _parse_locations(req.brand_input.value, req.brand_input.mode)
                if not brand_locations:
                    yield _sse({"type": "error", "message": "No valid locations parsed"})
                    return
                if req.enable_competition and req.beta > 0:
                    brand_pois_df = infer_location_categories(
                        brand_locations, req.resolution, req.country, req.city,
                    )

            # Tessellate
            yield _sse({"type": "progress", "step": "tessellating", "pct": 15})
            city_h3_cells_df = tessellate_city(req.country, req.city, req.resolution)
            city_geo = get_city_polygon(req.country, req.city)
            _has_poly = city_geo.get("has_polygon")
            if isinstance(_has_poly, str):
                _has_poly = _has_poly.lower() == "true"
            city_polygon_wkt = city_geo["geom_wkt"] if _has_poly else None

            city_cell_set = set(city_h3_cells_df["h3_cell"].tolist())

            # Detect brand outside city
            brand_outside = []
            for loc in brand_locations:
                hex_str = h3.latlng_to_cell(loc["lat"], loc["lon"], req.resolution)
                if h3.str_to_int(hex_str) not in city_cell_set:
                    brand_outside.append(loc)

            h3_cells_df = city_h3_cells_df
            if brand_outside:
                yield _sse({"type": "progress", "step": "fetching_brand_context", "pct": 18})
                brand_ctx_cells, brand_ctx_pois = get_pois_around_points(
                    brand_outside, req.resolution, req.categories, k_ring=2,
                )
                new_cells = brand_ctx_cells[~brand_ctx_cells["h3_cell"].isin(city_cell_set)]
                h3_cells_df = pd.concat([city_h3_cells_df, new_cells], ignore_index=True)
            else:
                brand_ctx_pois = pd.DataFrame()

            # Query POIs
            yield _sse({"type": "progress", "step": "querying_pois", "pct": 20})
            city_pois_df = get_pois_with_h3(req.country, req.city, req.resolution, req.categories)
            if brand_outside and not brand_ctx_pois.empty:
                pois_df = pd.concat([city_pois_df, brand_ctx_pois], ignore_index=True).drop_duplicates(subset=["poi_id"])
            else:
                pois_df = city_pois_df

            if pois_df.empty:
                yield _sse({"type": "error", "message": "No POIs found for selected categories"})
                return

            # Buildings
            buildings_features = pd.DataFrame()
            if req.include_buildings:
                yield _sse({"type": "progress", "step": "querying_buildings", "pct": 32})
                city_bldg_df = get_buildings_with_h3(req.country, req.city, req.resolution)
                if brand_outside:
                    brand_ctx_bldg = get_buildings_around_points(brand_outside, req.resolution, k_ring=2)
                    if not brand_ctx_bldg.empty:
                        city_bldg_df = pd.concat([city_bldg_df, brand_ctx_bldg], ignore_index=True).drop_duplicates(subset=["building_id"])
                if not city_bldg_df.empty:
                    buildings_features = normalise_buildings(city_bldg_df)

            poi_features = pois_df[["poi_id", "category", "lon", "lat", "h3_cell"]].copy()
            poi_features = poi_features.rename(columns={"poi_id": "feature_id"})
            if not buildings_features.empty:
                features_df = pd.concat([poi_features, buildings_features], ignore_index=True)
            else:
                features_df = poi_features

            all_cats = list(req.categories)
            if req.include_buildings and not buildings_features.empty:
                present_bldg_cats = sorted(buildings_features["category"].unique())
                all_cats = all_cats + present_bldg_cats

            yield _sse({"type": "progress", "step": "building_vectors", "pct": 40})
            count_vectors = build_count_vectors(features_df)

            # Embeddings
            use_pretrained = (
                pretrained is not None
                and pretrained_meta is not None
                and pretrained_meta.get("resolution") == req.resolution
            )

            yield _sse({"type": "progress", "step": "generating_embeddings", "pct": 50})
            if use_pretrained and pretrained_meta is not None:
                training_cats = pretrained_meta["categories"]
                embeddings = run_embedding_pipeline(
                    h3_cells_df, features_df, all_cats,
                    pretrained_embedder=pretrained,
                    training_categories=training_cats,
                )
            else:
                embeddings = run_embedding_pipeline(h3_cells_df, features_df, all_cats)

            # Similarity
            yield _sse({"type": "progress", "step": "computing_similarity", "pct": 80})
            scored, brand_cells_in_emb = compute_similarity(embeddings, brand_locations, req.resolution)

            brand_profile = build_brand_profile(count_vectors, brand_cells_in_emb)
            brand_avg = brand_profile["avg"]

            scored = scored[scored["h3_cell"].isin(city_cell_set)].reset_index(drop=True)
            s_min, s_max = scored["similarity"].min(), scored["similarity"].max()
            if s_max - s_min > 0:
                scored["similarity"] = (scored["similarity"] - s_min) / (s_max - s_min)
            else:
                scored["similarity"] = np.zeros(len(scored))

            # Competition
            competitor_pois = None
            if req.enable_competition and req.beta > 0 and brand_pois_df is not None and not brand_pois_df.empty:
                yield _sse({"type": "progress", "step": "finding_competitors", "pct": 85})
                competition, competitor_pois = find_competitors_in_similar_cells(
                    scored, brand_pois=brand_pois_df,
                    brand_query=req.brand_input.value if req.brand_input.mode == "brand_name" else "",
                    min_similarity=0.5, country=req.country, city=req.city,
                    resolution=req.resolution,
                )
                if competitor_pois is not None and not competitor_pois.empty:
                    scored = compute_opportunity_score(scored, competition, beta=req.beta)

            poi_totals = count_vectors.sum(axis=1).rename("poi_density")
            scored = scored.merge(poi_totals, left_on="h3_cell", right_index=True, how="left")
            scored["poi_density"] = scored["poi_density"].fillna(0).astype(int)

            address_lookup = get_nearest_address_per_cell(pois_df)

            # Cache results
            yield _sse({"type": "progress", "step": "caching_results", "pct": 95})
            pr = PipelineResult(
                count_vectors=count_vectors,
                brand_avg=brand_avg,
                brand_profile=brand_profile,
                scored=scored,
                address_lookup=address_lookup,
                brand_locations=brand_locations,
                city_h3_cells_df=city_h3_cells_df,
                competitor_pois=competitor_pois,
                city_polygon_wkt=city_polygon_wkt,
                brand_pois=brand_pois_df,
            )
            session_id = cache.save(pr)

            # Build hexagon data for the response
            hexagons = _build_hexagon_list(scored, address_lookup, count_vectors, brand_avg)
            brand_locs = _build_brand_location_list(brand_locations, req.resolution)

            city_polygon_geojson = None
            if city_polygon_wkt:
                city_polygon_geojson = _wkt_to_geojson(city_polygon_wkt)

            result_data = AnalyzeResultOut(
                session_id=session_id,
                hexagons=hexagons,
                brand_locations=brand_locs,
                city_polygon_geojson=city_polygon_geojson,
                has_competition="opportunity_score" in scored.columns,
                center_lat=float(city_h3_cells_df["center_lat"].mean()),
                center_lon=float(city_h3_cells_df["center_lon"].mean()),
            )

            yield _sse({"type": "progress", "step": "done", "pct": 100})
            yield _sse({"type": "result", "data": result_data.model_dump()})

        except Exception as exc:
            log.exception("Pipeline error")
            yield _sse({"type": "error", "message": str(exc)})

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# -- Results ------------------------------------------------------------------

@router.get("/results/{session_id}", response_model=AnalyzeResultOut, operation_id="getResults")
async def get_results(session_id: str):
    pr = _get_result(session_id)
    hexagons = _build_hexagon_list(pr.scored, pr.address_lookup, pr.count_vectors, pr.brand_avg)
    brand_locs = _build_brand_location_list(
        pr.brand_locations,
        h3.get_resolution(_h3_int_to_hex(pr.scored["h3_cell"].iloc[0])),
    )
    city_polygon_geojson = _wkt_to_geojson(pr.city_polygon_wkt) if pr.city_polygon_wkt else None

    return AnalyzeResultOut(
        session_id=session_id,
        hexagons=hexagons,
        brand_locations=brand_locs,
        city_polygon_geojson=city_polygon_geojson,
        has_competition="opportunity_score" in pr.scored.columns,
        center_lat=float(pr.city_h3_cells_df["center_lat"].mean()),
        center_lon=float(pr.city_h3_cells_df["center_lon"].mean()),
    )


# -- Brand Profile ------------------------------------------------------------

@router.get(
    "/results/{session_id}/brand-profile",
    response_model=BrandProfileOut,
    operation_id="getBrandProfile",
)
async def get_brand_profile(session_id: str):
    pr = _get_result(session_id)
    from config import ALL_BUILDING_CATEGORIES, ALL_FEATURE_GROUPS

    brand_avg = pr.brand_avg
    avg_nonzero = brand_avg[brand_avg > 0].sort_values(ascending=False)

    avg_items: list[CategoryAvgItem] = []
    if not avg_nonzero.empty:
        bldg_set = set(ALL_BUILDING_CATEGORIES)
        group_lookup = {}
        for grp, cats in ALL_FEATURE_GROUPS.items():
            for c in cats:
                group_lookup[c] = grp

        avg_df = avg_nonzero.reset_index()
        avg_df.columns = ["category_raw", "avg_count"]
        avg_df["feature_type"] = avg_df["category_raw"].apply(
            lambda c: "Building" if c in bldg_set else "POI"
        )
        for ft in ("POI", "Building"):
            mask = avg_df["feature_type"] == ft
            ft_total = avg_df.loc[mask, "avg_count"].sum()
            if ft_total > 0:
                avg_df.loc[mask, "pct"] = (avg_df.loc[mask, "avg_count"] / ft_total * 100).round(1)
            else:
                avg_df.loc[mask, "pct"] = 0.0

        for _, row in avg_df.iterrows():
            cat_display = row["category_raw"].replace("_", " ").title()
            avg_items.append(CategoryAvgItem(
                category=cat_display,
                avg_count=round(float(row["avg_count"]), 2),
                pct_within_type=float(row.get("pct", 0)),
                feature_type=row["feature_type"],
                group=group_lookup.get(row["category_raw"], "Other"),
            ))

    # Per-cell breakdown
    cell_rows: list[CellBreakdownRow] = []
    brand_cells_df = pr.brand_profile["cells"]
    if not brand_cells_df.empty:
        for cell_id, row in brand_cells_df.iterrows():
            loc_label = pr.address_lookup.get(cell_id, _h3_int_to_hex(cell_id))
            for cat in brand_cells_df.columns:
                val = float(row[cat])
                if val > 0:
                    cell_rows.append(CellBreakdownRow(
                        location=loc_label,
                        category=cat.replace("_", " ").title(),
                        count=val,
                    ))

    return BrandProfileOut(avg_profile=avg_items, cell_breakdown=cell_rows)


# -- Hexagon Detail -----------------------------------------------------------

@router.get(
    "/results/{session_id}/hexagon/{hex_id}",
    response_model=HexagonDetailOut,
    operation_id="getHexagonDetail",
)
async def get_hexagon_detail(session_id: str, hex_id: str):
    pr = _get_result(session_id)
    h3_cell = h3.str_to_int(hex_id)
    from explainability import (
        build_fingerprint_df,
        explain_competition,
        summarise_fingerprint,
    )

    fingerprint_df = build_fingerprint_df(h3_cell, pr.count_vectors, pr.brand_avg)
    try:
        summary = summarise_fingerprint(fingerprint_df)
    except Exception as exc:
        log.warning("summarise_fingerprint error: %s", exc)
        summary = ""
    fp_rows: list[FingerprintRow] = []
    if not fingerprint_df.empty:
        for _, row in fingerprint_df.iterrows():
            fp_rows.append(FingerprintRow(
                category=row["Category"],
                group=row["Group"],
                feature_type=row["Feature Type"],
                this_location=float(row["This Location"]),
                brand_average=float(row["Brand Average"]),
                this_location_pct=float(row["This Location (%)"]),
                brand_average_pct=float(row["Brand Average (%)"]),
            ))

    comp_info = None
    if "opportunity_score" in pr.scored.columns:
        comp_raw = explain_competition(h3_cell, pr.scored)
        if comp_raw:
            comp_info = CompetitionInfo(**comp_raw)

    hex_id = _h3_int_to_hex(h3_cell)
    address = pr.address_lookup.get(h3_cell, "")

    cell_competitor_pois: list[CompetitorPOI] = []
    if pr.competitor_pois is not None and not pr.competitor_pois.empty:
        cell_comps = pr.competitor_pois[pr.competitor_pois["h3_hex"] == hex_id]
        for _, cr in cell_comps.iterrows():
            cell_competitor_pois.append(CompetitorPOI(
                name=str(cr.get("poi_primary_name", "")),
                category=str(cr.get("basic_category", cr.get("poi_primary_category", ""))),
                brand=str(cr.get("brand_name_primary", "") or ""),
                address=str(cr.get("address_line", "") or ""),
            ))

    row_match = pr.scored[pr.scored["h3_cell"] == h3_cell]
    similarity = float(row_match.iloc[0]["similarity"]) if not row_match.empty else 0.0
    opp_score = None
    poi_count = 0
    if not row_match.empty:
        r = row_match.iloc[0]
        if "opportunity_score" in r.index:
            opp_score = float(r["opportunity_score"])
        if "poi_density" in r.index:
            poi_count = int(r["poi_density"])

    return HexagonDetailOut(
        h3_cell=h3_cell,
        hex_id=hex_id,
        address=address,
        similarity=similarity,
        opportunity_score=opp_score,
        poi_count=poi_count,
        explanation_summary=summary,
        competition=comp_info,
        competitor_pois=cell_competitor_pois,
        fingerprint=fp_rows,
    )


# -- Genie Debug (brand POIs + competitor summary) ----------------------------

@router.get(
    "/results/{session_id}/debug",
    response_model=GenieDebugOut,
    operation_id="getGenieDebug",
)
async def get_genie_debug(session_id: str):
    pr = _get_result(session_id)

    brand_rows: list[BrandPOIRow] = []
    if pr.brand_pois is not None and not pr.brand_pois.empty:
        for _, row in pr.brand_pois.iterrows():
            brand_rows.append(BrandPOIRow(
                name=str(row.get("poi_primary_name", "")),
                category=str(row.get("basic_category", row.get("poi_primary_category", ""))),
                brand=str(row.get("brand_name_primary", "") or ""),
                lat=float(row["lat"]) if pd.notna(row.get("lat")) else None,
                lon=float(row["lon"]) if pd.notna(row.get("lon")) else None,
                h3_cell=str(row.get("h3_cell", "")),
            ))

    competitor_total = 0
    if pr.competitor_pois is not None and not pr.competitor_pois.empty:
        competitor_total = len(pr.competitor_pois)

    return GenieDebugOut(
        brand_pois=brand_rows,
        total_brand_pois=len(brand_rows),
        competitor_pois_total=competitor_total,
    )


# -- Internal helpers ---------------------------------------------------------

def _sse(data: dict) -> str:
    return f"data: {json.dumps(data, default=str)}\n\n"


def _parse_map_selection(geojson: dict, resolution: int) -> list[dict]:
    """Extract brand locations from drawn GeoJSON features.

    Points are used directly. Polygons are H3-polyfilled and converted
    to cell centroids so they integrate with the existing pipeline.
    """
    seen_cells: set[str] = set()
    locs: list[dict] = []

    for feature in geojson.get("features", []):
        geom = feature.get("geometry", {})
        geom_type = geom.get("type")
        coords = geom.get("coordinates")
        if not coords:
            continue

        if geom_type == "Point":
            lon, lat = coords[0], coords[1]
            hex_id = h3.latlng_to_cell(lat, lon, resolution)
            if hex_id not in seen_cells:
                seen_cells.add(hex_id)
                locs.append({"lat": lat, "lon": lon})

        elif geom_type == "Polygon":
            outer_ring = coords[0]
            h3_outer = [(pt[1], pt[0]) for pt in outer_ring]
            holes = [[(pt[1], pt[0]) for pt in ring] for ring in coords[1:]]
            poly = h3.LatLngPoly(h3_outer, *holes)
            cells = h3.polygon_to_cells(poly, resolution)
            for cell in cells:
                if cell not in seen_cells:
                    seen_cells.add(cell)
                    lat, lon = h3.cell_to_latlng(cell)
                    locs.append({"lat": lat, "lon": lon})

    return locs


def _parse_locations(text: str, mode: str) -> list[dict]:
    from geopy.geocoders import Nominatim
    locs: list[dict] = []
    geocoder = Nominatim(user_agent="site-selection-accelerator") if mode == "addresses" else None
    for line in text.strip().splitlines():
        line = line.strip()
        if not line:
            continue
        if mode == "latlng":
            parts = line.split(",")
            if len(parts) != 2:
                continue
            try:
                lat, lon = float(parts[0].strip()), float(parts[1].strip())
                locs.append({"lat": lat, "lon": lon})
            except ValueError:
                continue
        elif geocoder is not None:
            result = geocoder.geocode(line, timeout=10)
            if result:
                locs.append({"lat": result.latitude, "lon": result.longitude})
    return locs


def _build_hexagon_list(
    scored: pd.DataFrame,
    address_lookup: dict,
    count_vectors: pd.DataFrame,
    brand_avg: pd.Series,
) -> list[HexagonData]:
    from explainability import tooltip_snippet

    has_competition = "opportunity_score" in scored.columns
    hexagons: list[HexagonData] = []
    for _, row in scored.iterrows():
        cell = int(row["h3_cell"])
        hex_id = _h3_int_to_hex(cell)
        lat, lon = _h3_center(cell)
        sim = float(row["similarity"])
        opp = float(row["opportunity_score"]) if has_competition and pd.notna(row.get("opportunity_score")) else None
        hexagons.append(HexagonData(
            h3_cell=cell,
            hex_id=hex_id,
            similarity=round(sim, 4),
            opportunity_score=round(opp, 4) if opp is not None else None,
            is_brand_cell=bool(row["is_brand_cell"]),
            lat=lat,
            lon=lon,
            address=address_lookup.get(cell, ""),
            poi_count=int(row.get("poi_density", 0)),
            competitor_count=int(row.get("competitor_count", 0)) if has_competition else 0,
            top_competitors=str(row.get("top_competitors", "")) if has_competition else "",
            cat_detail=tooltip_snippet(cell, count_vectors, brand_avg, max_cats=4),
        ))
    return hexagons


def _build_brand_location_list(brand_locations: list[dict], resolution: int) -> list[BrandLocationData]:
    cell_counts: dict[str, int] = {}
    for loc in brand_locations:
        hex_id = h3.latlng_to_cell(loc["lat"], loc["lon"], resolution)
        cell_counts[hex_id] = cell_counts.get(hex_id, 0) + 1

    result = []
    for hex_id, count in cell_counts.items():
        lat, lon = h3.cell_to_latlng(hex_id)
        result.append(BrandLocationData(lat=lat, lon=lon, hex_id=hex_id, count=count))
    return result


def _wkt_to_geojson(wkt_str: str) -> dict | None:
    try:
        from shapely import wkt
        geom = wkt.loads(wkt_str)
        return json.loads(json.dumps({
            "type": "Feature",
            "geometry": geom.__geo_interface__,
            "properties": {},
        }))
    except Exception:
        return None
