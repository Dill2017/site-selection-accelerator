"""Pre-train a Hex2Vec model on multiple cities and save to a UC Volume.

Runs as a DABs job task after the gold tables are created.
Follows the Hex2Vec paper methodology: fit on a diverse set of cities
so the encoder learns general urban spatial patterns, then save the
model for the app to load and transform at request time.

End users can also run this script directly:
    python train_hex2vec.py <catalog> <schema> <warehouse_id>
Or with env vars (loads from .env):
    python train_hex2vec.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import h3
import numpy as np
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Disposition, Format, StatementState
from shapely.geometry import Polygon
from srai.embedders import Hex2VecEmbedder
from srai.neighbourhoods import H3Neighbourhood

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Training configuration
# ---------------------------------------------------------------------------

ENCODER_SIZES = [48, 24, 12]
MAX_EPOCHS = 10
BATCH_SIZE = 256
H3_RESOLUTION = 9

TRAINING_CITIES: list[tuple[str, str]] = [
    ("RU", "Moscow"),
    ("GB", "London"),
    ("IT", "Rome"),
    ("US", "New York City"),
    ("DE", "Berlin"),
    ("FI", "Helsinki"),
    ("NO", "Oslo"),
    ("US", "Chicago"),
    ("PL", "Warszawa"),
    ("ES", "Madrid"),
    ("CZ", "Prague"),
    ("LT", "Vilnius"),
    ("KZ", "Nur-Sultan"),
    ("AT", "Vienna"),
    ("BY", "Minsk"),
    ("LV", "Riga"),
    ("RS", "Belgrade"),
    ("SK", "Bratislava"),
    ("US", "San Francisco"),
    ("PL", "Kraków"),
    ("PL", "Gdańsk"),
    ("PL", "Wrocław"),
    ("PL", "Łódź"),
    ("HR", "Zagreb"),
    ("SE", "Stockholm"),
    ("PL", "Poznań"),
    ("IS", "Reykjavík"),
    ("SI", "Ljubljana"),
    ("NL", "Amsterdam"),
    ("EE", "Tallinn"),
    ("BG", "Sofia"),
    ("IE", "Dublin"),
    ("FR", "Paris"),
    ("PT", "Lisbon"),
    ("LU", "Luxembourg City"),
    ("CH", "Bern"),
    ("BE", "Brussels"),
]

POI_CATEGORIES: list[str] = [
    "restaurant", "fast_food_restaurant", "cafe", "coffee_shop",
    "bar", "bakery", "food_truck",
    "clothing_store", "convenience_store", "grocery_store", "shopping",
    "furniture_store", "supermarket", "shopping_mall", "department_store",
    "bank", "pharmacy", "gas_station", "gym", "hospital", "dentist",
    "hair_salon", "beauty_salon", "automotive_repair",
    "movie_theater", "park", "hotel", "museum",
    "professional_services", "real_estate", "education", "school",
]

BUILDING_CATEGORIES: list[str] = [
    "bldg_residential", "bldg_commercial", "bldg_industrial",
    "bldg_agricultural", "bldg_transportation", "bldg_outbuilding",
    "bldg_other",
    "height_low_rise", "height_mid_rise", "height_high_rise",
    "height_skyscraper",
]

ALL_TRAINING_CATEGORIES: list[str] = POI_CATEGORIES + BUILDING_CATEGORIES

METADATA_FILENAME = "hex2vec_metadata.json"

# ---------------------------------------------------------------------------
# SQL execution helper
# ---------------------------------------------------------------------------


def _execute_sql(
    client: WorkspaceClient,
    warehouse_id: str,
    query: str,
    catalog: str | None = None,
    schema: str | None = None,
) -> pd.DataFrame:
    """Execute a SQL statement and return a DataFrame."""
    resp = client.statement_execution.execute_statement(
        statement=query,
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        wait_timeout="50s",
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
        byte_limit=26214400,
    )

    state = resp.status.state if resp.status else None
    if state in (StatementState.PENDING, StatementState.RUNNING):
        log.info("Query still running, polling…")
        while True:
            resp = client.statement_execution.get_statement(resp.statement_id)
            if resp.status.state in (
                StatementState.SUCCEEDED, StatementState.FAILED,
                StatementState.CANCELED, StatementState.CLOSED,
            ):
                break
            time.sleep(3)

    if resp.status and resp.status.state == StatementState.FAILED:
        msg = resp.status.error.message if resp.status.error else "Unknown"
        raise RuntimeError(f"SQL failed: {msg}\nQuery: {query[:200]}")

    if resp.manifest is None or resp.result is None:
        return pd.DataFrame()

    col_schemas = resp.manifest.schema.columns
    columns = [col.name for col in col_schemas]
    all_rows = list(resp.result.data_array or [])

    total_chunks = resp.manifest.total_chunk_count or 1
    if total_chunks > 1:
        for chunk_idx in range(1, total_chunks):
            chunk = client.statement_execution.get_statement_result_chunk_n(
                statement_id=resp.statement_id, chunk_index=chunk_idx,
            )
            if chunk.data_array:
                all_rows.extend(chunk.data_array)

    df = pd.DataFrame(all_rows, columns=columns)
    for col_schema in col_schemas:
        col_name = col_schema.name
        type_text = (col_schema.type_text or "").upper()
        if col_name not in df.columns or df[col_name].empty:
            continue
        try:
            if "BIGINT" in type_text or "LONG" in type_text:
                df[col_name] = df[col_name].apply(
                    lambda v: int(v) if v is not None else None
                )
                df[col_name] = df[col_name].astype("Int64")
            elif "INT" in type_text:
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce").astype("Int64")
            elif "DOUBLE" in type_text or "FLOAT" in type_text or "DECIMAL" in type_text:
                df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
        except Exception:
            pass

    log.info("SQL returned %d rows (%d chunks)", len(df), total_chunks)
    return df


# ---------------------------------------------------------------------------
# Data collection
# ---------------------------------------------------------------------------


def validate_cities(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    city_specs: list[tuple[str, str]],
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Check which training cities exist in gold_cities."""
    placeholders = ", ".join(
        f"('{country}', '{city}')" for country, city in city_specs
    )
    query = f"""
        SELECT DISTINCT country, city_name
        FROM {catalog}.{schema}.gold_cities
        WHERE (country, city_name) IN ({placeholders})
    """
    df = _execute_sql(client, warehouse_id, query)
    if df.empty:
        return [], list(city_specs)
    found_set = set(zip(df["country"], df["city_name"]))
    found = [cs for cs in city_specs if cs in found_set]
    missing = [cs for cs in city_specs if cs not in found_set]
    return found, missing


def tessellate_cities(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    city_specs: list[tuple[str, str]],
    resolution: int,
) -> pd.DataFrame:
    """H3-tessellate multiple cities and return the deduplicated union."""
    cities_table = f"{catalog}.{schema}.gold_cities"
    union_parts = []
    for country, city in city_specs:
        union_parts.append(f"""
            SELECT explode(h3_polyfillash3(geom_wkt, {resolution})) AS h3_cell
            FROM {cities_table}
            WHERE country = '{country}' AND city_name = '{city}'
        """)

    union_sql = " UNION ".join(union_parts)
    query = f"""
        WITH all_cells AS ({union_sql})
        SELECT DISTINCT
            h3_cell,
            CAST(h3_centerasgeojson(h3_cell):coordinates[1] AS DOUBLE) AS center_lat,
            CAST(h3_centerasgeojson(h3_cell):coordinates[0] AS DOUBLE) AS center_lon
        FROM all_cells
    """
    return _execute_sql(client, warehouse_id, query)


def fetch_pois(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    h3_cells_df: pd.DataFrame,
    resolution: int,
    categories: list[str],
) -> pd.DataFrame:
    """Fetch POIs that fall within the training H3 cells."""
    places_table = f"{catalog}.{schema}.gold_places"
    cell_set = set(h3_cells_df["h3_cell"].tolist())

    lat_min = h3_cells_df["center_lat"].min() - 0.05
    lat_max = h3_cells_df["center_lat"].max() + 0.05
    lon_min = h3_cells_df["center_lon"].min() - 0.05
    lon_max = h3_cells_df["center_lon"].max() + 0.05

    cat_list = ", ".join(f"'{c}'" for c in categories)
    query = f"""
        SELECT
            poi_id,
            category,
            lon,
            lat,
            h3_longlatash3(lon, lat, {resolution}) AS h3_cell
        FROM {places_table}
        WHERE lon BETWEEN {lon_min} AND {lon_max}
          AND lat BETWEEN {lat_min} AND {lat_max}
          AND category IN ({cat_list})
    """
    pois_df = _execute_sql(client, warehouse_id, query)
    return pois_df[pois_df["h3_cell"].isin(cell_set)].reset_index(drop=True)


def fetch_buildings(
    client: WorkspaceClient,
    warehouse_id: str,
    catalog: str,
    schema: str,
    h3_cells_df: pd.DataFrame,
) -> pd.DataFrame:
    """Fetch buildings that fall within the training H3 cells."""
    buildings_table = f"{catalog}.{schema}.gold_buildings"
    cell_set = set(h3_cells_df["h3_cell"].tolist())

    lat_min = h3_cells_df["center_lat"].min() - 0.05
    lat_max = h3_cells_df["center_lat"].max() + 0.05
    lon_min = h3_cells_df["center_lon"].min() - 0.05
    lon_max = h3_cells_df["center_lon"].max() + 0.05

    query = f"""
        SELECT
            building_id,
            building_category,
            height_bin,
            lon,
            lat,
            h3_cell
        FROM {buildings_table}
        WHERE lon BETWEEN {lon_min} AND {lon_max}
          AND lat BETWEEN {lat_min} AND {lat_max}
    """
    bldg_df = _execute_sql(client, warehouse_id, query)
    return bldg_df[bldg_df["h3_cell"].isin(cell_set)].reset_index(drop=True)


# ---------------------------------------------------------------------------
# SRAI GeoDataFrame builders (mirrors src/app/embeddings.py)
# ---------------------------------------------------------------------------


def _int_to_hex(cell_id: int) -> str:
    return h3.int_to_str(cell_id)


def _h3_hex_to_polygon(hex_id: str) -> Polygon:
    boundary = h3.cell_to_boundary(hex_id)
    return Polygon([(lon, lat) for lat, lon in boundary])


def _normalise_buildings(buildings_df: pd.DataFrame) -> pd.DataFrame:
    """Expand buildings into feature rows (category + height_bin)."""
    if buildings_df.empty:
        return pd.DataFrame(
            columns=["feature_id", "category", "lon", "lat", "h3_cell"]
        )
    cat_rows = buildings_df[
        ["building_id", "building_category", "lon", "lat", "h3_cell"]
    ].copy()
    cat_rows = cat_rows.rename(
        columns={"building_id": "feature_id", "building_category": "category"}
    )
    cat_rows["feature_id"] = "bc_" + cat_rows["feature_id"].astype(str)

    height_rows = buildings_df[buildings_df["height_bin"].notna()].copy()
    if not height_rows.empty:
        height_rows = height_rows[
            ["building_id", "height_bin", "lon", "lat", "h3_cell"]
        ]
        height_rows = height_rows.rename(
            columns={"building_id": "feature_id", "height_bin": "category"}
        )
        height_rows["feature_id"] = "bh_" + height_rows["feature_id"].astype(str)
        return pd.concat([cat_rows, height_rows], ignore_index=True)

    return cat_rows.reset_index(drop=True)


def build_regions_gdf(h3_cells_df: pd.DataFrame) -> gpd.GeoDataFrame:
    unique_ints = h3_cells_df["h3_cell"].unique()
    hex_ids = [_int_to_hex(c) for c in unique_ints]
    polys = [_h3_hex_to_polygon(h) for h in hex_ids]
    return gpd.GeoDataFrame(
        {"geometry": polys},
        index=pd.Index(hex_ids, name="region_id"),
        crs="EPSG:4326",
    )


def build_features_gdf(
    features_df: pd.DataFrame,
    categories: list[str],
) -> gpd.GeoDataFrame:
    geom = gpd.points_from_xy(features_df["lon"], features_df["lat"])
    gdf = gpd.GeoDataFrame(features_df, geometry=geom, crs="EPSG:4326")
    gdf = gdf.set_index("feature_id")
    gdf.index.name = "feature_id"

    dummies = pd.get_dummies(gdf["category"], dtype="int8")
    for cat in categories:
        if cat not in dummies.columns:
            dummies[cat] = np.zeros(len(dummies), dtype="int8")
    gdf = pd.concat([gdf, dummies[categories]], axis=1)
    gdf = gdf.drop(
        columns=["category", "lon", "lat", "address", "h3_cell"],
        errors="ignore",
    )
    return gdf


def build_joint_gdf(features_df: pd.DataFrame) -> pd.DataFrame:
    joint = features_df[["h3_cell", "feature_id"]].copy()
    joint["h3_cell"] = joint["h3_cell"].apply(_int_to_hex)
    joint = joint.rename(columns={"h3_cell": "region_id"})
    return joint.set_index(["region_id", "feature_id"])


# ---------------------------------------------------------------------------
# Main training pipeline
# ---------------------------------------------------------------------------


def main(catalog: str, schema: str, warehouse_id: str) -> str:
    """Train Hex2Vec on multiple cities and save to a UC Volume."""
    client = WorkspaceClient()

    # -- Create volume if needed ---------------------------------------------
    volume_path = f"/Volumes/{catalog}/{schema}/models"
    model_path = f"{volume_path}/hex2vec"

    _execute_sql(
        client, warehouse_id,
        f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.models",
    )

    # -- Validate training cities --------------------------------------------
    log.info("Validating %d training cities…", len(TRAINING_CITIES))
    found, missing = validate_cities(
        client, warehouse_id, catalog, schema, TRAINING_CITIES,
    )
    if missing:
        log.warning(
            "Missing %d cities from gold_cities: %s",
            len(missing),
            ", ".join(f"{c}/{n}" for c, n in missing),
        )
    if not found:
        raise RuntimeError("No training cities found in gold_cities.")
    log.info("Training on %d cities", len(found))

    # -- Tessellate all cities -----------------------------------------------
    log.info("Tessellating %d cities at resolution %d…", len(found), H3_RESOLUTION)
    h3_cells_df = tessellate_cities(
        client, warehouse_id, catalog, schema, found, H3_RESOLUTION,
    )
    log.info("Total H3 cells: %d", len(h3_cells_df))

    # -- Fetch POIs ----------------------------------------------------------
    log.info("Fetching POIs for %d categories…", len(POI_CATEGORIES))
    pois_df = fetch_pois(
        client, warehouse_id, catalog, schema,
        h3_cells_df, H3_RESOLUTION, POI_CATEGORIES,
    )
    log.info("POIs fetched: %d", len(pois_df))

    poi_features = pois_df[["poi_id", "category", "lon", "lat", "h3_cell"]].copy()
    poi_features = poi_features.rename(columns={"poi_id": "feature_id"})

    # -- Fetch buildings -----------------------------------------------------
    log.info("Fetching buildings…")
    bldg_df = fetch_buildings(
        client, warehouse_id, catalog, schema, h3_cells_df,
    )
    log.info("Buildings fetched: %d", len(bldg_df))

    bldg_features = _normalise_buildings(bldg_df)

    # -- Merge into unified features -----------------------------------------
    if not bldg_features.empty:
        features_df = pd.concat(
            [poi_features, bldg_features], ignore_index=True,
        )
    else:
        features_df = poi_features

    log.info("Total features: %d", len(features_df))

    # -- Build SRAI GeoDataFrames --------------------------------------------
    log.info("Building GeoDataFrames…")
    regions_gdf = build_regions_gdf(h3_cells_df)
    features_gdf = build_features_gdf(features_df, ALL_TRAINING_CATEGORIES)
    joint_gdf = build_joint_gdf(features_df)

    valid_regions = joint_gdf.index.get_level_values("region_id").unique()
    regions_gdf = regions_gdf.loc[regions_gdf.index.isin(valid_regions)]

    valid_features = joint_gdf.index.get_level_values("feature_id").unique()
    features_gdf = features_gdf.loc[features_gdf.index.isin(valid_features)]

    log.info(
        "Regions with features: %d / %d, Features: %d",
        len(regions_gdf), len(h3_cells_df), len(features_gdf),
    )

    if regions_gdf.empty or features_gdf.empty:
        raise RuntimeError("No feature data intersects any H3 cells.")

    # -- Fit Hex2Vec ---------------------------------------------------------
    neighbourhood = H3Neighbourhood(regions_gdf)
    log.info(
        "Fitting Hex2Vec: encoder=%s, epochs=%d, batch_size=%d",
        ENCODER_SIZES, MAX_EPOCHS, BATCH_SIZE,
    )

    embedder = Hex2VecEmbedder(encoder_sizes=ENCODER_SIZES)
    embedder.fit(
        regions_gdf, features_gdf, joint_gdf, neighbourhood,
        trainer_kwargs={"max_epochs": MAX_EPOCHS, "accelerator": "cpu"},
        batch_size=BATCH_SIZE,
    )
    log.info("Hex2Vec fitting complete.")

    # -- Save model and metadata ---------------------------------------------
    model_dir = Path(model_path)
    model_dir.mkdir(parents=True, exist_ok=True)

    embedder.save(model_dir)

    metadata = {
        "categories": ALL_TRAINING_CATEGORIES,
        "resolution": H3_RESOLUTION,
        "encoder_sizes": ENCODER_SIZES,
        "max_epochs": MAX_EPOCHS,
        "batch_size": BATCH_SIZE,
        "cities": [list(c) for c in found],
        "num_regions": len(regions_gdf),
        "num_features": len(features_gdf),
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    (model_dir / METADATA_FILENAME).write_text(json.dumps(metadata, indent=2))

    log.info("Model saved to %s", model_dir)
    return str(model_dir)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    try:
        from databricks.sdk.runtime import dbutils  # type: ignore[import]
        catalog = dbutils.widgets.get("catalog")
        schema = dbutils.widgets.get("schema")
        warehouse_id = dbutils.widgets.get("warehouse_id")
    except Exception:
        if len(sys.argv) >= 4:
            catalog, schema, warehouse_id = sys.argv[1], sys.argv[2], sys.argv[3]
        else:
            from dotenv import load_dotenv
            load_dotenv()
            catalog = os.getenv("GOLD_CATALOG", "dilshad_shawki")
            schema = os.getenv("GOLD_SCHEMA", "geospatial")
            warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID", "")

    result = main(catalog, schema, warehouse_id)
    print(f"HEX2VEC_MODEL_PATH={result}")
