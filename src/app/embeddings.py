"""SRAI Hex2Vec geospatial embedding pipeline.

Converts the DBSQL output (POIs and buildings assigned to H3 cells) into
the GeoDataFrames that SRAI expects, then trains a Hex2VecEmbedder to
produce dense embeddings per H3 cell.

SRAI requires H3 hex-string indices (e.g. "891f1d48177ffff").
DBSQL returns BIGINT cell IDs.  We convert at the boundary.
"""

from __future__ import annotations

import logging

import geopandas as gpd
import h3
import numpy as np
import pandas as pd
from shapely.geometry import Polygon
from srai.embedders import Hex2VecEmbedder
from srai.neighbourhoods import H3Neighbourhood

from config import TRAINING_BATCH_SIZE, TRAINING_EPOCHS

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Index conversion helpers
# ---------------------------------------------------------------------------


def _int_to_hex(cell_id: int) -> str:
    return h3.int_to_str(cell_id)


def _hex_to_int(hex_id: str) -> int:
    return h3.str_to_int(hex_id)


def _h3_hex_to_polygon(hex_id: str) -> Polygon:
    boundary = h3.cell_to_boundary(hex_id)
    return Polygon([(lon, lat) for lat, lon in boundary])


# ---------------------------------------------------------------------------
# Buildings normalisation
# ---------------------------------------------------------------------------


def normalise_buildings(buildings_df: pd.DataFrame) -> pd.DataFrame:
    """Expand each building into feature rows compatible with the POI schema.

    Each building produces up to two rows:
      1. One for its ``building_category`` (e.g. ``bldg_residential``).
      2. One for its ``height_bin`` (e.g. ``height_mid_rise``), when non-null.

    Returns a DataFrame with columns: feature_id, category, lon, lat, h3_cell
    """
    if buildings_df.empty:
        return pd.DataFrame(
            columns=["feature_id", "category", "lon", "lat", "h3_cell"]
        )

    cat_rows = buildings_df[["building_id", "building_category", "lon", "lat", "h3_cell"]].copy()
    cat_rows = cat_rows.rename(columns={"building_id": "feature_id", "building_category": "category"})
    cat_rows["feature_id"] = "bc_" + cat_rows["feature_id"].astype(str)

    height_rows = buildings_df[buildings_df["height_bin"].notna()].copy()
    if not height_rows.empty:
        height_rows = height_rows[["building_id", "height_bin", "lon", "lat", "h3_cell"]]
        height_rows = height_rows.rename(columns={"building_id": "feature_id", "height_bin": "category"})
        height_rows["feature_id"] = "bh_" + height_rows["feature_id"].astype(str)
        return pd.concat([cat_rows, height_rows], ignore_index=True)

    return cat_rows.reset_index(drop=True)


# ---------------------------------------------------------------------------
# GeoDataFrame builders
# ---------------------------------------------------------------------------


def build_regions_gdf(h3_cells: pd.DataFrame) -> gpd.GeoDataFrame:
    """Create a GeoDataFrame of H3 cell polygons indexed by hex string."""
    unique_ints = h3_cells["h3_cell"].unique()
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
    """Create a GeoDataFrame of features with category indicator columns.

    ``features_df`` must contain columns: feature_id, category, lon, lat.
    Each row gets a 1 for its own category.  Density is captured because
    cells with more features contribute more rows, so Hex2Vec's aggregation
    naturally sums higher counts.
    """
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
    """Construct the region-feature join table using hex-string region IDs."""
    joint = features_df[["h3_cell", "feature_id"]].copy()
    joint["h3_cell"] = joint["h3_cell"].apply(_int_to_hex)
    joint = joint.rename(columns={"h3_cell": "region_id"})
    joint = joint.set_index(["region_id", "feature_id"])
    return joint


# ---------------------------------------------------------------------------
# Embedding generation
# ---------------------------------------------------------------------------


def generate_embeddings(
    regions_gdf: gpd.GeoDataFrame,
    features_gdf: gpd.GeoDataFrame,
    joint_gdf: pd.DataFrame,
    max_epochs: int = TRAINING_EPOCHS,
    batch_size: int = TRAINING_BATCH_SIZE,
) -> pd.DataFrame:
    """Train a Hex2VecEmbedder from scratch and return embeddings."""
    neighbourhood = H3Neighbourhood(regions_gdf)

    _log.info("Training Hex2Vec from scratch (epochs=%d)", max_epochs)
    embedder = Hex2VecEmbedder(encoder_sizes=[15, 10])
    embeddings = embedder.fit_transform(
        regions_gdf, features_gdf, joint_gdf, neighbourhood,
        trainer_kwargs={"max_epochs": max_epochs, "accelerator": "cpu"},
        batch_size=batch_size,
    )
    return embeddings


def run_embedding_pipeline(
    h3_cells_df: pd.DataFrame,
    features_df: pd.DataFrame,
    categories: list[str],
    max_epochs: int = TRAINING_EPOCHS,
) -> pd.DataFrame:
    """End-to-end: build GeoDataFrames, train Hex2Vec, return embeddings.

    ``features_df`` is the unified feature table (POIs + buildings) with
    columns: feature_id, category, lon, lat, h3_cell.

    Returns embeddings indexed by BIGINT cell IDs (converted back from
    the hex strings SRAI uses internally).
    """
    regions_gdf = build_regions_gdf(h3_cells_df)
    features_gdf = build_features_gdf(features_df, categories)
    joint_gdf = build_joint_gdf(features_df)

    valid_regions = joint_gdf.index.get_level_values("region_id").unique()
    regions_gdf = regions_gdf.loc[regions_gdf.index.isin(valid_regions)]

    valid_features = joint_gdf.index.get_level_values("feature_id").unique()
    features_gdf = features_gdf.loc[features_gdf.index.isin(valid_features)]

    if regions_gdf.empty or features_gdf.empty:
        raise ValueError(
            "No feature data intersects the H3 cells. Try a different city, "
            "resolution, or category selection."
        )

    embeddings = generate_embeddings(
        regions_gdf, features_gdf, joint_gdf,
        max_epochs=max_epochs,
    )

    embeddings.index = embeddings.index.map(_hex_to_int)
    embeddings.index.name = "region_id"
    return embeddings
