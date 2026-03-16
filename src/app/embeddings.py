"""SRAI Hex2Vec geospatial embedding pipeline.

Converts the DBSQL output (POIs and buildings assigned to H3 cells) into
the GeoDataFrames that SRAI expects, then trains a Hex2VecEmbedder to
produce dense embeddings per H3 cell.

Supports two modes:
  1. fit_transform (legacy) — train and embed in a single pass on one city.
  2. Pre-trained — load a model fitted on many cities, then transform only.

SRAI requires H3 hex-string indices (e.g. "891f1d48177ffff").
DBSQL returns BIGINT cell IDs.  We convert at the boundary.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

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
# Model persistence
# ---------------------------------------------------------------------------


_METADATA_FILENAME = "hex2vec_metadata.json"


def save_hex2vec(
    embedder: Hex2VecEmbedder,
    base_path: str | Path,
    *,
    categories: list[str],
    resolution: int,
    cities: list[tuple[str, str]] | None = None,
) -> None:
    """Save a fitted Hex2VecEmbedder and its training metadata."""
    base = Path(base_path)
    base.mkdir(parents=True, exist_ok=True)

    embedder.save(base)

    metadata = {
        "categories": categories,
        "resolution": resolution,
        "cities": cities,
        "encoder_sizes": embedder._encoder_sizes,
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    (base / _METADATA_FILENAME).write_text(json.dumps(metadata, indent=2))
    _log.info("Saved Hex2Vec model to %s", base)


def load_hex2vec(
    base_path: str | Path,
) -> tuple[Hex2VecEmbedder, dict]:
    """Load a pre-trained Hex2VecEmbedder and its metadata.

    Returns (embedder, metadata_dict).
    Raises FileNotFoundError if the model directory does not exist.
    """
    base = Path(base_path)
    meta_file = base / _METADATA_FILENAME
    if not meta_file.exists():
        raise FileNotFoundError(f"No Hex2Vec metadata at {meta_file}")

    metadata = json.loads(meta_file.read_text())
    embedder = Hex2VecEmbedder.load(base)
    _log.info(
        "Loaded pre-trained Hex2Vec (resolution=%s, %d categories, saved %s)",
        metadata.get("resolution"),
        len(metadata.get("categories", [])),
        metadata.get("saved_at", "unknown"),
    )
    return embedder, metadata


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


def transform_embeddings(
    embedder: Hex2VecEmbedder,
    regions_gdf: gpd.GeoDataFrame,
    features_gdf: gpd.GeoDataFrame,
    joint_gdf: pd.DataFrame,
) -> pd.DataFrame:
    """Generate embeddings using a pre-trained Hex2VecEmbedder (no fitting)."""
    _log.info("Transforming %d regions with pre-trained Hex2Vec", len(regions_gdf))
    return embedder.transform(regions_gdf, features_gdf, joint_gdf)


def run_embedding_pipeline(
    h3_cells_df: pd.DataFrame,
    features_df: pd.DataFrame,
    categories: list[str],
    max_epochs: int = TRAINING_EPOCHS,
    *,
    pretrained_embedder: Hex2VecEmbedder | None = None,
    training_categories: list[str] | None = None,
) -> pd.DataFrame:
    """End-to-end: build GeoDataFrames, generate Hex2Vec embeddings.

    ``features_df`` is the unified feature table (POIs + buildings) with
    columns: feature_id, category, lon, lat, h3_cell.

    When ``pretrained_embedder`` is provided the model is **not** retrained;
    only ``transform`` is called.  ``training_categories`` must also be
    supplied so the feature GeoDataFrame matches the encoder's input
    dimension.

    Returns embeddings indexed by BIGINT cell IDs (converted back from
    the hex strings SRAI uses internally).
    """
    effective_cats = (
        training_categories if pretrained_embedder is not None and training_categories
        else categories
    )

    regions_gdf = build_regions_gdf(h3_cells_df)
    features_gdf = build_features_gdf(features_df, effective_cats)
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

    if pretrained_embedder is not None:
        embeddings = transform_embeddings(
            pretrained_embedder, regions_gdf, features_gdf, joint_gdf,
        )
    else:
        embeddings = generate_embeddings(
            regions_gdf, features_gdf, joint_gdf,
            max_epochs=max_epochs,
        )

    embeddings.index = embeddings.index.map(_hex_to_int)
    embeddings.index.name = "region_id"
    return embeddings
