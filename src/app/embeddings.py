"""SRAI Hex2Vec geospatial embedding pipeline.

Converts the DBSQL output (POIs assigned to H3 cells) into the
GeoDataFrames that SRAI expects, then runs Hex2VecEmbedder to produce
dense embeddings per H3 cell.

SRAI requires H3 hex-string indices (e.g. "891f1d48177ffff").
DBSQL returns BIGINT cell IDs.  We convert at the boundary.
"""

from __future__ import annotations

import geopandas as gpd
import h3
import pandas as pd
from shapely.geometry import Point, Polygon
from srai.embedders import Hex2VecEmbedder
from srai.neighbourhoods import H3Neighbourhood


def _int_to_hex(cell_id: int) -> str:
    return h3.int_to_str(cell_id)


def _hex_to_int(hex_id: str) -> int:
    return h3.str_to_int(hex_id)


def _h3_hex_to_polygon(hex_id: str) -> Polygon:
    boundary = h3.cell_to_boundary(hex_id)
    return Polygon([(lon, lat) for lat, lon in boundary])


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
    pois_df: pd.DataFrame,
    categories: list[str],
) -> gpd.GeoDataFrame:
    """Create a GeoDataFrame of POI features with category indicator columns.

    Each POI still gets a 1 for its own category (SRAI expects per-POI
    features).  Density is captured because cells with more POIs contribute
    more rows, so Hex2Vec's aggregation naturally sums higher counts.
    """
    geom = [Point(row.lon, row.lat) for _, row in pois_df.iterrows()]
    gdf = gpd.GeoDataFrame(pois_df, geometry=geom, crs="EPSG:4326")
    gdf = gdf.set_index("poi_id")
    gdf.index.name = "feature_id"

    for cat in categories:
        gdf[cat] = (gdf["category"] == cat).astype(int)

    gdf = gdf.drop(columns=["category", "lon", "lat", "address", "h3_cell"], errors="ignore")
    return gdf


def build_joint_gdf(pois_df: pd.DataFrame) -> pd.DataFrame:
    """Construct the region-feature join table using hex-string region IDs."""
    joint = pois_df[["h3_cell", "poi_id"]].copy()
    joint["h3_cell"] = joint["h3_cell"].apply(_int_to_hex)
    joint = joint.rename(columns={"h3_cell": "region_id", "poi_id": "feature_id"})
    joint = joint.set_index(["region_id", "feature_id"])
    return joint


def generate_embeddings(
    regions_gdf: gpd.GeoDataFrame,
    features_gdf: gpd.GeoDataFrame,
    joint_gdf: pd.DataFrame,
    max_epochs: int = 5,
    batch_size: int = 128,
) -> pd.DataFrame:
    """Train Hex2Vec and return embeddings indexed by hex-string region_id."""
    neighbourhood = H3Neighbourhood(regions_gdf)

    embedder = Hex2VecEmbedder(encoder_sizes=[15, 10])
    embeddings = embedder.fit_transform(
        regions_gdf,
        features_gdf,
        joint_gdf,
        neighbourhood,
        trainer_kwargs={"max_epochs": max_epochs, "accelerator": "cpu"},
        batch_size=batch_size,
    )
    return embeddings


def run_embedding_pipeline(
    h3_cells_df: pd.DataFrame,
    pois_df: pd.DataFrame,
    categories: list[str],
    max_epochs: int = 5,
) -> pd.DataFrame:
    """End-to-end: build GeoDataFrames, train Hex2Vec, return embeddings.

    Returns embeddings indexed by BIGINT cell IDs (converted back from
    the hex strings SRAI uses internally).
    """
    regions_gdf = build_regions_gdf(h3_cells_df)
    features_gdf = build_features_gdf(pois_df, categories)
    joint_gdf = build_joint_gdf(pois_df)

    valid_regions = joint_gdf.index.get_level_values("region_id").unique()
    regions_gdf = regions_gdf.loc[regions_gdf.index.isin(valid_regions)]

    valid_features = joint_gdf.index.get_level_values("feature_id").unique()
    features_gdf = features_gdf.loc[features_gdf.index.isin(valid_features)]

    if regions_gdf.empty or features_gdf.empty:
        raise ValueError(
            "No POI data intersects the H3 cells. Try a different city, "
            "resolution, or category selection."
        )

    embeddings = generate_embeddings(
        regions_gdf, features_gdf, joint_gdf, max_epochs=max_epochs
    )

    # Convert hex-string index back to BIGINT for downstream consumers
    embeddings.index = embeddings.index.map(_hex_to_int)
    embeddings.index.name = "region_id"
    return embeddings
