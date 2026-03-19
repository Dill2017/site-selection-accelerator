from __future__ import annotations

from pydantic import BaseModel, Field
from .. import __version__


class VersionOut(BaseModel):
    version: str

    @classmethod
    def from_metadata(cls):
        return cls(version=__version__)


# -- Config / Lookups --------------------------------------------------------

class CategoryGroup(BaseModel):
    name: str
    categories: list[str]


class AppConfigOut(BaseModel):
    h3_resolutions: list[int]
    default_resolution: int
    category_groups: list[CategoryGroup]
    building_category_groups: list[CategoryGroup]


# -- Analyze Request / Response -----------------------------------------------

class BrandInput(BaseModel):
    mode: str = Field(description="'brand_name', 'latlng', 'addresses', or 'map_selection'")
    value: str = Field(default="", description="Brand query, lat/lon lines, or address lines")
    geojson: dict | None = Field(default=None, description="GeoJSON FeatureCollection for map_selection mode")


class AnalyzeRequest(BaseModel):
    country: str
    city: str
    resolution: int = 9
    categories: list[str]
    brand_input: BrandInput
    enable_competition: bool = True
    beta: float = 1.0
    include_buildings: bool = True


class HexagonData(BaseModel):
    h3_cell: int
    hex_id: str
    similarity: float
    opportunity_score: float | None = None
    is_brand_cell: bool
    lat: float
    lon: float
    address: str = ""
    poi_count: int = 0
    competitor_count: int = 0
    top_competitors: str = ""
    cat_detail: str = ""


class BrandLocationData(BaseModel):
    lat: float
    lon: float
    hex_id: str
    count: int = 1


class AnalyzeResultOut(BaseModel):
    session_id: str
    hexagons: list[HexagonData]
    brand_locations: list[BrandLocationData]
    city_polygon_geojson: dict | None = None
    has_competition: bool = False
    center_lat: float
    center_lon: float


# -- Brand Profile ------------------------------------------------------------

class CategoryAvgItem(BaseModel):
    category: str
    avg_count: float
    pct_within_type: float
    feature_type: str
    group: str


class CellBreakdownRow(BaseModel):
    location: str
    category: str
    count: float


class BrandProfileOut(BaseModel):
    avg_profile: list[CategoryAvgItem]
    cell_breakdown: list[CellBreakdownRow]


# -- Hexagon Fingerprint ------------------------------------------------------

class FingerprintRow(BaseModel):
    category: str
    group: str
    feature_type: str
    this_location: float
    brand_average: float
    this_location_pct: float
    brand_average_pct: float


class CompetitionInfo(BaseModel):
    vibe_score: float
    competitor_count: int
    competition_score: float
    opportunity_score: float
    top_competitors: str


class CompetitorPOI(BaseModel):
    name: str
    category: str
    brand: str = ""
    address: str = ""


class HexagonDetailOut(BaseModel):
    h3_cell: int
    hex_id: str
    address: str
    similarity: float
    opportunity_score: float | None = None
    poi_count: int = 0
    explanation_summary: str = ""
    competition: CompetitionInfo | None = None
    competitor_pois: list[CompetitorPOI] = []
    fingerprint: list[FingerprintRow]


# -- Genie Debug ---------------------------------------------------------------

class BrandPOIRow(BaseModel):
    name: str
    category: str
    brand: str = ""
    lat: float | None = None
    lon: float | None = None
    h3_cell: str = ""


class GenieDebugOut(BaseModel):
    brand_pois: list[BrandPOIRow]
    total_brand_pois: int = 0
    competitor_pois_total: int = 0


# -- Persist / Assets ---------------------------------------------------------

class PersistResultOut(BaseModel):
    analysis_id: str
    tables_written: list[str]


class AnalysisSummary(BaseModel):
    analysis_id: str
    brand_input_value: str = ""
    city: str = ""
    country: str = ""
    created_at: str = ""


class AssetLink(BaseModel):
    name: str
    url: str
    asset_type: str = Field(description="table | job | genie | volume | workspace")


class AssetsOut(BaseModel):
    workspace_url: str = ""
    links: list[AssetLink] = []
    recent_analyses: list[AnalysisSummary] = []
