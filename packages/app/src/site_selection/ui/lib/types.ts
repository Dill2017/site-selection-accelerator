export interface CategoryGroup {
  name: string;
  categories: string[];
}

export interface AppConfig {
  h3_resolutions: number[];
  default_resolution: number;
  category_groups: CategoryGroup[];
  building_category_groups: CategoryGroup[];
}

export interface BrandInput {
  mode: "brand_name" | "latlng" | "addresses" | "map_selection";
  value: string;
  geojson?: GeoJSON.FeatureCollection | null;
}

export type DrawingMode = "navigate" | "point" | "polygon";

export interface AnalyzeRequest {
  country: string;
  city: string;
  resolution: number;
  categories: string[];
  brand_input: BrandInput;
  enable_competition: boolean;
  beta: number;
  include_buildings: boolean;
}

export interface HexagonData {
  h3_cell: number;
  hex_id: string;
  similarity: number;
  opportunity_score: number | null;
  is_brand_cell: boolean;
  lat: number;
  lon: number;
  address: string;
  poi_count: number;
  competitor_count: number;
  top_competitors: string;
  cat_detail: string;
}

export interface BrandLocationData {
  lat: number;
  lon: number;
  hex_id: string;
  count: number;
}

export interface AnalyzeResult {
  session_id: string;
  hexagons: HexagonData[];
  brand_locations: BrandLocationData[];
  city_polygon_geojson: Record<string, unknown> | null;
  has_competition: boolean;
  center_lat: number;
  center_lon: number;
}

export interface SSEProgress {
  type: "progress";
  step: string;
  pct: number;
}

export interface SSEError {
  type: "error";
  message: string;
}

export interface SSEResult {
  type: "result";
  data: AnalyzeResult;
}

export type SSEEvent = SSEProgress | SSEError | SSEResult;

export interface CategoryAvgItem {
  category: string;
  avg_count: number;
  pct_within_type: number;
  feature_type: string;
  group: string;
}

export interface CellBreakdownRow {
  location: string;
  category: string;
  count: number;
}

export interface BrandProfile {
  avg_profile: CategoryAvgItem[];
  cell_breakdown: CellBreakdownRow[];
}

export interface FingerprintRow {
  category: string;
  group: string;
  feature_type: string;
  this_location: number;
  brand_average: number;
  this_location_pct: number;
  brand_average_pct: number;
}

export interface CompetitionInfo {
  vibe_score: number;
  competitor_count: number;
  competition_score: number;
  opportunity_score: number;
  top_competitors: string;
}

export interface CompetitorPOI {
  name: string;
  category: string;
  brand: string;
  address: string;
}

export interface HexagonDetail {
  h3_cell: number;
  hex_id: string;
  address: string;
  similarity: number;
  opportunity_score: number | null;
  poi_count: number;
  explanation_summary: string;
  competition: CompetitionInfo | null;
  competitor_pois: CompetitorPOI[];
  fingerprint: FingerprintRow[];
}

export interface BrandPOIRow {
  name: string;
  category: string;
  brand: string;
  lat: number | null;
  lon: number | null;
  h3_cell: string;
}

export interface GenieDebug {
  brand_pois: BrandPOIRow[];
  total_brand_pois: number;
  competitor_pois_total: number;
}

export const STEP_LABELS: Record<string, string> = {
  starting: "Starting pipeline...",
  loading_model: "Loading Hex2Vec model...",
  resolving_brand: "Resolving brand locations...",
  tessellating: "Tessellating city with H3...",
  fetching_brand_context: "Fetching brand neighbourhood context...",
  querying_pois: "Querying POIs...",
  querying_buildings: "Querying buildings...",
  building_vectors: "Building count vectors...",
  generating_embeddings: "Generating embeddings...",
  computing_similarity: "Computing similarity scores...",
  finding_competitors: "Finding competitors...",
  caching_results: "Preparing results...",
  done: "Done!",
};
