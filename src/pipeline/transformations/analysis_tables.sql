-- Analysis result tables for persisting app outputs.
-- These are append-only tables keyed by analysis_id so that multiple
-- analyses (different brands, cities, parameters) coexist.

CREATE TABLE IF NOT EXISTS IDENTIFIER({{catalog}} || '.' || {{schema}} || '.analyses') (
    analysis_id          STRING    NOT NULL,
    session_id           STRING    NOT NULL,
    brand_input_mode     STRING,
    brand_input_value    STRING,
    country              STRING,
    city                 STRING,
    h3_resolution        INT,
    categories           STRING,
    enable_competition   BOOLEAN,
    beta                 DOUBLE,
    include_buildings    BOOLEAN,
    city_polygon_geojson STRING,
    center_lat           DOUBLE,
    center_lon           DOUBLE,
    created_at           TIMESTAMP,
    created_by           STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS IDENTIFIER({{catalog}} || '.' || {{schema}} || '.analysis_brand_profiles') (
    analysis_id      STRING NOT NULL,
    category         STRING,
    avg_count        DOUBLE,
    pct_within_type  DOUBLE,
    feature_type     STRING,
    group_name       STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS IDENTIFIER({{catalog}} || '.' || {{schema}} || '.analysis_hexagons') (
    analysis_id       STRING  NOT NULL,
    h3_cell           BIGINT,
    hex_id            STRING,
    similarity        DOUBLE,
    opportunity_score DOUBLE,
    is_brand_cell     BOOLEAN,
    lat               DOUBLE,
    lon               DOUBLE,
    address           STRING,
    poi_count         INT,
    competitor_count  INT,
    top_competitors   STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS IDENTIFIER({{catalog}} || '.' || {{schema}} || '.analysis_fingerprints') (
    analysis_id          STRING NOT NULL,
    hex_id               STRING,
    category             STRING,
    group_name           STRING,
    feature_type         STRING,
    this_location        DOUBLE,
    brand_average        DOUBLE,
    this_location_pct    DOUBLE,
    brand_average_pct    DOUBLE,
    explanation_summary  STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS IDENTIFIER({{catalog}} || '.' || {{schema}} || '.analysis_competitors') (
    analysis_id STRING NOT NULL,
    hex_id      STRING,
    poi_name    STRING,
    category    STRING,
    brand       STRING,
    address     STRING
) USING DELTA;
