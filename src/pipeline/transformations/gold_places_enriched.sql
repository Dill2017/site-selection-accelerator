-- Enriched POI table: full attribute extraction from CARTO Overture Maps.
-- Unlike gold_places (which filters to accelerator categories only), this
-- table includes ALL categorised POIs with brand names, addresses, and
-- coordinates — designed for Genie Space queries and competition analysis.
-- H3 cell assignment happens at query time (resolution is user-selected).
CREATE OR REPLACE TABLE IDENTIFIER({{catalog}} || '.' || {{schema}} || '.gold_places_enriched') AS
SELECT
    p.id                                                AS poi_id,
    p.names.primary                                     AS poi_primary_name,
    concat_ws(', ', map_values(p.names.common))         AS poi_common_name,
    p.categories.primary                                AS basic_category,
    p.categories.primary                                AS poi_primary_category,
    concat_ws(', ', p.categories.alternate)             AS poi_alt_category,
    p.categories.primary                                AS poi_primary_taxonomy,
    concat_ws(', ', p.categories.alternate)             AS poi_alt_taxonomy,
    p.brand.names.primary                               AS brand_name_primary,
    concat_ws(', ', map_values(p.brand.names.common))   AS brand_name_common,
    p.addresses[0].freeform                             AS address_line,
    p.addresses[0].locality                             AS locality,
    p.addresses[0].postcode                             AS postcode,
    p.addresses[0].region                               AS region,
    p.addresses[0].country                              AS country,
    p.confidence                                        AS confidence,
    ST_X(ST_GeomFromWKB(p.geom))                        AS lon,
    ST_Y(ST_GeomFromWKB(p.geom))                        AS lat,
    CAST(p.bbox.xmin AS DOUBLE)                         AS bbox_xmin,
    CAST(p.bbox.xmax AS DOUBLE)                         AS bbox_xmax,
    CAST(p.bbox.ymin AS DOUBLE)                         AS bbox_ymin,
    CAST(p.bbox.ymax AS DOUBLE)                         AS bbox_ymax
FROM IDENTIFIER({{carto_places_catalog}} || '.carto.place') p
WHERE p.categories.primary IS NOT NULL
