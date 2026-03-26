-- Pre-processed buildings table: centroids extracted from footprint polygons,
-- building type categorised from subtype/class, height discretised into bins.
-- H3 cell pre-computed at resolution 9 to avoid per-row computation at query time.
-- Z-ordered by bbox coordinates for fast spatial range scans.
CREATE OR REPLACE TABLE IDENTIFIER({{catalog}} || '.' || {{schema}} || '.gold_buildings') AS
SELECT
    b.id                                                        AS building_id,
    b.subtype                                                   AS building_subtype,
    b.class                                                     AS building_class,
    b.height                                                    AS height,
    b.num_floors                                                AS num_floors,
    CONCAT('bldg_', COALESCE(b.subtype, b.class, 'other'))     AS building_category,
    CASE
        WHEN b.height IS NULL            THEN NULL
        WHEN b.height <= 6              THEN 'height_low_rise'
        WHEN b.height <= 15             THEN 'height_mid_rise'
        WHEN b.height <= 35             THEN 'height_high_rise'
        ELSE                                 'height_skyscraper'
    END                                                         AS height_bin,
    ST_X(ST_Centroid(ST_GeomFromWKB(b.geom)))                   AS lon,
    ST_Y(ST_Centroid(ST_GeomFromWKB(b.geom)))                   AS lat,
    h3_longlatash3(
        ST_X(ST_Centroid(ST_GeomFromWKB(b.geom))),
        ST_Y(ST_Centroid(ST_GeomFromWKB(b.geom))),
        9
    )                                                           AS h3_cell,
    CAST(b.bbox.xmin AS DOUBLE)                                 AS bbox_xmin,
    CAST(b.bbox.xmax AS DOUBLE)                                 AS bbox_xmax,
    CAST(b.bbox.ymin AS DOUBLE)                                 AS bbox_ymin,
    CAST(b.bbox.ymax AS DOUBLE)                                 AS bbox_ymax
FROM carto_overture_maps_buildings.carto.building b
WHERE b.subtype IS NOT NULL OR b.class IS NOT NULL;

OPTIMIZE IDENTIFIER({{catalog}} || '.' || {{schema}} || '.gold_buildings')
ZORDER BY (bbox_xmin, bbox_ymin);
