-- Pre-processed city lookup with polygons, centres, and bounding boxes.
-- Joins divisions + division_areas so the app never touches raw WKB geometry.
CREATE OR REPLACE TABLE dilshad_shawki.geospatial.gold_cities AS
SELECT
    d.country,
    d.names.primary                                         AS city_name,
    CAST(d.bbox.xmin AS DOUBLE)                             AS center_lon,
    CAST(d.bbox.ymin AS DOUBLE)                             AS center_lat,
    da.division_id IS NOT NULL                              AS has_polygon,
    COALESCE(
        ST_AsText(ST_GeomFromWKB(da.geom)),
        CONCAT(
            'POLYGON((',
            CAST(d.bbox.xmin - 0.15 AS STRING), ' ', CAST(d.bbox.ymin - 0.15 AS STRING), ', ',
            CAST(d.bbox.xmin + 0.15 AS STRING), ' ', CAST(d.bbox.ymin - 0.15 AS STRING), ', ',
            CAST(d.bbox.xmin + 0.15 AS STRING), ' ', CAST(d.bbox.ymin + 0.15 AS STRING), ', ',
            CAST(d.bbox.xmin - 0.15 AS STRING), ' ', CAST(d.bbox.ymin + 0.15 AS STRING), ', ',
            CAST(d.bbox.xmin - 0.15 AS STRING), ' ', CAST(d.bbox.ymin - 0.15 AS STRING),
            '))'
        )
    )                                                       AS geom_wkt,
    COALESCE(
        CAST(ST_XMin(ST_GeomFromWKB(da.geom)) AS DOUBLE),
        CAST(d.bbox.xmin - 0.15 AS DOUBLE)
    )                                                       AS bbox_xmin,
    COALESCE(
        CAST(ST_XMax(ST_GeomFromWKB(da.geom)) AS DOUBLE),
        CAST(d.bbox.xmin + 0.15 AS DOUBLE)
    )                                                       AS bbox_xmax,
    COALESCE(
        CAST(ST_YMin(ST_GeomFromWKB(da.geom)) AS DOUBLE),
        CAST(d.bbox.ymin - 0.15 AS DOUBLE)
    )                                                       AS bbox_ymin,
    COALESCE(
        CAST(ST_YMax(ST_GeomFromWKB(da.geom)) AS DOUBLE),
        CAST(d.bbox.ymin + 0.15 AS DOUBLE)
    )                                                       AS bbox_ymax
FROM carto_overture_maps_divisions.carto.division d
LEFT JOIN carto_overture_maps_divisions.carto.division_area da
    ON da.division_id = d.id
WHERE d.subtype = 'locality'
    AND d.class IN ('city', 'town')
    AND d.country IS NOT NULL
    AND d.names.primary IS NOT NULL
