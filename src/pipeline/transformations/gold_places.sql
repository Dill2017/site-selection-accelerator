-- Pre-processed POI table: coordinates extracted, category flattened,
-- address denested. Filtered to the categories used by the accelerator.
CREATE OR REPLACE TABLE IDENTIFIER({{catalog}} || '.' || {{schema}} || '.gold_places') AS
SELECT
    p.id                                AS poi_id,
    p.categories.primary                AS category,
    ST_X(ST_GeomFromWKB(p.geom))        AS lon,
    ST_Y(ST_GeomFromWKB(p.geom))        AS lat,
    p.addresses[0].freeform             AS address,
    CAST(p.bbox.xmin AS DOUBLE)         AS bbox_xmin,
    CAST(p.bbox.xmax AS DOUBLE)         AS bbox_xmax,
    CAST(p.bbox.ymin AS DOUBLE)         AS bbox_ymin,
    CAST(p.bbox.ymax AS DOUBLE)         AS bbox_ymax
FROM IDENTIFIER({{carto_places_catalog}} || '.carto.place') p
WHERE p.categories.primary IN (
    'restaurant', 'fast_food_restaurant', 'cafe', 'coffee_shop',
    'bar', 'bakery', 'food_truck',
    'clothing_store', 'convenience_store', 'grocery_store', 'shopping',
    'furniture_store', 'supermarket', 'shopping_mall', 'department_store',
    'bank', 'pharmacy', 'gas_station', 'gym', 'hospital', 'dentist',
    'hair_salon', 'beauty_salon', 'automotive_repair',
    'movie_theater', 'park', 'hotel', 'museum',
    'professional_services', 'real_estate', 'education', 'school'
)
