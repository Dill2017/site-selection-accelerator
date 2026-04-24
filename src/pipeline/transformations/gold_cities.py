# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Cities
# MAGIC Pre-processed city lookup with polygons, centres, and bounding boxes.
# MAGIC Joins divisions + division_areas so the app never touches raw WKB geometry.
# MAGIC Polygon resolution: locality area first, then ST_Union of all same-name
# MAGIC region/county/neighborhood/macrohood areas (covers city-states and metro areas).

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("carto_divisions_catalog", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
carto_divisions_catalog = dbutils.widgets.get("carto_divisions_catalog")

print(f"Catalog:            {catalog}")
print(f"Schema:             {schema}")
print(f"Divisions catalog:  {carto_divisions_catalog}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER('${catalog}' || '.' || '${schema}' || '.gold_cities') AS
# MAGIC WITH localities AS (
# MAGIC     SELECT
# MAGIC         d.id,
# MAGIC         d.country,
# MAGIC         d.names.primary  AS city_name,
# MAGIC         d.bbox
# MAGIC     FROM IDENTIFIER('${carto_divisions_catalog}' || '.carto.division') d
# MAGIC     WHERE d.subtype = 'locality'
# MAGIC         AND d.class IN ('city', 'town')
# MAGIC         AND d.country IS NOT NULL
# MAGIC         AND d.names.primary IS NOT NULL
# MAGIC ),
# MAGIC locality_area AS (
# MAGIC     SELECT l.id, FIRST(da.geom) AS geom
# MAGIC     FROM localities l
# MAGIC     INNER JOIN IDENTIFIER('${carto_divisions_catalog}' || '.carto.division_area') da
# MAGIC         ON da.division_id = l.id
# MAGIC     GROUP BY l.id
# MAGIC ),
# MAGIC cities_needing_fallback AS (
# MAGIC     SELECT DISTINCT l.city_name, l.country
# MAGIC     FROM localities l
# MAGIC     LEFT JOIN locality_area la ON la.id = l.id
# MAGIC     WHERE la.geom IS NULL
# MAGIC ),
# MAGIC fallback_area AS (
# MAGIC     SELECT
# MAGIC         cn.city_name,
# MAGIC         cn.country,
# MAGIC         REDUCE(
# MAGIC             COLLECT_LIST(da.geom),
# MAGIC             CAST(NULL AS BINARY),
# MAGIC             (acc, x) -> CASE
# MAGIC                 WHEN acc IS NULL THEN x
# MAGIC                 ELSE ST_AsBinary(ST_Union(ST_GeomFromWKB(acc), ST_GeomFromWKB(x)))
# MAGIC             END,
# MAGIC             acc -> acc
# MAGIC         ) AS geom
# MAGIC     FROM cities_needing_fallback cn
# MAGIC     INNER JOIN IDENTIFIER('${carto_divisions_catalog}' || '.carto.division') d
# MAGIC         ON d.names.primary = cn.city_name AND d.country = cn.country
# MAGIC     INNER JOIN IDENTIFIER('${carto_divisions_catalog}' || '.carto.division_area') da
# MAGIC         ON da.division_id = d.id
# MAGIC     WHERE d.subtype IN ('region', 'county', 'neighborhood', 'macrohood')
# MAGIC     GROUP BY cn.city_name, cn.country
# MAGIC ),
# MAGIC resolved AS (
# MAGIC     SELECT
# MAGIC         l.*,
# MAGIC         COALESCE(la.geom, fa.geom) AS geom
# MAGIC     FROM localities l
# MAGIC     LEFT JOIN locality_area la ON la.id = l.id
# MAGIC     LEFT JOIN fallback_area fa
# MAGIC         ON fa.city_name = l.city_name AND fa.country = l.country
# MAGIC )
# MAGIC SELECT
# MAGIC     r.country,
# MAGIC     r.city_name,
# MAGIC     CAST(r.bbox.xmin AS DOUBLE)                              AS center_lon,
# MAGIC     CAST(r.bbox.ymin AS DOUBLE)                              AS center_lat,
# MAGIC     r.geom IS NOT NULL                                       AS has_polygon,
# MAGIC     COALESCE(
# MAGIC         ST_AsText(ST_GeomFromWKB(r.geom)),
# MAGIC         CONCAT(
# MAGIC             'POLYGON((',
# MAGIC             CAST(r.bbox.xmin - 0.15 AS STRING), ' ', CAST(r.bbox.ymin - 0.15 AS STRING), ', ',
# MAGIC             CAST(r.bbox.xmin + 0.15 AS STRING), ' ', CAST(r.bbox.ymin - 0.15 AS STRING), ', ',
# MAGIC             CAST(r.bbox.xmin + 0.15 AS STRING), ' ', CAST(r.bbox.ymin + 0.15 AS STRING), ', ',
# MAGIC             CAST(r.bbox.xmin - 0.15 AS STRING), ' ', CAST(r.bbox.ymin + 0.15 AS STRING), ', ',
# MAGIC             CAST(r.bbox.xmin - 0.15 AS STRING), ' ', CAST(r.bbox.ymin - 0.15 AS STRING),
# MAGIC             '))'
# MAGIC         )
# MAGIC     )                                                        AS geom_wkt,
# MAGIC     COALESCE(
# MAGIC         CAST(ST_XMin(ST_GeomFromWKB(r.geom)) AS DOUBLE),
# MAGIC         CAST(r.bbox.xmin - 0.15 AS DOUBLE)
# MAGIC     )                                                        AS bbox_xmin,
# MAGIC     COALESCE(
# MAGIC         CAST(ST_XMax(ST_GeomFromWKB(r.geom)) AS DOUBLE),
# MAGIC         CAST(r.bbox.xmin + 0.15 AS DOUBLE)
# MAGIC     )                                                        AS bbox_xmax,
# MAGIC     COALESCE(
# MAGIC         CAST(ST_YMin(ST_GeomFromWKB(r.geom)) AS DOUBLE),
# MAGIC         CAST(r.bbox.ymin - 0.15 AS DOUBLE)
# MAGIC     )                                                        AS bbox_ymin,
# MAGIC     COALESCE(
# MAGIC         CAST(ST_YMax(ST_GeomFromWKB(r.geom)) AS DOUBLE),
# MAGIC         CAST(r.bbox.ymin + 0.15 AS DOUBLE)
# MAGIC     )                                                        AS bbox_ymax
# MAGIC FROM resolved r
