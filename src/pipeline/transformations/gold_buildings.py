# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Buildings
# MAGIC Pre-processed buildings table: centroids extracted from footprint polygons,
# MAGIC building type categorised from subtype/class, height discretised into bins.
# MAGIC H3 cell pre-computed at resolution 9 to avoid per-row computation at query time.
# MAGIC Z-ordered by bbox coordinates for fast spatial range scans.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("carto_buildings_catalog", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
carto_buildings_catalog = dbutils.widgets.get("carto_buildings_catalog")

print(f"Catalog:            {catalog}")
print(f"Schema:             {schema}")
print(f"Buildings catalog:  {carto_buildings_catalog}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER('${catalog}' || '.' || '${schema}' || '.gold_buildings') AS
# MAGIC SELECT
# MAGIC     b.id                                                        AS building_id,
# MAGIC     b.subtype                                                   AS building_subtype,
# MAGIC     b.class                                                     AS building_class,
# MAGIC     b.height                                                    AS height,
# MAGIC     b.num_floors                                                AS num_floors,
# MAGIC     CONCAT('bldg_', COALESCE(b.subtype, b.class, 'other'))     AS building_category,
# MAGIC     CASE
# MAGIC         WHEN b.height IS NULL            THEN NULL
# MAGIC         WHEN b.height <= 6              THEN 'height_low_rise'
# MAGIC         WHEN b.height <= 15             THEN 'height_mid_rise'
# MAGIC         WHEN b.height <= 35             THEN 'height_high_rise'
# MAGIC         ELSE                                 'height_skyscraper'
# MAGIC     END                                                         AS height_bin,
# MAGIC     ST_X(ST_Centroid(ST_GeomFromWKB(b.geom)))                   AS lon,
# MAGIC     ST_Y(ST_Centroid(ST_GeomFromWKB(b.geom)))                   AS lat,
# MAGIC     h3_longlatash3(
# MAGIC         ST_X(ST_Centroid(ST_GeomFromWKB(b.geom))),
# MAGIC         ST_Y(ST_Centroid(ST_GeomFromWKB(b.geom))),
# MAGIC         9
# MAGIC     )                                                           AS h3_cell,
# MAGIC     CAST(b.bbox.xmin AS DOUBLE)                                 AS bbox_xmin,
# MAGIC     CAST(b.bbox.xmax AS DOUBLE)                                 AS bbox_xmax,
# MAGIC     CAST(b.bbox.ymin AS DOUBLE)                                 AS bbox_ymin,
# MAGIC     CAST(b.bbox.ymax AS DOUBLE)                                 AS bbox_ymax
# MAGIC FROM IDENTIFIER('${carto_buildings_catalog}' || '.carto.building') b
# MAGIC WHERE b.subtype IS NOT NULL OR b.class IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE IDENTIFIER('${catalog}' || '.' || '${schema}' || '.gold_buildings')
# MAGIC ZORDER BY (bbox_xmin, bbox_ymin)
