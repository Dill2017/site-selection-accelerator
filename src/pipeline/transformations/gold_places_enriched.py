# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Places Enriched
# MAGIC Enriched POI table: full attribute extraction from CARTO Overture Maps.
# MAGIC Unlike gold_places (which filters to accelerator categories only), this
# MAGIC table includes ALL categorised POIs with brand names, addresses, and
# MAGIC coordinates — designed for Genie Space queries and competition analysis.
# MAGIC H3 cell assignment happens at query time (resolution is user-selected).

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("carto_places_catalog", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
carto_places_catalog = dbutils.widgets.get("carto_places_catalog")

print(f"Catalog:         {catalog}")
print(f"Schema:          {schema}")
print(f"Places catalog:  {carto_places_catalog}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER('${catalog}' || '.' || '${schema}' || '.gold_places_enriched') AS
# MAGIC SELECT
# MAGIC     p.id                                                AS poi_id,
# MAGIC     p.names.primary                                     AS poi_primary_name,
# MAGIC     concat_ws(', ', map_values(p.names.common))         AS poi_common_name,
# MAGIC     p.categories.primary                                AS basic_category,
# MAGIC     p.categories.primary                                AS poi_primary_category,
# MAGIC     concat_ws(', ', p.categories.alternate)             AS poi_alt_category,
# MAGIC     p.categories.primary                                AS poi_primary_taxonomy,
# MAGIC     concat_ws(', ', p.categories.alternate)             AS poi_alt_taxonomy,
# MAGIC     p.brand.names.primary                               AS brand_name_primary,
# MAGIC     concat_ws(', ', map_values(p.brand.names.common))   AS brand_name_common,
# MAGIC     p.addresses[0].freeform                             AS address_line,
# MAGIC     p.addresses[0].locality                             AS locality,
# MAGIC     p.addresses[0].postcode                             AS postcode,
# MAGIC     p.addresses[0].region                               AS region,
# MAGIC     p.addresses[0].country                              AS country,
# MAGIC     p.confidence                                        AS confidence,
# MAGIC     ST_X(ST_GeomFromWKB(p.geom))                        AS lon,
# MAGIC     ST_Y(ST_GeomFromWKB(p.geom))                        AS lat,
# MAGIC     CAST(p.bbox.xmin AS DOUBLE)                         AS bbox_xmin,
# MAGIC     CAST(p.bbox.xmax AS DOUBLE)                         AS bbox_xmax,
# MAGIC     CAST(p.bbox.ymin AS DOUBLE)                         AS bbox_ymin,
# MAGIC     CAST(p.bbox.ymax AS DOUBLE)                         AS bbox_ymax
# MAGIC FROM IDENTIFIER('${carto_places_catalog}' || '.carto.place') p
# MAGIC WHERE p.categories.primary IS NOT NULL
