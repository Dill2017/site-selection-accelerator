# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold Places
# MAGIC Pre-processed POI table: coordinates extracted, category flattened,
# MAGIC address denested. Filtered to the categories used by the accelerator.

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
# MAGIC CREATE OR REPLACE TABLE IDENTIFIER('${catalog}' || '.' || '${schema}' || '.gold_places') AS
# MAGIC SELECT
# MAGIC     p.id                                AS poi_id,
# MAGIC     p.categories.primary                AS category,
# MAGIC     ST_X(ST_GeomFromWKB(p.geom))        AS lon,
# MAGIC     ST_Y(ST_GeomFromWKB(p.geom))        AS lat,
# MAGIC     p.addresses[0].freeform             AS address,
# MAGIC     CAST(p.bbox.xmin AS DOUBLE)         AS bbox_xmin,
# MAGIC     CAST(p.bbox.xmax AS DOUBLE)         AS bbox_xmax,
# MAGIC     CAST(p.bbox.ymin AS DOUBLE)         AS bbox_ymin,
# MAGIC     CAST(p.bbox.ymax AS DOUBLE)         AS bbox_ymax
# MAGIC FROM IDENTIFIER('${carto_places_catalog}' || '.carto.place') p
# MAGIC WHERE p.categories.primary IN (
# MAGIC     'restaurant', 'fast_food_restaurant', 'cafe', 'coffee_shop',
# MAGIC     'bar', 'bakery', 'food_truck',
# MAGIC     'clothing_store', 'convenience_store', 'grocery_store', 'shopping',
# MAGIC     'furniture_store', 'supermarket', 'shopping_mall', 'department_store',
# MAGIC     'bank', 'pharmacy', 'gas_station', 'gym', 'hospital', 'dentist',
# MAGIC     'hair_salon', 'beauty_salon', 'automotive_repair',
# MAGIC     'movie_theater', 'park', 'hotel', 'museum',
# MAGIC     'professional_services', 'real_estate', 'education', 'school'
# MAGIC )
