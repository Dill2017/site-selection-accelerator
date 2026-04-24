# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Setup Schema
# MAGIC Creates the target schema if it does not already exist.

# COMMAND ----------

dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"Catalog: {catalog}")
print(f"Schema:  {schema}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS IDENTIFIER('${catalog}' || '.' || '${schema}')
