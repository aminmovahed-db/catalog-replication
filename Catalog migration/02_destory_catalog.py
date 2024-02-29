# Databricks notebook source
# MAGIC %md
# MAGIC ##Drop catalog
# MAGIC  In order to create a new catalog from backup we need to drop the srouce catalog first.

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("catalogName", "edp_catalog_new", "information_schema catalog")

# COMMAND ----------

catalog_name =  dbutils.widgets.get("catalogName")
owner = spark.sql("SELECT current_user()").collect()[0][0]

# COMMAND ----------

print(catalog_name)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Drop tables

# COMMAND ----------

table_df = spark.sql(f"select * from {catalog_name}.information_schema.tables")
for table in table_df.collect():
  if (table.table_type != 'VIEW' and table.table_schema != 'information_schema'):
    try:
        # Attempt to alter the table and set the owner
        spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{table.table_schema}.{table.table_name}")
    except Exception as e:
        # Check if the exception is because the table does not exist
        if "PERMISSION_DENIED" in str(e):
            spark.sql(f"ALTER TABLE {catalog_name}.{table.table_schema}.{table.table_name} SET OWNER TO `{owner}`")
            spark.sql(f"DROP TABLE IF EXISTS {catalog_name}.{table.table_schema}.{table.table_name}")
        else:
            print(e)
            continue

# COMMAND ----------

# MAGIC %md
# MAGIC ## Drop Schemas

# COMMAND ----------

schema_df = spark.sql(f"SELECT * FROM system.information_schema.schemata WHERE catalog_name = '{catalog_name}'")
for schema in schema_df.collect():
  if schema.schema_name != 'information_schema':
    spark.sql(f"DROP SCHEMA IF EXISTS {catalog_name}.{schema.schema_name} CASCADE")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Drop Catalogs

# COMMAND ----------

spark.sql(f"DROP CATALOG {catalog_name}")

# COMMAND ----------

# %fs rm -r s3://external-storage-workspace101/edp_catalog/external

# COMMAND ----------

# %fs rm -r s3://external-storage-workspace101/edp_catalog/backup

# COMMAND ----------


