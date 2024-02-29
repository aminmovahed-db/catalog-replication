# Databricks notebook source
# MAGIC %md
# MAGIC ##Backup catalog
# MAGIC
# MAGIC This notebook will read from the information_schema of a given catalog and dump its contents to external storage.
# MAGIC
# MAGIC This external storage will be independent from the UC storage and will be accessible by a remote workspace on a different region with a different UC.
# MAGIC
# MAGIC Assumptions:
# MAGIC - Dump and restore one catalog at a time (currently overwrites to the same folder)

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("storageLocation", "s3://external-storage-workspace101/edp_catalog/backup", "Storage location for copy")
dbutils.widgets.text("catalogName", "edp_catalog", "information_schema catalog")

# COMMAND ----------

storage_location = dbutils.widgets.get("storageLocation")
catalog_name =  dbutils.widgets.get("catalogName")

table_list = spark.catalog.listTables(f"{catalog_name}.information_schema")

# COMMAND ----------

from pyspark.sql.functions import lit

for table in table_list:
    if table.name not in ['catalogs','schemata','tables','views']:
        info_schema_table_df = spark.sql(f"SELECT * FROM {catalog_name}.information_schema.{table.name}")
        info_schema_table_df.write.format("delta").mode("overwrite").save(f"{storage_location}/{table.name}")

info_schema_table_df = spark.sql(f"SELECT * FROM system.information_schema.catalogs WHERE catalog_name = '{catalog_name}'")
info_schema_table_df.write.format("delta").mode("overwrite").save(f"{storage_location}/catalogs")

info_schema_table_df = spark.sql(f"SELECT * FROM system.information_schema.schemata WHERE catalog_name = '{catalog_name}'")
info_schema_table_df.write.format("delta").mode("overwrite").save(f"{storage_location}/schemata")

info_schema_table_df = spark.sql(f"SELECT * FROM {catalog_name}.information_schema.tables").withColumn("ddl", lit('None'))
info_schema_table_df.write.format("delta").mode("overwrite").save(f"{storage_location}/tables")

info_schema_table_df = spark.sql(f"SELECT * FROM {catalog_name}.information_schema.views").withColumn("ddl", lit('None'))
info_schema_table_df.write.format("delta").mode("overwrite").save(f"{storage_location}/views")


# COMMAND ----------

# MAGIC %md
# MAGIC ##Backing up Tables

# COMMAND ----------

from delta.tables import *

table_location_columns = ["table_catalog","table_schema","table_name","table_location","ddl"]
location_list = []

# Need to filter out Unity Catalog data source that counts as external
describe_table_list = spark.read.format("delta").load(f"{storage_location}/tables").filter("table_type=='EXTERNAL' AND data_source_format <> 'UNITY_CATALOG'")

for d_table in describe_table_list.collect():
    try:
        d_location = spark.sql(f"DESCRIBE EXTENDED {catalog_name}.{d_table.table_schema}.{d_table.table_name}").filter("col_name = 'Location'").select("data_type").head()[0]
        ddl = spark.sql(f"SHOW CREATE TABLE {catalog_name}.{d_table.table_schema}.{d_table.table_name}").collect()[0][0]
        location_list.append([d_table.table_catalog, d_table.table_schema, d_table.table_name, d_location, ddl])
    except Exception as e:
        # Handle the exception
        print(f"An error occurred: {e}")
        location_list.append([d_table.table_catalog, d_table.table_schema, d_table.table_name, "ERROR", ddl]) 
        continue

location_df = spark.createDataFrame(data=location_list, schema = table_location_columns)


table_df = DeltaTable.forPath(spark, f"{storage_location}/tables")
table_df.alias('tables').merge(
location_df.alias('locations'),
'tables.table_catalog = locations.table_catalog and tables.table_schema = locations.table_schema and tables.table_name = locations.table_name'
).whenMatchedUpdate(set =
{
    "storage_sub_directory" : "locations.table_location",
    "ddl" : "locations.ddl"
}
).execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Backing up Views

# COMMAND ----------

from delta.tables import *

table_location_columns = ["table_catalog","table_schema","table_name","ddl"]
ddl_list = []

# Need to filter out Unity Catalog data source that counts as external
describe_view_list = spark.read.format("delta").load(f"{storage_location}/views")

for view in describe_view_list.collect():
    ddl = spark.sql(f"SHOW CREATE TABLE {catalog_name}.{view.table_schema}.{view.table_name}").collect()[0][0]
    ddl_list.append([view.table_catalog, view.table_schema, view.table_name, ddl])

ddl_df = spark.createDataFrame(data=ddl_list, schema = table_location_columns)


table_df = DeltaTable.forPath(spark, f"{storage_location}/views")
table_df.alias('tables').merge(
ddl_df.alias('ddls'),
'tables.table_catalog = ddls.table_catalog and tables.table_schema = ddls.table_schema and tables.table_name = ddls.table_name'
).whenMatchedUpdate(set =
{
    "ddl" : "ddls.ddl"
}
).execute()

# COMMAND ----------

tables_df = spark.read.format("delta").load(f"{storage_location}/tables").filter("table_type='EXTERNAL' and storage_sub_directory='ERROR'")
display(tables_df)
