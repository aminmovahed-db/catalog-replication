# Databricks notebook source
# MAGIC %md
# MAGIC ##Re-create catalog, schemas and external tables at a remote UC
# MAGIC
# MAGIC This notebook will read from an external container with a backup from a source catalog and re-create it.
# MAGIC
# MAGIC If run on a DR site, the information_schema parameter should be the same as the source catalog.
# MAGIC
# MAGIC Assumptions:
# MAGIC - The storage credential(s) and external location(s) of the parent external location needs to be created on the target UC beforehand.
# MAGIC - The external location is taken from the overwritten storage_sub_directory column on information_schema.tables (previously it was formed by ```<storage location root>/<schema name>/<table name>```)
# MAGIC - All tables are Delta

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("backupStorageLocation", "s3://external-storage-workspace101/edp_catalog/backup", "Storage with source catalog info")
dbutils.widgets.text("storageLocation", "s3://external-storage-workspace101/edp_catalog/managed", "Storage for managed tables in the new catalog")
dbutils.widgets.text("catalogName", "edp_catalog_new", "information_schema catalog")
dbutils.widgets.dropdown("testCatalog", 'True', ['True', 'False'])

# COMMAND ----------

backup_storage_location = dbutils.widgets.get("backupStorageLocation")
managed_storage_location = dbutils.widgets.get("storageLocation")
catalog_name =  dbutils.widgets.get("catalogName")
test_catalog = dbutils.widgets.get("testCatalog") == "True"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create catalog

# COMMAND ----------

catalog_df = spark.read.format("delta").load(f"{backup_storage_location}/catalogs")

spark.sql(f"CREATE CATALOG {catalog_name} MANAGED LOCATION '{managed_storage_location}' COMMENT '{catalog_df.collect()[0].comment}'")
spark.sql(f"ALTER CATALOG {catalog_name} SET OWNER to `{catalog_df.collect()[0].catalog_owner}`")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Catalog ACLs

# COMMAND ----------

#Re-run grants from backed-up information_schema.catalog_privileges    
catalog_grant_df = spark.read.format("delta").load(f"{backup_storage_location}/catalog_privileges")
for catalog_grant in catalog_grant_df.collect(): 
    if (catalog_grant.grantor != 'System user' and catalog_grant.inherited_from == 'NONE'):
        spark.sql(f"GRANT {catalog_grant.privilege_type} ON CATALOG {catalog_name} TO `{catalog_grant.grantee}`")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create schemas

# COMMAND ----------

from pyspark.sql.functions import col, upper

#Get only user schemas
schemas_df = spark.read.format("delta").load(f"{backup_storage_location}/schemata").filter("schema_name<>'information_schema'")

#Drop the default schema
# spark.sql(f"DROP SCHEMA {catalog_name}.default")

#Create all user schemas on the target catalog
for schema in schemas_df.collect(): 
    if schema.schema_name != 'default':
        spark.sql(f"CREATE SCHEMA {catalog_name}.{schema.schema_name} COMMENT '{schema.comment}'")
        spark.sql(f"ALTER SCHEMA {catalog_name}.{schema.schema_name} SET OWNER to `{schema.schema_owner}`")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Schema ACLs

# COMMAND ----------

schema_grant_df = spark.read.format("delta").load(f"{backup_storage_location}/schema_privileges")
for schema_grant in schema_grant_df.collect(): 
  if (schema_grant.grantor != 'System user' and schema_grant.inherited_from == 'NONE'):
    if schema_grant.privilege_type == 'CREATE_VIEW':
      continue
    spark.sql(f"GRANT {schema_grant.privilege_type} ON SCHEMA {catalog_name}.{schema_grant.schema_name} TO `{schema_grant.grantee}`")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create external tables

# COMMAND ----------

def clean_ddl(script):

  script = script.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
  
  # Find the index where 'TBLPROPERTIES' starts
  tblproperties_index = script.find('TBLPROPERTIES')

  # Keep only the part of the script before 'TBLPROPERTIES'
  if tblproperties_index != -1:  # Check if 'TBLPROPERTIES' was found
      cleaned_script = script[:tblproperties_index]
  else:
      cleaned_script = script 

  return cleaned_script

def remove_location(script):
    # Split the script into lines
    lines = script.splitlines()

    # Filter out the line containing 'LOCATION'
    filtered_lines = [line for line in lines if "LOCATION '" not in line]

    # Join the filtered lines back into a single string
    cleaned_script = '\n'.join(filtered_lines)
    return cleaned_script

# COMMAND ----------

table_error_columns = ["table_catalog","table_schema","table_name","error","ddl"]
table_error_list = []

#Get only external user tables
tables_df = spark.read.format("delta").load(f"{backup_storage_location}/tables").filter("table_schema<>'information_schema' and table_type='EXTERNAL' and storage_sub_directory<> 'ERROR'")

for table in tables_df.collect():
    ddl_script = table.ddl.replace(f"{table.table_catalog}.", f"{catalog_name}.")
    cleaned_ddl = clean_ddl(ddl_script)
    if test_catalog:
        cleaned_ddl = remove_location(cleaned_ddl)
    try:
        spark.sql(cleaned_ddl)
    except Exception as e:
        print(e)
        table_error_list.append([table.table_catalog, table.table_schema, table.table_name, f'{e}', cleaned_ddl])
        continue

if table_error_list:
    table_error = spark.createDataFrame(data=table_error_list, schema = table_error_columns)
    table_error.write.format("delta").mode("overwrite").save(f"{backup_storage_location}/tables_error")

# COMMAND ----------

if table_error_list:
    for i in range(4):
        for table in table_error.collect():
            try:
                spark.sql(table.ddl)       
            except Exception as e:
                print(e)
                continue

# COMMAND ----------

# MAGIC %md
# MAGIC #### Table ACLs

# COMMAND ----------

table_grant_df = spark.read.format("delta").load(f"{backup_storage_location}/table_privileges")

for table_grant in table_grant_df.collect():
  if (table_grant.grantor != 'System user' and table_grant.inherited_from == 'NONE'):
    try:
      spark.sql(f"GRANT {table_grant.privilege_type} ON TABLE {catalog_name}.{table_grant.table_schema}.{table_grant.table_name} TO `{table_grant.grantee}`")
    except Exception as e:
      print(e)
      continue

# COMMAND ----------

for table in tables_df.collect():
  try:
    spark.sql(f"ALTER TABLE {catalog_name}.{table.table_schema}.{table.table_name} SET OWNER to `{table.table_owner}`")
  except Exception as e:
    print(e)
    continue

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create managed tables
# MAGIC
# MAGIC For this to work you'll have to:
# MAGIC
# MAGIC 1) create a managed identity with Storage Blob Contributor permission to the storage account, or create IAM role with the same process used for the primary metastore storage.
# MAGIC 2) create a storage credential on the secondary site using this managed identity/IAM role
# MAGIC 3) create an external location pointing to the storage account/bucket used by the primary metastore
# MAGIC 4) inform the primary storage location as a parameter

# COMMAND ----------

# import requests

# #TODO: Move to secrets
# databricks_url = "<SECRET>"
# my_token = "<SECRET>"

# #TODO: move as parameter
# metastore_id = "<METASTORE ID>"

# header = {'Authorization': 'Bearer {}'.format(my_token)}

# endpoint = f"/api/2.0/unity-catalog/metastores/{metastore_id}"
 
# resp = requests.get(
#   databricks_url + endpoint,
#   headers=header
# )

# base_metastore_url=resp.json().get("storage_root")

# COMMAND ----------

# #if Catalog uses its own storage, change base_metastore_url

# endpoint = f"/api/2.0/unity-catalog/catalogs/{catalog_name}"
 
# resp = requests.get(
#   databricks_url + endpoint,
#   headers=header
# )

# if (resp.json().get("backup_storage_location")):
#     base_metastore_url = resp.json().get("backup_storage_location")

# COMMAND ----------

# #Get only managed tables
# tables_df = spark.read.format("delta").load(f"{backup_storage_location}/tables").filter("table_schema<>'information_schema' and table_type='MANAGED'")

# for table in tables_df.collect():
#     columns_df = spark.read.format("delta").load(f"{backup_storage_location}/columns").filter((col("table_schema") == table.table_schema) & (col("table_name") == table.table_name))
#     columns = return_schema(columns_df)

#     #Extracted path
#     spark.sql(f"CREATE OR REPLACE TABLE {catalog_name}.{table.table_schema}.{table.table_name} CLONE delta.`{base_metastore_url}{table.storage_sub_directory}`")
#     spark.sql(f"ALTER TABLE {catalog_name}.{table.table_schema}.{table.table_name} SET OWNER to `{table.table_owner}`")

# COMMAND ----------


# tables_df = spark.read.format("delta").load(f"{backup_storage_location}/tables").filter("table_name == 'settlements_unpackedsettlementsoutput'")
# display(tables_df)
# for table in tables_df.collect():
#     if table.table_schema != "wemdb_transformed":
#       continue
#     ddl_script = table.ddl.replace(f"{table.table_catalog}", f"{catalog_name}")
#     cleaned_ddl = clean_ddl(ddl_script)
#     cleaned_ddl = remove_location(cleaned_ddl)
#     try:
#         # spark.sql(cleaned_ddl)
#         print(table.table_name)
#     except Exception as e:
#         print(e)
#         continue

