# Databricks notebook source
# MAGIC %md
# MAGIC ##Comparing catalog with backup

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("Catalog", "edp_catalog", "The replicated catalog")
dbutils.widgets.text("StorageAddress", "abfss://ucmanagedtable@dataedpdlsys1ppdstaaue.dfs.core.windows.net/backup", "The original backup location")

# COMMAND ----------

catalog = dbutils.widgets.get("Catalog")
backup_storage_location = dbutils.widgets.get("StorageAddress")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare Table Lists in information_schema of Each catalog

# COMMAND ----------

# For the first catalog
tables_catalog = [table.name for table in spark.catalog.listTables(f"{catalog}.information_schema")]

# For the backup
tables_backup = spark.read.format("delta").load(f"{backup_storage_location}/tables").filter("table_schema == 'information_schema'").select("table_name").collect()
tables_backup = [row.table_name for row in tables_backup]

# Compare the lists
tables_match = set(tables_catalog) == set(tables_backup)
if not tables_match:
  print("ERROR: The Information schemas are not match")
  print(f"tables in the information schema of the catalog: {set(tables_catalog)}")
  print(f"tables in the information schema of the backup: {set(tables_backup)}")
  dbutils.notebook.exit("Stopping execution because the condition was met")
else:
  print("The Information schemas are match")


# COMMAND ----------

exclude_tables_df = spark.read.format("delta").load(f"{backup_storage_location}/tables").filter("table_type='EXTERNAL' and storage_sub_directory='ERROR'")
backup_views_df = spark.read.format("delta").load(f"{backup_storage_location}/views").fillna('None')

for table in tables_catalog:
  
  # if table != 'columns':
  #   continue
  
  df_a = spark.read.format("delta").table(f"{catalog}.information_schema.{table}").fillna('None')
  df_b = spark.read.format("delta").load(f"{backup_storage_location}/{table}").fillna('None')
  if table in 'tables':
    df_a = df_a.filter("table_schema != 'information_schema'")
    df_b = df_b.drop("ddl")
    df_b = df_b.join(backup_views_df, on=["table_schema","table_name"], how="anti")
  elif table == 'views':
    df_a = df_a.filter("table_schema != 'information_schema'")
    df_b = df_b.drop("ddl")
  elif table == 'columns':
    columns = ["comment","ordinal_position","full_data_type"]
    df_a = df_a.drop(*columns)
    df_a = df_a.filter("table_schema != 'information_schema'")
    df_b = df_b.drop(*columns)
    df_b = df_b.join(exclude_tables_df, on=["table_schema","table_name"], how="anti")
    df_b = df_b.join(backup_views_df, on=["table_schema","table_name"], how="anti")

  for column in ['table_catalog', 'catalog_name', 'created', 'created_by', 'last_altered', 'last_altered_by','storage_sub_directory','table_type','is_insertable_into', 'grantor', 'constraint_catalog','unique_constraint_catalog','comment']:
    if column in df_a.columns:
      df_a = df_a.drop(column)
      df_b = df_b.drop(column)

  print(f"checking {catalog}.information_schema.{table} with backup...")
  if df_a.count() == df_b.count():
    print("PASSED: The row counts match!")
  else:
    print("ERROR: The row counts don't match!")
    print(f"Count {catalog}.information_schema.{table}: {df_a.count()}")
    print(f"Count backup {table}: {df_b.count()}")
    # break
  diff1 = df_b.exceptAll(df_a)
  diff2 = df_a.exceptAll(df_b)
  if diff1.count() != 0 or diff2.count() != 0:
    print(f"ERROR: There are discrepencies in {table}")
    print(f"The items that exist in back up but not in {catalog}")
    display(diff1)
    print(f"The items that exist in {catalog} up but not the backup")
    display(diff2)
  else:
    print(f"PASSED: {table} is identical between the catalogs!")

# COMMAND ----------

tables_df = spark.read.format("delta").load(f"{backup_storage_location}/tables").filter("table_schema<>'information_schema' and table_type<>'EXTERNAL' and storage_sub_directory<>'ERROR'")
# .filter("table_schema='wemdb_transformed'")
display(tables_df)
