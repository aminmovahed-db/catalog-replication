# Databricks notebook source
dbutils.widgets.dropdown("save_file?", 'False', ['True', 'False'])
dbutils.widgets.text("storageLocation", "Not specified", "Location to save views ddl extracts")

# COMMAND ----------

save_file = dbutils.widgets.get("save_file?") == "True"
storage_location =  dbutils.widgets.get("storageLocation")
if save_file and storage_location == "Not specified":
  dbutils.notebook.exit("Error: Location for file storage has not been specified...!")


# COMMAND ----------

spark.sql("USE CATALOG hive_metastore")

# List all databases
databases = spark.sql("SHOW DATABASES").collect()

table_views_columns = ["view_schema","view_name","ddl"]
views_list = []

# Iterate over each database and list views
for db in databases:
    dbName = db.databaseName
    spark.sql(f"USE {dbName}")
    views = spark.sql("SHOW VIEWS").collect()
    for view in views:
        print(f"Extracting DDL for {dbName}.{view.viewName}")
        viewName = view.viewName
        ddl = spark.sql(f"SHOW CREATE TABLE {viewName}").collect()[0][0]
        views_list.append([dbName,viewName, ddl])

views_df = spark.createDataFrame(data=views_list, schema = table_views_columns)

display(views_df)

if save_file:
  views_df.write.format("delta").mode("overwrite").save(f"{storage_location}/HMS_views")

