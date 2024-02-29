# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("catalogName", "edp_catalog_test", "the name of the new catalog")
dbutils.widgets.text("storageLocation", "s3://external-storage-workspace101/edp_catalog/external", "Location for the external tables")

# COMMAND ----------

catalog_name =  dbutils.widgets.get("catalogName")
external_location =  dbutils.widgets.get("storageLocation")

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name} COMMENT 'This is a test catalog for replication.';")
spark.sql(f"ALTER CATALOG {catalog_name} OWNER TO `amin.movahed@databricks.com`;")
spark.sql(f"GRANT ALL PRIVILEGES ON CATALOG {catalog_name} TO `amin.movahed@databricks.com`;")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.test_schema;")
spark.sql(f"GRANT ALL PRIVILEGES ON SCHEMA {catalog_name}.test_schema TO `amin.movahed@databricks.com`;")

# COMMAND ----------

query = f'''
-- Create a new table
CREATE TABLE {catalog_name}.test_schema.test_external_table (
    id INT,
    name STRING,
    age INT,
    year INT,
    month INT
)
PARTITIONED BY (year, month)
LOCATION '{external_location}/test_external_table';
'''
spark.sql(query)

# COMMAND ----------

query = f'''
-- Insert data into the table
INSERT INTO {catalog_name}.test_schema.test_external_table (id, name, age, year, month) VALUES
(1, 'Alice', 30, 2023, 04),
(2, 'Bob', 25, 2021, 12),
(3, 'Charlie', 35, 2023, 07);
'''

spark.sql(query)

# COMMAND ----------

spark.sql(f"GRANT ALL PRIVILEGES ON TABLE {catalog_name}.test_schema.test_external_table TO `amin.movahed@databricks.com`;")
