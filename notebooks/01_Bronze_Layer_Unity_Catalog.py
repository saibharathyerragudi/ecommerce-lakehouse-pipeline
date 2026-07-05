# Databricks notebook source
# MAGIC %md
# MAGIC # 01 - Bronze Layer with Unity Catalog
# MAGIC
# MAGIC This notebook reads raw parquet files copied by Azure Data Factory into `landing-zone-2/raw_files` and writes them as Bronze Delta tables in Unity Catalog.
# MAGIC
# MAGIC Flow:
# MAGIC
# MAGIC `landing-zone-2/raw_files` -> `ecom_db_bharath.raw.raw_files` volume -> `ecom_db_bharath.bronze` Delta tables
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG ecom_db_bharath
# MAGIC COMMENT 'Unity Catalog catalog for the ecommerce lakehouse pipeline';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA ecom_db_bharath.raw
# MAGIC COMMENT 'Raw ADF-landed files from landing-zone-2';
# MAGIC
# MAGIC CREATE SCHEMA ecom_db_bharath.bronze
# MAGIC COMMENT 'Bronze Delta tables converted from raw files';
# MAGIC
# MAGIC CREATE SCHEMA ecom_db_bharath.silver
# MAGIC COMMENT 'Cleaned and standardized Silver Delta tables';
# MAGIC
# MAGIC CREATE SCHEMA ecom_db_bharath.gold
# MAGIC COMMENT 'Business-ready Gold reporting tables';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE EXTERNAL VOLUME ecom_db_bharath.raw.raw_files
# MAGIC LOCATION 'abfss://landing-zone-2@ecomadlsbharath.dfs.core.windows.net/raw_files/'
# MAGIC COMMENT 'Raw files copied by ADF into landing-zone-2/raw_files';

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '/Volumes/ecom_db_bharath/raw/raw_files/';

# COMMAND ----------

raw_base_path = "/Volumes/ecom_db_bharath/raw/raw_files"
bronze_schema = "ecom_db_bharath.bronze"

print(raw_base_path)
print(bronze_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confirm Raw Files
# MAGIC
# MAGIC The Unity Catalog volume `ecom_db_bharath.raw.raw_files` points to the ADLS folder:
# MAGIC
# MAGIC `abfss://landing-zone-2@ecomadlsbharath.dfs.core.windows.net/raw_files/`
# MAGIC
# MAGIC This keeps raw file access governed through Unity Catalog instead of direct workspace paths.

# COMMAND ----------

display(dbutils.fs.ls(raw_base_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Raw Parquet Files
# MAGIC
# MAGIC Each folder inside the volume contains parquet files produced by the ADF ingestion pipeline.
# MAGIC
# MAGIC At this stage, the data is still raw and has not been cleaned or transformed.

# COMMAND ----------

usersDF = spark.read.format("parquet").load(f"{raw_base_path}/users-raw-2")
buyersDF = spark.read.format("parquet").load(f"{raw_base_path}/buyers-raw-2")
sellersDF = spark.read.format("parquet").load(f"{raw_base_path}/sellers-raw-2")
countriesDF = spark.read.format("parquet").load(f"{raw_base_path}/countries-raw-2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Raw Users Data
# MAGIC
# MAGIC Preview a few records from the users dataset to confirm that the parquet files were read successfully.

# COMMAND ----------

display(usersDF.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check Source Row Counts
# MAGIC
# MAGIC Count each raw dataframe before writing to Bronze.
# MAGIC
# MAGIC These counts help confirm that the Bronze tables receive the expected number of records.

# COMMAND ----------

print("users:", usersDF.count())
print("buyers:", buyersDF.count())
print("sellers:", sellersDF.count())
print("countries:", countriesDF.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Bronze Delta Tables
# MAGIC
# MAGIC Write each raw dataframe as a managed Delta table in Unity Catalog.
# MAGIC
# MAGIC Managed tables keep the Bronze layer discoverable and governed in Unity Catalog.
# MAGIC
# MAGIC Bronze tables created:
# MAGIC
# MAGIC - `ecom_db_bharath.bronze.users`
# MAGIC - `ecom_db_bharath.bronze.buyers`
# MAGIC - `ecom_db_bharath.bronze.sellers`
# MAGIC - `ecom_db_bharath.bronze.countries`

# COMMAND ----------

usersDF.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{bronze_schema}.users")

buyersDF.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{bronze_schema}.buyers")

sellersDF.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{bronze_schema}.sellers")

countriesDF.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{bronze_schema}.countries")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Bronze Tables
# MAGIC
# MAGIC Confirm that the four Bronze Delta tables were registered in the `ecom_db_bharath.bronze` schema.

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {bronze_schema}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preview Bronze Users Table
# MAGIC
# MAGIC Query the Bronze users table using its Unity Catalog table name.
# MAGIC
# MAGIC From this point forward, downstream layers should read from table names instead of file paths.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {bronze_schema}.users LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Bronze Row Counts
# MAGIC
# MAGIC Compare final Bronze table row counts after writing.
# MAGIC
# MAGIC This is a simple sanity check to make sure each table was created and populated.

# COMMAND ----------

for table_name in ["users", "buyers", "sellers", "countries"]:
    count_df = spark.sql(f"SELECT COUNT(*) AS row_count FROM {bronze_schema}.{table_name}")
    print(table_name)
    display(count_df)
