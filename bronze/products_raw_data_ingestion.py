# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC ## Bronze Layer
# MAGIC ##### products feed -> banking.bronze.cbs_product

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

# The error is likely due to a naming conflict with the variable 'table' shadowing a method.
# Rename the variable to avoid conflict.

entity = "products"
table_name = "cbs_product"

# COMMAND ----------

# DBTITLE 1,Configure storage paths
base="abfss://data@stnajdbanking.dfs.core.windows.net"
source_path=f"{base}/staging/{entity}/"
checkpoint  = f"{base}/bronze/{table_name}/checkpoints"
schema_location = f"{base}/bronze/{table_name}/schemas"
target= f"banking.bronze.{table_name}"


# COMMAND ----------

# DBTITLE 1,Configure storage paths
base="abfss://data@stnajdbanking.dfs.core.windows.net"
source_path=f"{base}/staging/{entity}/"
checkpoint  = f"{base}/bronze/{table_name}/checkpoints"
schema_location = f"{base}/bronze/{table_name}/schemas"
target= f"banking.bronze.{table_name}"


# COMMAND ----------

# MAGIC %md
# MAGIC ### Auto-loader

# COMMAND ----------

df=(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header","true")
    .option("InferSchema","true")
    .option("cloudFiles.maxFilesPerTrigger", 1)
    .option("cloudFiles.schemaLocation", schema_location)
    .load(source_path)
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("bronze_ingested_at", current_timestamp())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Bronze Table (Append)

# COMMAND ----------

(   
    df.drop("_rescued_data")  # drops the column from the streaming DataFrame before writing
    .writeStream.format("delta")
    .option("checkpointLocation", checkpoint)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable(target)
    .awaitTermination()
)

display(spark.table(target))

# COMMAND ----------

