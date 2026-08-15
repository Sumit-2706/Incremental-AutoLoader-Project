# Databricks notebook source
# =========================================================
# ENVIRONMENT DISCOVERY — run this first, before writing any pipeline code
# =========================================================
#
# Purpose: confirm the real workspace environment instead of assuming it.
# Every fact in README.md Section 3 ("Environment") was verified here,
# directly against the live workspace, before project_sentinel_pipeline.py
# was written.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Spark / runtime / cloud provider

# COMMAND ----------

print("Spark version:", spark.version)

try:
    print("Databricks Runtime:", spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion"))
except Exception as e:
    print("Runtime tag not available (expected on serverless):", e)

try:
    print("Cloud provider:", spark.conf.get("spark.databricks.cloudProvider"))
except Exception as e:
    print("Cloud provider tag not available:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catalog / Unity Catalog

# COMMAND ----------

print("Current catalog:", spark.sql("SELECT current_catalog()").collect()[0][0])

print()
print("Available catalogs:")
display(spark.sql("SHOW CATALOGS"))

print()
print("Schemas in current catalog:")
display(spark.sql("SHOW SCHEMAS"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Lake availability

# COMMAND ----------

try:
    spark.sql("CREATE TABLE IF NOT EXISTS default.delta_test_check (id INT) USING DELTA")
    print("Delta Lake: WORKING")
    spark.sql("DROP TABLE default.delta_test_check")
except Exception as e:
    print("Delta Lake check failed:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader (cloudFiles) availability

# COMMAND ----------

try:
    df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .schema("id INT")
        .load("/tmp/nonexistent_probe_path/"))
    print("Auto Loader (cloudFiles): available (stream object created)")
except Exception as e:
    print("Auto Loader check note:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the project's dedicated schema + Volume
# MAGIC
# MAGIC A schema/Volume separate from `default` keeps this project
# MAGIC self-contained and easy to tear down cleanly.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.sentinel")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.sentinel.files")

print("Schema and Volume created.")
display(spark.sql("SHOW VOLUMES IN workspace.sentinel"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confirmed result
# MAGIC
# MAGIC | Item | Value |
# MAGIC |---|---|
# MAGIC | Platform | Databricks Free Edition |
# MAGIC | Cloud provider | AWS |
# MAGIC | Compute | Serverless |
# MAGIC | Spark version | 4.1.0 |
# MAGIC | Catalog | Unity Catalog (`workspace`) |
# MAGIC | Storage | Unity Catalog Volume (`workspace.sentinel.files`) |
# MAGIC
# MAGIC Next: run `project_sentinel_pipeline.py`, starting with its SETUP cell.
