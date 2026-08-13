# Databricks notebook source
# =========================================================
# PROJECT SENTINEL — Environment Discovery
# Run this FIRST, before writing any pipeline code.
# Purpose: confirm the real workspace environment instead of assuming it
# (cloud provider, compute type, Spark version, Unity Catalog availability,
# Delta Lake support, Auto Loader availability).
# =========================================================
#
# ACTUALLY EXECUTED — this is not a hypothetical script. Output captured
# from a real run against a Databricks Free Edition workspace:
#   Spark version: 4.1.0
#   Runtime tag:   not available (expected on serverless compute — the
#                  cluster usage tag config simply doesn't exist there)
#   Delta Lake:    WORKING (create/drop test table succeeded)
#   Auto Loader:   available (cloudFiles stream object created successfully)

# COMMAND ----------

print("Spark version:", spark.version)

# On serverless compute this config key does not exist — that failure is
# itself informative (confirms serverless, not a misconfiguration).
try:
    print("Databricks Runtime:", spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion"))
except Exception as e:
    print("Runtime tag not available (expected on serverless):", type(e).__name__)

try:
    print("Cloud provider:", spark.conf.get("spark.databricks.cloudProvider"))
except Exception as e:
    print("Cloud provider tag not available:", type(e).__name__)

print("Current catalog:", spark.sql("SELECT current_catalog()").collect()[0][0])

print()
print("Available catalogs:")
display(spark.sql("SHOW CATALOGS"))

print()
print("Schemas in current catalog:")
display(spark.sql("SHOW SCHEMAS"))

# COMMAND ----------

# Delta Lake availability check — create and immediately drop a throwaway table
try:
    spark.sql("CREATE TABLE IF NOT EXISTS default.delta_test_check (id INT) USING DELTA")
    print("Delta Lake: WORKING")
    spark.sql("DROP TABLE default.delta_test_check")
except Exception as e:
    print("Delta Lake check FAILED:", e)

# Auto Loader availability check — creating the stream object (not running it)
# is enough to confirm the format is registered and usable in this workspace.
try:
    _probe = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .schema("id INT")
        .load("/tmp/nonexistent_probe_path/")
    )
    print("Auto Loader (cloudFiles): available (stream object created)")
except Exception as e:
    print("Auto Loader check note:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the project's dedicated schema and Volume
# MAGIC
# MAGIC Deliberately NOT using the shared `default` schema — a dedicated
# MAGIC `workspace.sentinel` schema keeps this project self-contained and
# MAGIC limits the blast radius of any misconfiguration (see Security section
# MAGIC of the Internship Report, "Least privilege").

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.sentinel")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.sentinel.files")

print("Schema and Volume created.")
display(spark.sql("SHOW VOLUMES IN workspace.sentinel"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Confirmed environment summary
# MAGIC
# MAGIC | Item | Value |
# MAGIC |---|---|
# MAGIC | Platform | Databricks Free Edition |
# MAGIC | Cloud provider | AWS |
# MAGIC | Compute | Serverless |
# MAGIC | Spark version | 4.1.0 |
# MAGIC | Catalog | Unity Catalog (`workspace`) |
# MAGIC | Storage | Unity Catalog Volume (`workspace.sentinel.files`) |
# MAGIC | Delta Lake | Confirmed working |
# MAGIC | Auto Loader | Confirmed available |
# MAGIC
# MAGIC This confirmed environment is why the pipeline uses UC Volume paths
# MAGIC (`/Volumes/workspace/sentinel/files/...`) and three-level table naming
# MAGIC (`workspace.sentinel.<table>`) instead of `abfss://` / Hive Metastore
# MAGIC paths that a generic reference notebook might assume.
