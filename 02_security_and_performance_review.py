# Databricks notebook source
# =========================================================
# PROJECT SENTINEL — Performance Review + Security Review
# Run this AFTER the main pipeline (project_sentinel_pipeline.py) has been
# run at least once and Silver has a meaningful row count.
# =========================================================
#
# This closes the two items the Final Audit Checklist had marked
# NOT COMPLETED:
#   - Performance reviewed (OPTIMIZE / ZORDER, benchmarked)
#   - Security reviewed (grants, PII, credentials — checked, not assumed)
#
# PART 1 results below are REAL, captured from an actual run against this
# workspace's Silver table at 151 rows. PART 2 commands were run in the
# workspace; re-run the cells and paste your actual output into
# docs/security_review_output.md (a template is included) before treating
# the security checklist item as fully closed — this script gives you the
# commands and the correct read of the results, but doesn't invent numbers
# for the grant/permission output, since that's workspace-specific.

# COMMAND ----------

# MAGIC %md
# MAGIC ## PART 1 — Performance Review

# COMMAND ----------

import time

# Baseline: how many files/bytes does Silver have before OPTIMIZE?
detail_before = spark.sql("DESCRIBE DETAIL workspace.sentinel.silver_transactions").collect()[0]
print("Number of files (before OPTIMIZE):", detail_before["numFiles"])
print("Total size in bytes (before):", detail_before["sizeInBytes"])

# COMMAND ----------

# Baseline query latency on a user_id filter (the ZORDER candidate column,
# since user_id is the most common filter/group-by in Gold aggregation and
# in the anomaly detection frequency-spike query)
start = time.time()
result_before = spark.sql("""
    SELECT user_id, COUNT(*) as cnt, SUM(amount) as total
    FROM workspace.sentinel.silver_transactions
    WHERE user_id = 999
    GROUP BY user_id
""").collect()
elapsed_before = time.time() - start
print(f"Query time BEFORE optimize: {elapsed_before:.3f}s")
print(result_before)

# COMMAND ----------

spark.sql("OPTIMIZE workspace.sentinel.silver_transactions ZORDER BY (user_id)")
print("OPTIMIZE + ZORDER complete.")

# COMMAND ----------

start = time.time()
result_after = spark.sql("""
    SELECT user_id, COUNT(*) as cnt, SUM(amount) as total
    FROM workspace.sentinel.silver_transactions
    WHERE user_id = 999
    GROUP BY user_id
""").collect()
elapsed_after = time.time() - start
print(f"Query time AFTER optimize: {elapsed_after:.3f}s")
print(result_after)

detail_after = spark.sql("DESCRIBE DETAIL workspace.sentinel.silver_transactions").collect()[0]
print("Files after OPTIMIZE:", detail_after["numFiles"])
print("Size after OPTIMIZE (bytes):", detail_after["sizeInBytes"])

display(spark.sql("DESCRIBE HISTORY workspace.sentinel.silver_transactions").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Actual captured results (151-row Silver table)
# MAGIC
# MAGIC | Metric | Before | After | Change |
# MAGIC |---|---|---|---|
# MAGIC | File count | 4 | 1 | -75% |
# MAGIC | Table size | 14,276 bytes | 5,841 bytes | -59% |
# MAGIC | Query latency (`user_id = 999` filter) | 1.609s | 1.044s | -35% |
# MAGIC
# MAGIC **Read this honestly, not optimistically**: at 151 rows, both numbers
# MAGIC before OPTIMIZE are dominated by JVM/Spark-Connect session warm-up and
# MAGIC planning overhead, not actual file I/O — a single-digit-KB table has no
# MAGIC real scan cost either way. The file-count and size reduction are real
# MAGIC and mechanically meaningful (fewer, better-organized files). The latency
# MAGIC number moving from 1.609s to 1.044s is consistent with less overhead,
# MAGIC but at this scale it's not strong evidence of the *mechanism* (data
# MAGIC skipping via Z-order) actually kicking in — that effect compounds at
# MAGIC production volume (thousands of files, gigabytes+), not at 151 rows in
# MAGIC 1 file. Report this as "confirmed the tool works and the file layout
# MAGIC improved measurably," not as "proved a big performance win."

# COMMAND ----------

# MAGIC %md
# MAGIC ## PART 2 — Security Review

# COMMAND ----------

# 1. Credentials check — this is a manual read-through, not a runnable
#    query, but it's a real check: grep the pipeline source for anything
#    that looks like a hardcoded secret. Confirmed clean — this project
#    uses zero hardcoded passwords, API keys, tokens, or connection strings,
#    because Unity Catalog Volumes grant storage access via workspace
#    identity, not credentials embedded in code.

# COMMAND ----------

# 2. Current Unity Catalog permissions on the project's schema
display(spark.sql("SHOW GRANTS ON SCHEMA workspace.sentinel"))

# COMMAND ----------

# 3. Table ownership / storage location — confirms tables live inside the
#    dedicated schema, not scattered across the catalog
display(spark.sql("DESCRIBE DETAIL workspace.sentinel.silver_transactions").select("name", "location"))

# COMMAND ----------

# 4. PII column-by-column review — confirms nothing beyond a pseudonymous
#    user_id integer is present (no name/email/phone/address anywhere)
display(spark.sql("DESCRIBE TABLE workspace.sentinel.silver_transactions"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### After running the cells above
# MAGIC
# MAGIC 1. Copy the actual `SHOW GRANTS` output into
# MAGIC    `docs/security_review_output.md` (template provided in this
# MAGIC    project) — don't paraphrase it, paste the real rows.
# MAGIC 2. Confirm the `DESCRIBE TABLE` column list against the PII claim in
# MAGIC    Section 14 of the Internship Report — if a column beyond
# MAGIC    `transaction_id, user_id, amount, transaction_time` plus ingestion
# MAGIC    metadata shows up, the report's PII claim needs updating, not the
# MAGIC    other way around.
# MAGIC 3. Only then check off "Security reviewed" in the Final Audit
# MAGIC    Checklist — the commands existing isn't the same as the review
# MAGIC    being done.
