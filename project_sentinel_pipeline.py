# Databricks notebook source
# =========================================================
# PROJECT SENTINEL — Incremental AutoLoader Pipeline
# Bronze -> Silver -> Gold | Unity Catalog | AWS | Serverless
# =========================================================
#
# CONFIRMED ENVIRONMENT (do not assume — verified via discovery notebook):
#   Platform:        Databricks Free Edition
#   Cloud provider:  AWS
#   Compute:         Serverless
#   Spark version:   4.1.0
#   Unity Catalog:   Enabled (catalog = workspace)
#   Storage:         Unity Catalog Volume (workspace.sentinel.files)
#
# NOTE ON SESSION STATE (learned via real debugging, see report):
#   Serverless compute detaches after inactivity. Delta tables and files on
#   the Volume persist, but Python variables/functions defined in earlier
#   cells do NOT survive a detach. Always re-run this setup section first
#   after returning to the notebook after a break.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SETUP — run this first every session

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, when

# Create dedicated schema + Volume (idempotent — safe to re-run)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.sentinel")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.sentinel.files")

base_path = "/Volumes/workspace/sentinel/files"
raw_landing = f"{base_path}/raw_landing"
checkpoint_base = f"{base_path}/checkpoints"

dbutils.fs.mkdirs(raw_landing)

print("Setup complete.")
print("raw_landing:", raw_landing)
print("checkpoint_base:", checkpoint_base)

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE LAYER — Auto Loader incremental ingestion
# MAGIC
# MAGIC Bug fix vs. reference notebook: original used bare string literals
# MAGIC (`.withColumn("file_name", "_metadata.file_path")`) which write the
# MAGIC literal text, not the actual value. Fixed here with `col(...)`.
# MAGIC
# MAGIC `mergeSchema` is required on the writer so the target Delta table's
# MAGIC own schema can evolve when Auto Loader detects new source columns
# MAGIC (two separate schema-evolution layers — reader schema log vs. table
# MAGIC schema — both must be handled).

# COMMAND ----------

def run_bronze_ingestion():
    """Reads all new files from raw_landing via Auto Loader and appends to Bronze.
    Safe to call repeatedly — checkpoint ensures already-seen files are skipped."""
    bronze_df = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{checkpoint_base}/bronze_schema")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("header", "true")
        .load(raw_landing)
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("file_arrival_time", col("_metadata.file_modification_time"))
        .withColumn("ingestion_time", current_timestamp())
    )

    (bronze_df.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_base}/bronze_chkpt")
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .toTable("workspace.sentinel.bronze_transactions")
        .awaitTermination()
    )

    cnt = spark.sql("SELECT COUNT(*) AS c FROM workspace.sentinel.bronze_transactions").collect()[0]["c"]
    print("Bronze row count:", cnt)
    return cnt

# COMMAND ----------

# MAGIC %md
# MAGIC ### NOTE: Auto Loader schema evolution behavior (observed, real)
# MAGIC When a genuinely new column arrives in the source data, the FIRST call to
# MAGIC `run_bronze_ingestion()` after that file lands will raise:
# MAGIC   `UnknownFieldException: [UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_FILE]`
# MAGIC This is expected, intentional behavior — Auto Loader logs the new field to
# MAGIC its schema log and requires an explicit re-run (not a silent auto-merge).
# MAGIC Simply call `run_bronze_ingestion()` again and it will succeed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER LAYER — data quality, deduplication, quarantine

# COMMAND ----------

# Pre-create Silver + Quarantine tables with explicit schema.
# (Reference notebook's MERGE fails on first run if target table doesn't
#  already exist with a defined schema — fixed by creating it up front.)

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.sentinel.silver_transactions (
    transaction_id INT,
    user_id INT,
    amount DOUBLE,
    transaction_time TIMESTAMP,
    source_file STRING,
    file_arrival_time TIMESTAMP,
    ingestion_time TIMESTAMP,
    processed_time TIMESTAMP
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.sentinel.silver_quarantine (
    transaction_id INT,
    user_id INT,
    amount DOUBLE,
    transaction_time TIMESTAMP,
    source_file STRING,
    file_arrival_time TIMESTAMP,
    ingestion_time TIMESTAMP,
    quarantine_reason STRING,
    quarantine_time TIMESTAMP
) USING DELTA
""")

print("Silver and Quarantine tables ready.")

# COMMAND ----------

def process_silver_microbatch(microBatchDF, batchId):
    """Data quality gate + dedup + upsert, applied per Auto Loader microbatch.

    Validity rule (Phase 8 — explainable, not silently discarding data):
      - transaction_id must not be null
      - amount must not be null
      - amount must be > 0
    Invalid rows -> quarantine table (append, preserved, auditable).
    Valid rows   -> deduplicated by transaction_id, then MERGE upsert into Silver
                    (handles both in-file duplicates and cross-batch/late updates).
    """
    invalid_df = microBatchDF.filter(
        col("transaction_id").isNull() |
        col("amount").isNull() |
        (col("amount") <= 0)
    ).withColumn(
        "quarantine_reason",
        when(col("transaction_id").isNull(), lit("missing transaction_id"))
        .when(col("amount").isNull(), lit("missing amount"))
        .when(col("amount") <= 0, lit("invalid amount: must be > 0"))
        .otherwise(lit("unknown"))
    ).withColumn("quarantine_time", current_timestamp()) \
     .select("transaction_id", "user_id", "amount", "transaction_time",
             "source_file", "file_arrival_time", "ingestion_time",
             "quarantine_reason", "quarantine_time")

    valid_df = microBatchDF.filter(
        col("transaction_id").isNotNull() &
        col("amount").isNotNull() &
        (col("amount") > 0)
    ).dropDuplicates(["transaction_id"]) \
     .withColumn("processed_time", current_timestamp()) \
     .select("transaction_id", "user_id", "amount", "transaction_time",
             "source_file", "file_arrival_time", "ingestion_time", "processed_time")

    if invalid_df.count() > 0:
        invalid_df.write.format("delta").mode("append") \
            .saveAsTable("workspace.sentinel.silver_quarantine")

    valid_df.createOrReplaceTempView("silver_updates")

    # NOTE: using spark.sql(...) directly here — the reference notebook used
    # the fragile microBatchDF._jdf.sparkSession().sql(...) private-API idiom,
    # which Doc2's own hardening notes flagged as non-standard.
    spark.sql("""
        MERGE INTO workspace.sentinel.silver_transactions AS target
        USING silver_updates AS source
        ON target.transaction_id = source.transaction_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def run_silver_transformation():
    """Streams new Bronze rows through the quality/dedup/upsert logic above."""
    (spark.readStream
        .table("workspace.sentinel.bronze_transactions")
        .writeStream
        .foreachBatch(process_silver_microbatch)
        .option("checkpointLocation", f"{checkpoint_base}/silver_chkpt")
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )
    silver_cnt = spark.sql("SELECT COUNT(*) AS c FROM workspace.sentinel.silver_transactions").collect()[0]["c"]
    quarantine_cnt = spark.sql("SELECT COUNT(*) AS c FROM workspace.sentinel.silver_quarantine").collect()[0]["c"]
    print("Silver row count:", silver_cnt)
    print("Quarantine row count:", quarantine_cnt)
    return silver_cnt, quarantine_cnt

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD LAYER — business-ready analytics

# COMMAND ----------

def run_gold_layer():
    """Builds Gold KPI tables from Silver. Overwrite mode — Gold is a
    recomputed view of current Silver state, not an append-only log."""

    # 1. Overall transaction summary
    spark.sql("""
        SELECT
            COUNT(*) AS total_transactions,
            SUM(amount) AS total_transaction_value,
            ROUND(AVG(amount), 2) AS avg_transaction_amount,
            MAX(amount) AS max_transaction_amount,
            MIN(amount) AS min_transaction_amount,
            COUNT(DISTINCT user_id) AS unique_users
        FROM workspace.sentinel.silver_transactions
    """).write.format("delta").mode("overwrite") \
        .saveAsTable("workspace.sentinel.gold_transaction_summary")

    # 2. High-value transaction flagging (simple, explainable rule-based
    #    anomaly signal — Phase 7: threshold is configurable, not hardcoded logic)
    HIGH_VALUE_THRESHOLD = 5000
    spark.sql(f"""
        SELECT transaction_id, user_id, amount, transaction_time, source_file
        FROM workspace.sentinel.silver_transactions
        WHERE amount > {HIGH_VALUE_THRESHOLD}
        ORDER BY amount DESC
    """).write.format("delta").mode("overwrite") \
        .saveAsTable("workspace.sentinel.gold_high_value_transactions")

    # 3. User-level metrics
    spark.sql("""
        SELECT
            user_id,
            COUNT(*) AS transaction_count,
            SUM(amount) AS total_spent,
            ROUND(AVG(amount), 2) AS avg_transaction_amount
        FROM workspace.sentinel.silver_transactions
        GROUP BY user_id
        ORDER BY total_spent DESC
    """).write.format("delta").mode("overwrite") \
        .saveAsTable("workspace.sentinel.gold_user_metrics")

    print("Gold layer refreshed: gold_transaction_summary, "
          "gold_high_value_transactions, gold_user_metrics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## MONITORING / AUDIT LOGGING (Phase 9)
# MAGIC Every pipeline stage run gets a row here: what ran, how long it took,
# MAGIC row counts before/after, and whether it succeeded or failed. This is
# MAGIC the table you'd query to answer "why did last night's run fail?".

# COMMAND ----------

import uuid
import time
import traceback

spark.sql("""
CREATE TABLE IF NOT EXISTS workspace.sentinel.pipeline_audit_log (
    run_id STRING,
    stage STRING,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    duration_seconds DOUBLE,
    row_count_before BIGINT,
    row_count_after BIGINT,
    status STRING,
    error_message STRING
) USING DELTA
""")

def get_row_count(table_name):
    try:
        return spark.sql(f"SELECT COUNT(*) AS c FROM {table_name}").collect()[0]["c"]
    except Exception:
        return None  # table may not exist yet (e.g. first-ever run)

def run_stage_with_audit(stage_name, target_table, stage_fn, run_id):
    """Wraps a pipeline stage function with timing, row-count, and error capture.
    Never swallows the error silently — logs it AND re-raises, so the notebook
    still shows the failure clearly while the audit table keeps a durable record."""
    start_time = time.time()
    start_ts = spark.sql("SELECT current_timestamp() AS t").collect()[0]["t"]
    before_count = get_row_count(target_table)
    status = "SUCCESS"
    error_message = None

    try:
        stage_fn()
    except Exception as e:
        status = "FAILED"
        error_message = f"{type(e).__name__}: {str(e)[:500]}"
        raise
    finally:
        end_time = time.time()
        end_ts = spark.sql("SELECT current_timestamp() AS t").collect()[0]["t"]
        after_count = get_row_count(target_table)
        duration = round(end_time - start_time, 2)

        log_row = spark.createDataFrame([{
            "run_id": run_id,
            "stage": stage_name,
            "start_time": start_ts,
            "end_time": end_ts,
            "duration_seconds": duration,
            "row_count_before": before_count,
            "row_count_after": after_count,
            "status": status,
            "error_message": error_message
        }])
        log_row.write.format("delta").mode("append") \
            .saveAsTable("workspace.sentinel.pipeline_audit_log")

        print(f"[{stage_name}] status={status} duration={duration}s "
              f"rows {before_count} -> {after_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ANOMALY DETECTION (Phase 7) — expanded
# MAGIC Two explainable, rule-based signals (deliberately not ML — thresholds
# MAGIC are configurable constants, easy to defend in review):
# MAGIC   1. High-value transaction  — amount exceeds a fixed threshold
# MAGIC   2. Frequency spike         — a user has an unusually high transaction
# MAGIC                                count relative to other users (simple
# MAGIC                                z-score-free rule: count > mean + 2*stddev,
# MAGIC                                computed from the data itself, not hardcoded)

# COMMAND ----------

def run_anomaly_detection():
    HIGH_VALUE_THRESHOLD = 5000

    # Signal 1: high-value transactions
    high_value_df = spark.sql(f"""
        SELECT
            transaction_id, user_id, amount, transaction_time, source_file,
            'HIGH_VALUE' AS anomaly_type,
            'amount > {HIGH_VALUE_THRESHOLD}' AS anomaly_reason
        FROM workspace.sentinel.silver_transactions
        WHERE amount > {HIGH_VALUE_THRESHOLD}
    """)

    # Signal 2: frequency spike, threshold computed from the data (mean + 2*stddev)
    stats = spark.sql("""
        SELECT AVG(cnt) AS mean_cnt, STDDEV(cnt) AS std_cnt
        FROM (
            SELECT user_id, COUNT(*) AS cnt
            FROM workspace.sentinel.silver_transactions
            GROUP BY user_id
        )
    """).collect()[0]
    mean_cnt = stats["mean_cnt"] or 0
    std_cnt = stats["std_cnt"] or 0
    freq_threshold = mean_cnt + 2 * std_cnt

    frequency_df = spark.sql(f"""
        SELECT
            user_id,
            COUNT(*) AS transaction_count,
            SUM(amount) AS total_amount,
            'FREQUENCY_SPIKE' AS anomaly_type,
            'transaction_count > mean + 2*stddev ({round(freq_threshold, 2)})' AS anomaly_reason
        FROM workspace.sentinel.silver_transactions
        GROUP BY user_id
        HAVING COUNT(*) > {freq_threshold}
    """) if std_cnt and std_cnt > 0 else None

    high_value_df.write.format("delta").mode("overwrite") \
        .saveAsTable("workspace.sentinel.gold_anomalies_high_value")

    if frequency_df is not None:
        frequency_df.write.format("delta").mode("overwrite") \
            .saveAsTable("workspace.sentinel.gold_anomalies_frequency")
        print(f"Frequency threshold this run: {round(freq_threshold, 2)}")
    else:
        print("Frequency anomaly check skipped — not enough variance in data yet "
              "(need more users/transactions for a meaningful stddev).")

    print("Anomaly detection complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## RUN THE FULL PIPELINE
# MAGIC Call in order. Each stage is idempotent / safe to re-run.
# MAGIC Wrapped with audit logging — check `pipeline_audit_log` after any run.

# COMMAND ----------

pipeline_run_id = str(uuid.uuid4())
print("Pipeline run_id:", pipeline_run_id)

run_stage_with_audit("bronze_ingestion", "workspace.sentinel.bronze_transactions",
                      run_bronze_ingestion, pipeline_run_id)

# COMMAND ----------

run_stage_with_audit("silver_transformation", "workspace.sentinel.silver_transactions",
                      run_silver_transformation, pipeline_run_id)

# COMMAND ----------

run_stage_with_audit("gold_layer", "workspace.sentinel.gold_transaction_summary",
                      run_gold_layer, pipeline_run_id)

# COMMAND ----------

run_stage_with_audit("anomaly_detection", "workspace.sentinel.gold_anomalies_high_value",
                      run_anomaly_detection, pipeline_run_id)

# COMMAND ----------

# MAGIC %md
# MAGIC ## VERIFICATION QUERIES

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.sentinel.gold_transaction_summary"))

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.sentinel.gold_high_value_transactions"))

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.sentinel.gold_user_metrics"))

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.sentinel.silver_quarantine"))

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.sentinel.gold_anomalies_high_value"))

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.sentinel.pipeline_audit_log ORDER BY start_time DESC"))
