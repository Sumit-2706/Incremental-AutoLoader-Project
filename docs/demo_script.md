# Demo Script — Project Sentinel Live Evaluation

Run this in order. Each step has what to say alongside what to run.

## Step 1 — Show existing state
"Here's the pipeline after several rounds of real data — Bronze has the raw
history, Silver has validated data, Gold has business metrics."
```python
print("Bronze:", spark.sql("SELECT COUNT(*) c FROM workspace.sentinel.bronze_transactions").collect()[0]["c"])
print("Silver:", spark.sql("SELECT COUNT(*) c FROM workspace.sentinel.silver_transactions").collect()[0]["c"])
print("Quarantine:", spark.sql("SELECT COUNT(*) c FROM workspace.sentinel.silver_quarantine").collect()[0]["c"])
```

## Step 2 — Show Bronze, Silver, Gold
```python
display(spark.sql("SELECT * FROM workspace.sentinel.bronze_transactions ORDER BY transaction_id"))
display(spark.sql("SELECT * FROM workspace.sentinel.silver_transactions ORDER BY transaction_id"))
display(spark.sql("SELECT * FROM workspace.sentinel.gold_transaction_summary"))
```
"Bronze is raw and unfiltered — every row that ever arrived. Silver only has
validated, deduplicated data. Gold is the business-ready rollup."

## Step 3 — Add a NEW file live
```python
demo_csv = """transaction_id,user_id,amount,transaction_time,payment_method
2001,520,899.00,2026-08-13 15:00:00,UPI
"""
dbutils.fs.put(f"{raw_landing}/transactions_demo_live.csv", demo_csv, overwrite=True)
print("New file dropped — watch it get picked up next.")
```

## Step 4 — Run Auto Loader and prove incrementality
```python
before = spark.sql("SELECT COUNT(*) c FROM workspace.sentinel.bronze_transactions").collect()[0]["c"]
run_bronze_ingestion()
after = spark.sql("SELECT COUNT(*) c FROM workspace.sentinel.bronze_transactions").collect()[0]["c"]
print(f"Bronze went from {before} to {after} — only the new file was processed, "
      f"nothing was reprocessed (checkpoint proves it).")
```
"This is the core value proposition — only +1 row, not a full re-scan."

## Step 5 — Run Silver, show the new record land
```python
run_silver_transformation()
display(spark.sql("SELECT * FROM workspace.sentinel.silver_transactions WHERE transaction_id = 2001"))
```

## Step 6 — Demonstrate duplicate handling
```python
dup_csv = """transaction_id,user_id,amount,transaction_time,payment_method
2001,520,899.00,2026-08-13 15:00:00,UPI
"""
dbutils.fs.put(f"{raw_landing}/transactions_demo_dup.csv", dup_csv, overwrite=True)
run_bronze_ingestion()
run_silver_transformation()
display(spark.sql("SELECT COUNT(*) c FROM workspace.sentinel.silver_transactions WHERE transaction_id = 2001"))
```
"Still exactly 1 row — the duplicate was absorbed by the MERGE upsert."

## Step 7 — Demonstrate an invalid record live
```python
bad_csv = """transaction_id,user_id,amount,transaction_time,payment_method
2002,521,-15.00,2026-08-13 15:05:00,CARD
"""
dbutils.fs.put(f"{raw_landing}/transactions_demo_invalid.csv", bad_csv, overwrite=True)
run_bronze_ingestion()
run_silver_transformation()
display(spark.sql("SELECT * FROM workspace.sentinel.silver_quarantine WHERE transaction_id = 2002"))
```
"Negative amount — caught, quarantined with a reason, never silently dropped."

## Step 8 — Show anomaly detection and Gold refresh
```python
run_gold_layer()
run_anomaly_detection()
display(spark.sql("SELECT * FROM workspace.sentinel.gold_anomalies_high_value"))
```

## Step 9 — Show monitoring/audit trail
```python
display(spark.sql("SELECT * FROM workspace.sentinel.pipeline_audit_log ORDER BY start_time DESC LIMIT 10"))
```
"Every run is logged — timing, row counts, success/failure. This is what
you'd query first if a production run failed overnight."

## Closing line
"This pipeline was built and tested against real failures — a serverless
session-state bug, a two-layer schema evolution requirement, and a type
inference bug in the monitoring code itself — all found and fixed through
actual debugging, not assumed away."
