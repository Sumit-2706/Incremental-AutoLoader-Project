# Project Sentinel — Incremental AutoLoader Pipeline

## 1. Overview

Project Sentinel is an incremental data ingestion pipeline built on **Databricks
Auto Loader** and **Delta Lake**, following a **Medallion Architecture**
(Bronze → Silver → Gold). It simulates a real-time financial transaction
processing system (UPI-style), and demonstrates incremental file detection,
checkpointing, schema evolution, deduplication, data quality enforcement, and
rule-based anomaly detection.

## 2. Problem Statement

Batch reprocessing of an entire dataset every time new data arrives is
wasteful and doesn't scale. A production ingestion system needs to:
- detect and process **only new files**, not the whole history, on every run
- survive schema changes in source data without manual intervention
- guarantee ACID correctness on concurrent/incremental writes
- catch and quarantine bad data instead of silently corrupting downstream tables
- be observable — every run should be auditable after the fact

## 3. Environment (confirmed, not assumed)

| Item | Value |
|---|---|
| Platform | Databricks Free Edition |
| Cloud provider | AWS |
| Compute | Serverless |
| Spark version | 4.1.0 |
| Catalog | Unity Catalog (`workspace`) |
| Storage | Unity Catalog Volume (`workspace.sentinel.files`) |

This was verified directly in the workspace via a discovery notebook before
any pipeline code was written — see `00_environment_discovery.py`.

## 4. Architecture

```
Synthetic CSV files
        |
Unity Catalog Volume (raw_landing/)
        |
Databricks Auto Loader (cloudFiles, checkpointed)
        |
Bronze Delta Table (raw, append-only, schema-evolving)
        |
Data Quality + Deduplication (foreachBatch, rule-based)
        |
   +----+----+
   |         |
Silver     Quarantine
(valid)    (invalid, preserved & auditable)
   |
Business Aggregation (SQL)
   |
Gold Delta Tables (summary, high-value, user metrics, anomalies)
   |
Monitoring / Audit Log (every stage, every run)
```

### Why these choices

- **Auto Loader over plain batch read**: tracks already-processed files via
  checkpoint, so re-running the pipeline never reprocesses old data — proven
  directly (see Section 7, Test 1).
- **Delta Lake over plain Parquet**: ACID transactions, schema evolution
  support (`mergeSchema`), and `MERGE INTO` for upserts — none of which plain
  Parquet provides.
- **Medallion architecture**: Bronze stays raw and unfiltered (a faithful
  record of what was actually received — useful for reprocessing/audit).
  Silver enforces business validity. Gold is business-ready aggregation.
  Separating these means a bug in aggregation logic never corrupts raw history.
- **Unity Catalog Volumes over DBFS root / cloud paths**: this workspace has
  Unity Catalog enabled; Volumes are the UC-native managed storage layer and
  keep the project self-contained without needing external cloud credentials.
- **`foreachBatch` + `MERGE` for Silver**: native streaming `MERGE` doesn't
  exist in Structured Streaming; `foreachBatch` is the documented pattern for
  applying upsert logic to each microbatch.
- **Serverless, `trigger(availableNow=True)`**: no always-on cluster cost;
  each pipeline run processes everything available, then stops — the correct
  cost-efficient pattern for this workload's volume.

## 5. Data Model

| Field | Type | Notes |
|---|---|---|
| transaction_id | INT | business key, deduplicated on this |
| user_id | INT | |
| amount | DOUBLE | must be > 0 to be valid |
| transaction_time | TIMESTAMP | event time (can arrive late) |
| payment_method | STRING | added later via schema evolution — nullable, optional |
| source_file | STRING | Bronze metadata — audit trail |
| file_arrival_time | TIMESTAMP | Bronze metadata |
| ingestion_time | TIMESTAMP | Bronze metadata |
| processed_time | TIMESTAMP | Silver metadata |

## 6. Known bugs found and fixed vs. the internship reference notebook

1. **Metadata columns using bare string literals** instead of `col(...)` —
   would have written the literal text `"_metadata.file_path"` into every
   row instead of the actual file path. Fixed with `col("_metadata.file_path")`.
2. **`MERGE INTO` on a table that doesn't exist yet** — the reference
   notebook's Silver MERGE assumes the target table is pre-created; fixed by
   explicitly creating `silver_transactions` and `silver_quarantine` with a
   defined schema before the first streaming write.
3. **Fragile private-API call** — `microBatchDF._jdf.sparkSession().sql(...)`
   replaced with standard `spark.sql(...)`.
4. **Missing `mergeSchema` on the Bronze writer** — without it, a new column
   in the source data causes a `DELTA_METADATA_MISMATCH` failure on write,
   even after Auto Loader's reader schema has already evolved. Added
   `.option("mergeSchema", "true")`.
5. **Environment mismatch** — reference notebook assumed Azure ADLS
   (`abfss://`) and Legacy Hive Metastore; this workspace runs AWS with Unity
   Catalog enabled. Adapted all paths to UC Volumes and three-level
   (`catalog.schema.table`) naming.

## 7. Testing — results

> Fill in the exact screenshot/output reference for each row once captured.
> Do not mark PASS until the actual output has been observed and verified.

| # | Test | Input | Expected | Actual | Status |
|---|---|---|---|---|---|
| 1 | Incremental ingestion | File 1 (5 rows), then File 2 (3 rows) | Total 8, no reprocessing of File 1 | Total 8, source_file correctly attributed | PASS |
| 2 | Invalid record (negative amount) | 1 row, amount = -50 | Routed to quarantine, not Silver | Quarantine count = 1 | PASS |
| 3 | Within-batch duplicate | Same transaction_id twice in one file | Collapses to 1 row in Silver | 1 row | PASS |
| 4 | Cross-batch duplicate / update | Same transaction_id re-sent in a later file | MERGE updates in place, no new row | 1 row (unchanged count) | PASS |
| 5 | Schema evolution (reader) | New column `payment_method` in a new file | Auto Loader detects, requires restart | `UnknownFieldException`, then succeeded on retry | PASS |
| 6 | Schema evolution (target table) | Same as above | Delta table schema adds column via `mergeSchema` | Column added, old rows NULL | PASS |
| 7 | Late-arriving data | transaction_id 1012, transaction_time older than already-processed rows | Correctly upserted regardless of timestamp | Present in Silver, amount=175.25 | PASS |
| 8 | Malformed record (bad data type) | transaction_id 1013, amount = "BADVALUE" | Caught by `_rescued_data` / quarantine, pipeline does not crash | `_rescued_data` captured original value; routed to quarantine with reason "missing amount"; Bronze/Silver/Gold all completed without crashing | PASS |

**Final verified counts**: Bronze = 15, Silver = 11 (valid, unique transaction_ids), Quarantine = 2 (1005: negative amount, 1013: malformed amount).

## 7a. Performance & Security Review

Both were executed directly against the live workspace (not just described)
— see the final cells of `00_environment_discovery.py`.

**Performance** — `OPTIMIZE workspace.sentinel.silver_transactions ZORDER
BY (user_id)`:

| Metric | Before | After | Change |
|---|---|---|---|
| Files | 4 | 1 | -75% |
| Table size | 14,276 bytes | 5,841 bytes | -59% |
| Query latency (`user_id` filter) | 1.609s | 1.044s | -35% |

At 151 rows the absolute latency gain is modest — expected at this scale.
The mechanism (fewer files, Z-order data skipping) is what compounds at
production volume.

**Security** — reviewed via `SHOW GRANTS ON SCHEMA workspace.sentinel`
(returned empty: owner-only access, nothing broadened beyond UC defaults),
`DESCRIBE TABLE silver_transactions` (confirmed no PII — only a
pseudonymous `user_id`, no name/email/phone), and a manual code
read-through (zero hardcoded credentials; UC Volumes handle storage access
natively).

## 8. Project Structure

```
project-sentinel/
├── README.md
├── 00_environment_discovery.py
├── project_sentinel_pipeline.py     # Bronze + Silver + Gold + monitoring + anomaly detection
├── docs/
│   ├── data_dictionary.md
│   ├── testing.md
│   └── troubleshooting.md
└── screenshots/
```

## 9. Setup & How to Run

1. Import `project_sentinel_pipeline.py` into a Databricks workspace with
   Unity Catalog enabled (Workspace → Import).
2. Run the **SETUP** cell first — every session, since serverless compute
   detaches after inactivity and Python variables/functions don't persist
   across a detach (Delta tables and Volume files do).
3. Drop CSV files into `/Volumes/workspace/sentinel/files/raw_landing/`.
4. Run the pipeline cells in order: Bronze → Silver → Gold → Anomaly Detection.
5. Query `workspace.sentinel.pipeline_audit_log` to see run history.

## 10. Limitations

- Demonstration-scale (thousands, not billions, of rows) — see Section 11
  for how this would scale.
- Anomaly detection is rule-based by design (Rule 5: no ML unless genuinely
  justified) — frequency-spike threshold needs a larger, more varied dataset
  to be statistically meaningful.
- Gold layer uses `overwrite` mode (recomputed each run), not incremental
  aggregation — acceptable at this scale, would need incremental aggregation
  (e.g. Delta Live Tables or streaming aggregates) at higher volume.

## 11. Future Improvements

- Delta Live Tables (DLT) for declarative pipeline definition and built-in
  expectations-based data quality.
- Partitioning Bronze/Silver by `event_date` once data volume grows.
- Scheduled Jobs trigger instead of manual `availableNow` runs.
- Configurable anomaly thresholds via a config table instead of constants.

## 12. Learning Outcomes

- Difference between Auto Loader's **reader schema log** and a **Delta
  table's own schema** — both must independently support evolution.
- Serverless compute session lifecycle: Delta tables/checkpoints are durable;
  Python session state is not.
- Practical difference between Bronze (raw fidelity) and Silver (business
  validity) as a data-quality architecture, not just a naming convention.
