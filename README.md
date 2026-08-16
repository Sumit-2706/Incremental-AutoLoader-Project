# Project Sentinel — Incremental Auto Loader Pipeline

An incremental data ingestion pipeline built on Databricks Auto Loader and
Delta Lake, following a Medallion Architecture (**Bronze → Silver → Gold**).
It simulates a real-time financial transaction processing system
(UPI-style) and demonstrates incremental file detection, checkpointing,
schema evolution, deduplication, data quality enforcement, rule-based
anomaly detection, and full run-level observability.

Every number in this README is a real captured result from a live
Databricks workspace run — none of it is illustrative or hypothetical.

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Environment (Confirmed, Not Assumed)](#2-environment-confirmed-not-assumed)
3. [Architecture](#3-architecture)
4. [Data Model](#4-data-model)
5. [Bugs Found and Fixed vs. the Reference Notebook](#5-bugs-found-and-fixed-vs-the-reference-notebook)
6. [Testing — Results](#6-testing--results)
7. [Performance & Security Review](#7-performance--security-review)
8. [Dashboard](#8-dashboard)
9. [Screenshot Evidence](#9-screenshot-evidence)
10. [Project Structure](#10-project-structure)
11. [Setup & How to Run](#11-setup--how-to-run)
12. [Limitations](#12-limitations)
13. [Future Improvements](#13-future-improvements)
14. [Learning Outcomes](#14-learning-outcomes)

---

## 1. Problem Statement

Batch reprocessing an entire dataset every time new data arrives is
wasteful and doesn't scale. A production ingestion system needs to:

- detect and process only **new** files, not the whole history, on every run
- survive **schema changes** in source data without manual intervention
- guarantee **ACID correctness** on concurrent/incremental writes
- **catch and quarantine bad data** instead of silently corrupting downstream tables
- be **observable** — every run should be auditable after the fact

---

## 2. Environment (Confirmed, Not Assumed)

Verified directly in the workspace via a discovery notebook *before* any
pipeline code was written — see [`00_environment_discovery.py`](00_environment_discovery.py).
Confirming instead of assuming caught a real mismatch early: the reference
notebook this project started from assumed Azure + Hive Metastore; this
workspace is AWS + Unity Catalog (see [Section 5](#5-bugs-found-and-fixed-vs-the-reference-notebook)).

| Item | Value |
|---|---|
| Platform | Databricks Free Edition |
| Cloud provider | AWS |
| Compute | Serverless |
| Spark version | 4.1.0 |
| Catalog | Unity Catalog (`workspace`) |
| Storage | Unity Catalog Volume (`workspace.sentinel.files`) |
| Delta Lake | Confirmed working |
| Auto Loader | Confirmed available |

---

## 3. Architecture

![Pipeline architecture](architecture/pipeline.svg)

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

| Choice | Reason |
|---|---|
| Auto Loader over plain batch read | Tracks already-processed files via checkpoint — re-running never reprocesses old data (proven in [Test 1](#6-testing--results)). |
| Delta Lake over plain Parquet | ACID transactions, native schema evolution (`mergeSchema`), and `MERGE INTO` for upserts — none of which Parquet alone provides. |
| Medallion architecture | Bronze stays raw and unfiltered (a faithful record of what was actually received). Silver enforces business validity. Gold is business-ready aggregation. A bug in aggregation logic can never corrupt raw history. |
| Unity Catalog Volumes over DBFS root / cloud paths | This workspace has UC enabled; Volumes are the UC-native managed storage layer — no external cloud credentials needed anywhere in the project. |
| `foreachBatch` + `MERGE` for Silver | Native streaming `MERGE` doesn't exist in Structured Streaming; `foreachBatch` is the documented pattern for applying upsert logic per microbatch. |
| Serverless, `trigger(availableNow=True)` | No always-on cluster cost; each run processes everything available, then stops — the cost-efficient pattern for this workload. |

---

## 4. Data Model

| Field | Type | Notes |
|---|---|---|
| `transaction_id` | INT | business key, deduplicated on this |
| `user_id` | INT | pseudonymous — no PII |
| `amount` | DOUBLE | must be `> 0` to be valid |
| `transaction_time` | TIMESTAMP | event time (can arrive late) |
| `payment_method` | STRING | added later via schema evolution — nullable, optional |
| `source_file` | STRING | Bronze metadata — audit trail |
| `file_arrival_time` | TIMESTAMP | Bronze metadata |
| `ingestion_time` | TIMESTAMP | Bronze metadata |
| `processed_time` | TIMESTAMP | Silver metadata |

Full per-table schemas (including quarantine, Gold tables, and the audit
log) are in [`docs/data_dictionary.md`](docs/data_dictionary.md).

---

## 5. Bugs Found and Fixed vs. the Reference Notebook

| # | Bug | Fix |
|---|---|---|
| 1 | Metadata columns used bare string literals instead of `col(...)` — would have written the literal text `"_metadata.file_path"` into every row instead of the actual file path | `col("_metadata.file_path")` |
| 2 | `MERGE INTO` assumed the target table already exists | Explicitly create `silver_transactions` and `silver_quarantine` with a defined schema before the first streaming write |
| 3 | Fragile private-API call: `microBatchDF._jdf.sparkSession().sql(...)` | Replaced with standard `spark.sql(...)` |
| 4 | Missing `mergeSchema` on the Bronze writer — a new source column causes `DELTA_METADATA_MISMATCH` even after Auto Loader's reader schema has already evolved | Added `.option("mergeSchema", "true")` |
| 5 | Environment mismatch — reference notebook assumed Azure ADLS (`abfss://`) and legacy Hive Metastore | Adapted all paths to UC Volumes and three-level (`catalog.schema.table`) naming, confirmed live in `00_environment_discovery.py` |
| 6 | Audit log write failed with `CANNOT_DETERMINE_TYPE` — Spark can't infer a type for a single-row DataFrame where `error_message` is `None` on a successful run | Passed an explicit `StructType` schema to `createDataFrame` instead of relying on inference |

Full debugging narrative — including the serverless session-state
`NameError`s hit during actual development — is in
[`docs/troubleshooting.md`](docs/troubleshooting.md).

---

## 6. Testing — Results

All 8 tests executed against the live workspace, not simulated.
Methodology for each is in [`docs/testing.md`](docs/testing.md).

| # | Test | Input | Expected | Actual | Status |
|---|---|---|---|---|---|
| 1 | Incremental ingestion | File 1 (5 rows), then File 2 (3 rows) | Total 8, no reprocessing of File 1 | Total 8, `source_file` correctly attributed | PASS |
| 2 | Invalid record (negative amount) | 1 row, `amount = -50` | Routed to quarantine, not Silver | Quarantine count = 1 | PASS |
| 3 | Within-batch duplicate | Same `transaction_id` twice in one file | Collapses to 1 row in Silver | 1 row | PASS |
| 4 | Cross-batch duplicate / update | Same `transaction_id` re-sent in a later file | `MERGE` updates in place, no new row | 1 row (unchanged count) | PASS |
| 5 | Schema evolution (reader) | New column `payment_method` in a new file | Auto Loader detects, requires restart | `UnknownFieldException`, then succeeded on retry | PASS |
| 6 | Schema evolution (target table) | Same as above | Delta table schema adds column via `mergeSchema` | Column added, old rows `NULL` | PASS |
| 7 | Late-arriving data | `transaction_id` 1012, `transaction_time` older than already-processed rows | Correctly upserted regardless of timestamp | Present in Silver, `amount=175.25` | PASS |
| 8 | Malformed record (bad data type) | `transaction_id` 1013, `amount = "BADVALUE"` | Caught by `_rescued_data` / quarantine, pipeline does not crash | `_rescued_data` captured original value; routed to quarantine with reason `"missing amount"`; Bronze/Silver/Gold all completed without crashing | PASS |

**Final verified counts**: Bronze = 157, Silver = 153, Quarantine = 4.

---

## 7. Performance & Security Review

Both executed directly against the live workspace, not just described —
see the final cells of [`00_environment_discovery.py`](00_environment_discovery.py).

### Performance — `OPTIMIZE workspace.sentinel.silver_transactions ZORDER BY (user_id)`

| Metric | Before | After | Change |
|---|---|---|---|
| Files | 4 | 1 | -75% |
| Table size | 14,276 bytes | 5,841 bytes | -59% |
| Query latency (`user_id` filter) | 1.609s | 1.044s | -35% |

At this row count, the file-count and size reduction are the solid
evidence — mechanically real and directly attributable to `OPTIMIZE`. The
latency improvement is consistent with less overhead but isn't strong
proof of the Z-order data-skipping mechanism at this scale; that payoff
compounds at production volume (thousands of files, gigabytes+), not a
few hundred rows in one file. Reported honestly rather than oversold.

### Security

- `SHOW GRANTS ON SCHEMA workspace.sentinel` — reviewed; owner-only
  access, nothing broadened beyond Unity Catalog defaults.
- `DESCRIBE TABLE silver_transactions` — reviewed column-by-column; no PII
  present, only a pseudonymous `user_id` integer (no name/email/phone).
- Manual source read-through — zero hardcoded credentials, API keys, or
  connection strings anywhere in the project; Unity Catalog Volumes grant
  storage access via workspace identity, not embedded secrets.

---

## 8. Dashboard

A Databricks SQL Dashboard sits on top of the Gold layer — 4 widgets
(volume/value summary, top users by spend, high-value transactions over
time, flagged anomalies). Built entirely inside Unity Catalog with no
external BI tool or extra credentials. Setup steps and the exact SQL for
each widget are in [`docs/dashboard_setup.md`](docs/dashboard_setup.md).

---

## 9. Screenshot Evidence

Every claim in this README (test results, schema evolution failure/recovery,
performance benchmark, security review, dashboard) has a corresponding
screenshot captured directly from the live workspace — not staged.

- Capture checklist and what each item proves: [`docs/screenshot_plan.md`](docs/screenshot_plan.md)
- Compiled evidence document (all screenshots, organized by item):
  [`Project_Sentinel_Screenshot_Evidence.docx`](Project_Sentinel_Screenshot_Evidence.docx)

---

## 10. Project Structure

```
project-sentinel/
├── README.md
├── Project_Sentinel_Screenshot_Evidence.docx
├── 00_environment_discovery.py
├── project_sentinel_pipeline.py     # Bronze + Silver + Gold + monitoring + anomaly detection
├── architecture/
│   └── pipeline.svg                 # visual architecture diagram
├── docs/
│   ├── data_dictionary.md
│   ├── testing.md
│   ├── troubleshooting.md
│   ├── demo_script.md
│   ├── screenshot_plan.md
│   ├── dashboard_setup.md           # SQL Dashboard over the Gold layer
│   └── internship_report.md
└── screenshots/                     # raw screenshot files, per docs/screenshot_plan.md
```

---

## 11. Setup & How to Run

1. Import `project_sentinel_pipeline.py` into a Databricks workspace with
   Unity Catalog enabled (Workspace → Import).
2. Run the **SETUP** cell first every session — serverless compute
   detaches after inactivity and Python variables/functions don't persist
   across a detach (Delta tables and Volume files do).
3. Drop CSV files into `/Volumes/workspace/sentinel/files/raw_landing/`.
4. Run the pipeline cells in order: Bronze → Silver → Gold → Anomaly Detection.
5. Query `workspace.sentinel.pipeline_audit_log` to see run history.
6. Follow [`docs/dashboard_setup.md`](docs/dashboard_setup.md) to build the
   SQL Dashboard once Gold has data.

---

## 12. Limitations

- Demonstration-scale (thousands, not billions, of rows) — see
  [Section 13](#13-future-improvements) for how this would scale.
- Anomaly detection is rule-based by design (no ML unless genuinely
  justified) — the frequency-spike threshold needs a larger, more varied
  dataset to be statistically meaningful.
- Gold layer uses `overwrite` mode (recomputed each run), not incremental
  aggregation — acceptable at this scale, would need incremental
  aggregation (e.g. Delta Live Tables or streaming aggregates) at higher
  volume.

---

## 13. Future Improvements

- **Delta Live Tables (DLT)** for declarative pipeline definition and
  built-in expectations-based data quality.
- **Partitioning** Bronze/Silver by `event_date` once data volume grows.
- **Scheduled Jobs** trigger instead of manual `availableNow` runs.
- **Configurable anomaly thresholds** via a config table instead of
  constants.

---

## 14. Learning Outcomes

- The difference between Auto Loader's reader schema log and a Delta
  table's own schema — both must independently support evolution.
- Serverless compute session lifecycle: Delta tables and checkpoints are
  durable; Python session state is not.
- The practical difference between Bronze (raw fidelity) and Silver
  (business validity) as a data-quality architecture, not just a naming
  convention.

---

*Built as part of a data engineering internship project. See
`docs/internship_report.md` for the full write-up.*
