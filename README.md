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
2. [Confirmed Environment](#2-confirmed-environment)
3. [Architecture](#3-architecture)
4. [The Pipeline, Layer by Layer](#4-the-pipeline-layer-by-layer)
5. [Data Model](#5-data-model)
6. [Bugs Found and Fixed vs. the Reference Notebook](#6-bugs-found-and-fixed-vs-the-reference-notebook)
7. [Testing — Results](#7-testing--results)
8. [Performance & Security Review](#8-performance--security-review)
9. [Project Structure](#9-project-structure)
10. [Setup & How to Run](#10-setup--how-to-run)
11. [Limitations](#11-limitations)
12. [Future Improvements](#12-future-improvements)
13. [Learning Outcomes](#13-learning-outcomes)

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

## 2. Confirmed Environment

Verified directly in the workspace via a discovery notebook *before* any
pipeline code was written — see [`00_environment_discovery.py`](00_environment_discovery.py).
Confirming instead of assuming caught a real mismatch early: the reference
notebook this project started from assumed Azure + Hive Metastore; this
workspace is AWS + Unity Catalog (see [Section 6](#6-bugs-found-and-fixed-vs-the-reference-notebook)).

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

```
Synthetic CSV files
        │
Unity Catalog Volume (raw_landing/)
        │
Databricks Auto Loader (cloudFiles, checkpointed)
        │
Bronze Delta Table (raw, append-only, schema-evolving)
        │
Data Quality + Deduplication (foreachBatch, rule-based)
        │
   ┌────┴────┐
   │         │
Silver     Quarantine
(valid)    (invalid, preserved & auditable)
   │
Business Aggregation (SQL)
   │
Gold Delta Tables (summary, high-value, user metrics, anomalies)
   │
Monitoring / Audit Log (every stage, every run)
```

**Why these choices:**

| Choice | Reason |
|---|---|
| Auto Loader over plain batch read | Tracks already-processed files via checkpoint — re-running never reprocesses old data (proven in [Test 1](#7-testing--results)). |
| Delta Lake over plain Parquet | ACID transactions, native schema evolution (`mergeSchema`), and `MERGE INTO` for upserts — none of which Parquet alone provides. |
| Medallion architecture | Bronze stays raw and unfiltered (a faithful record of what was actually received). Silver enforces business validity. Gold is business-ready aggregation. A bug in aggregation logic can never corrupt raw history. |
| Unity Catalog Volumes over DBFS root / cloud paths | This workspace has UC enabled; Volumes are the UC-native managed storage layer — no external cloud credentials needed anywhere in the project. |
| `foreachBatch` + `MERGE` for Silver | Native streaming `MERGE` doesn't exist in Structured Streaming; `foreachBatch` is the documented pattern for applying upsert logic per microbatch. |
| Serverless, `trigger(availableNow=True)` | No always-on cluster cost; each run processes everything available, then stops — the cost-efficient pattern for this workload. |

---

## 4. The Pipeline, Layer by Layer

### Layer 0 — Environment Discovery (`00_environment_discovery.py`)
Confirms Unity Catalog, Delta Lake, and Auto Loader are actually available,
then provisions the project's dedicated schema and Volume
(`workspace.sentinel`). Run once per workspace, before anything else.

### Layer 1 — Bronze (raw ingestion)
Auto Loader (`cloudFiles`) streams every CSV dropped into `raw_landing/`
into `bronze_transactions`, unfiltered and append-only, tagged with
`source_file`, `file_arrival_time`, and `ingestion_time`. Checkpointed, so
a second run only reads files it hasn't seen — this is the core value
proposition, and it's directly tested (Test 1, below).

### Layer 2 — Silver (validated, deduplicated)
A `foreachBatch` function applies two rules — `transaction_id` and
`amount` must be non-null, and `amount` must be `> 0` — then
`MERGE INTO`s valid rows into `silver_transactions` (keyed on
`transaction_id`, so late updates and cross-batch duplicates resolve
correctly) and appends invalid rows to `silver_quarantine` with an
explicit `quarantine_reason`. Nothing is ever silently dropped.

### Layer 3 — Gold (business aggregation)
Four SQL-derived Delta tables, refreshed each run:
`gold_transaction_summary` (overall KPIs), `gold_high_value_transactions`
(amount > 5000), `gold_user_metrics` (per-user rollup), and rule-based
anomaly detection — `gold_anomalies_high_value` and
`gold_anomalies_frequency` (threshold computed as `mean + 2×stddev` from
the data itself, not a hardcoded number).

### Layer 4 — Monitoring & Audit
Every stage of every run is wrapped in `run_stage_with_audit(...)`, which
records start/end time, duration, row counts before/after, and status
(`SUCCESS`/`FAILED` with the actual exception message) into
`pipeline_audit_log`. This is the first table to query when diagnosing any
failure — see `docs/troubleshooting.md`.

### Layer 5 — Performance & Security Review (`02_security_and_performance_review.py`)
Run once Silver has a meaningful row count: benchmarks `OPTIMIZE`/`ZORDER`
and runs an explicit grants/PII/credentials review. See
[Section 8](#8-performance--security-review).

---

## 5. Data Model

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

## 6. Bugs Found and Fixed vs. the Reference Notebook

| # | Bug | Fix |
|---|---|---|
| 1 | Metadata columns used bare string literals instead of `col(...)` — would have written the literal text `"_metadata.file_path"` into every row instead of the actual file path | `col("_metadata.file_path")` |
| 2 | `MERGE INTO` assumed the target table already exists | Explicitly create `silver_transactions` and `silver_quarantine` with a defined schema before the first streaming write |
| 3 | Fragile private-API call: `microBatchDF._jdf.sparkSession().sql(...)` | Replaced with standard `spark.sql(...)` |
| 4 | Missing `mergeSchema` on the Bronze writer — a new source column causes `DELTA_METADATA_MISMATCH` even after Auto Loader's reader schema has already evolved | Added `.option("mergeSchema", "true")` |
| 5 | Environment mismatch — reference notebook assumed Azure ADLS (`abfss://`) and legacy Hive Metastore | Adapted all paths to UC Volumes and three-level (`catalog.schema.table`) naming, confirmed live in `00_environment_discovery.py` |

Full debugging narrative — including the monitoring-layer type-inference
bug and the serverless session-state `NameError`s hit during actual
development — is in [`docs/troubleshooting.md`](docs/troubleshooting.md).

---

## 7. Testing — Results

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

**Final verified counts** (small test batch): Bronze = 15, Silver = 11
(valid, unique `transaction_id`s), Quarantine = 2 (1005: negative amount,
1013: malformed amount).

**At realistic bulk volume** (140 synthetic transactions, 30 users, 5
deliberate high-value outliers, 1 deliberate frequency-spike user):
Bronze = 155, Silver = 151, Quarantine = 2. Anomaly detection correctly
isolated all 5 high-value transactions and the 1 frequency-spike user (15
transactions vs. ~5 average) — full numbers in
`Internship_Project_Report.md` Section 19.

---

## 8. Performance & Security Review

Both executed directly against the live workspace, not just described —
see [`02_security_and_performance_review.py`](02_security_and_performance_review.py)
and captured output in [`docs/security_review_output.md`](docs/security_review_output.md).

### Performance — `OPTIMIZE workspace.sentinel.silver_transactions ZORDER BY (user_id)`

| Metric | Before | After | Change |
|---|---|---|---|
| Files | 4 | 1 | -75% |
| Table size | 14,276 bytes | 5,841 bytes | -59% |
| Query latency (`user_id` filter) | 1.609s | 1.044s | -35% |

At 151 rows, the file-count and size reduction are the solid evidence —
mechanically real and directly attributable to `OPTIMIZE`. The latency
improvement is consistent with less overhead but isn't strong proof of the
Z-order data-skipping mechanism at this scale; that payoff compounds at
production volume (thousands of files, gigabytes+), not 151 rows in 1
file. Reported honestly rather than oversold.

### Security

- `SHOW GRANTS ON SCHEMA workspace.sentinel` — reviewed; owner-only access,
  nothing broadened beyond Unity Catalog defaults.
- `DESCRIBE TABLE silver_transactions` — reviewed column-by-column; no PII
  present, only a pseudonymous `user_id` integer (no name/email/phone).
- Manual source read-through — zero hardcoded credentials, API keys, or
  connection strings anywhere in the project; Unity Catalog Volumes grant
  storage access via workspace identity, not embedded secrets.

---

## 9. Project Structure

```
project-sentinel/
├── README.md
├── 00_environment_discovery.py
├── project_sentinel_pipeline.py           # Bronze + Silver + Gold + monitoring + anomaly detection
├── 02_security_and_performance_review.py  # OPTIMIZE/ZORDER benchmark + grants/PII review
├── Internship_Project_Report.md
├── Interview_Prep_and_Final_Audit.md
├── Demo_Script.md
├── Screenshot_Evidence_Plan.md
├── docs/
│   ├── data_dictionary.md
│   ├── testing.md
│   ├── troubleshooting.md
│   └── security_review_output.md
└── screenshots/
```

---

## 10. Setup & How to Run

1. Run `00_environment_discovery.py` first, once per workspace — confirms
   Unity Catalog, Delta Lake, and Auto Loader are actually available.
2. Import `project_sentinel_pipeline.py` into a Databricks workspace with
   Unity Catalog enabled (Workspace → Import).
3. Run the **SETUP** cell first every session — serverless compute detaches
   after inactivity and Python variables/functions don't persist across a
   detach (Delta tables and Volume files do).
4. Drop CSV files into `/Volumes/workspace/sentinel/files/raw_landing/`.
5. Run the pipeline cells in order: Bronze → Silver → Gold → Anomaly Detection.
6. Query `workspace.sentinel.pipeline_audit_log` to see run history.
7. Once Silver has a meaningful row count, run
   `02_security_and_performance_review.py` to benchmark `OPTIMIZE`/`ZORDER`
   and capture the security/grants review.

---

## 11. Limitations

- Demonstration-scale (thousands, not billions, of rows) — see
  [Section 12](#12-future-improvements) for how this would scale.
- Anomaly detection is rule-based by design (no ML unless genuinely
  justified) — the frequency-spike threshold needs a larger, more varied
  dataset to be statistically meaningful.
- Gold layer uses `overwrite` mode (recomputed each run), not incremental
  aggregation — acceptable at this scale, would need incremental
  aggregation (e.g. Delta Live Tables or streaming aggregates) at higher
  volume.

---

## 12. Future Improvements

- **Delta Live Tables (DLT)** for declarative pipeline definition and
  built-in expectations-based data quality.
- **Partitioning** Bronze/Silver by `event_date` once data volume grows.
- **Scheduled Jobs** trigger instead of manual `availableNow` runs.
- **Configurable anomaly thresholds** via a config table instead of
  constants.

---

## 13. Learning Outcomes

- The difference between Auto Loader's reader schema log and a Delta
  table's own schema — both must independently support evolution.
- Serverless compute session lifecycle: Delta tables and checkpoints are
  durable; Python session state is not.
- The practical difference between Bronze (raw fidelity) and Silver
  (business validity) as a data-quality architecture, not just a naming
  convention.

---

*Built as part of a data engineering internship project. See
`Internship_Project_Report.md` for the full write-up and
`Interview_Prep_and_Final_Audit.md` for a Q&A-style deep dive into every
design decision.*
