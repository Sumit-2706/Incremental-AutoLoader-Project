# Project Sentinel — Incremental AutoLoader Pipeline
### Internship Project Report

---

## 1. Introduction

Project Sentinel is an incremental data ingestion pipeline built on Databricks
Auto Loader, Delta Lake, and a Medallion Architecture (Bronze/Silver/Gold). It
simulates a real-time financial transaction processing system, modeled
loosely on UPI-style payment flows, and demonstrates the core engineering
concerns of a production ingestion system: incremental file detection,
checkpointed fault-tolerant processing, schema evolution, deduplication,
data quality enforcement, and rule-based anomaly detection.

## 2. Problem Statement

Traditional batch ingestion re-reads and reprocesses an entire dataset on
every run. This wastes compute, scales poorly, and makes it hard to
distinguish "what changed" from "what's always been there." A financial
transaction system in particular cannot afford to silently lose or duplicate
records, or to fail entirely when a single malformed row or an unannounced
schema change arrives.

## 3. Existing Problem

Naive approaches to this problem — full-table batch reprocessing, or
custom-built file-tracking logic — are either wasteful at scale or brittle
and hard to maintain. They typically lack: automatic new-file detection,
built-in fault tolerance via checkpointing, native schema evolution support,
and a clean separation between "raw as received" and "cleaned and validated"
data.

## 4. Proposed Solution

A Medallion Architecture pipeline using Databricks Auto Loader for
checkpointed incremental file detection, writing to Delta Lake tables at
each stage:

- **Bronze**: raw, append-only, unfiltered — a faithful record of what was
  actually received, including metadata (source file, arrival time).
- **Silver**: validated, deduplicated, business-ready records; invalid
  records are quarantined (not discarded) for auditability.
- **Gold**: business-ready aggregates and anomaly signals for reporting.

## 5. Objectives

- Detect and ingest only new files on each run (proven, not assumed)
- Maintain checkpoints for fault-tolerant, resumable processing
- Support schema evolution without manual table redefinition
- Deduplicate transactions both within a batch and across batches
- Quarantine invalid/malformed records rather than silently dropping them
- Produce meaningful business KPIs and simple, explainable anomaly signals
- Log every pipeline run for observability and troubleshooting

## 6. Technologies Used

Databricks Free Edition (AWS, Serverless compute), Apache Spark 4.1.0
(Structured Streaming, Spark Connect), Delta Lake, Databricks Auto Loader
(`cloudFiles`), Unity Catalog (catalog/schema/Volumes), PySpark, SQL.

## 7. System Architecture

```
Synthetic CSV files
        |
Unity Catalog Volume (raw_landing/)
        |
Auto Loader (cloudFiles, checkpointed, schema-evolving)
        |
Bronze Delta Table (raw, append-only)
        |
foreachBatch: quality checks + dedup + MERGE upsert
        |
   +----+----+
   |         |
Silver     Quarantine
   |
SQL aggregation
   |
Gold Delta Tables (summary / high-value / user metrics / anomalies)
   |
Audit Log (every stage, every run: timing, row counts, status)
```

## 8. Data Flow

1. A CSV file lands in the Unity Catalog Volume landing zone.
2. Auto Loader detects it (via directory listing + checkpoint state),
   infers/evolves schema, and streams new rows into Bronze.
3. A `foreachBatch` microbatch job reads new Bronze rows, splits them into
   valid/invalid, writes invalid rows to quarantine, deduplicates valid
   rows, and MERGE-upserts them into Silver by `transaction_id`.
4. SQL aggregation queries recompute Gold summary, high-value, user-metric,
   and anomaly tables from current Silver state.
5. Every stage's execution is logged to `pipeline_audit_log`.

## 9. Implementation

Implemented as a single importable Databricks notebook
(`project_sentinel_pipeline.py`), structured as callable functions per
stage (`run_bronze_ingestion`, `run_silver_transformation`, `run_gold_layer`,
`run_anomaly_detection`), each wrapped by `run_stage_with_audit` for
consistent logging. See the file itself for full commented code.

## 10. Bronze Layer

Auto Loader reads CSV files from the Volume landing zone with
`cloudFiles.schemaEvolutionMode = "addNewColumns"` and
`cloudFiles.inferColumnTypes = "true"`. Ingestion metadata (`source_file`,
`file_arrival_time`, `ingestion_time`) is captured via `_metadata` columns.
Writes use `mergeSchema = "true"` so the Delta table's own schema can
evolve alongside the reader schema. Trigger mode is `availableNow=True` —
processes everything currently available, then stops (no idle cluster cost).

## 11. Silver Layer

A `foreachBatch` function applies data quality rules (transaction_id and
amount must not be null; amount must be > 0), splits the microbatch into
valid/invalid, deduplicates valid rows by `transaction_id`
(`dropDuplicates`), and performs a `MERGE INTO` upsert keyed on
`transaction_id` — this handles both cross-batch duplicate updates and
late-arriving data correctly, since the merge key is business identity, not
arrival order.

## 12. Gold Layer

Three SQL-derived Gold tables: `gold_transaction_summary` (overall totals,
averages, unique users), `gold_high_value_transactions` (amount > threshold),
`gold_user_metrics` (per-user transaction count, total spent, average).
Gold tables use `overwrite` mode — they are a recomputed view of current
Silver state, not an append-only log.

## 13. Data Quality

Completeness (required fields non-null), validity (amount > 0), uniqueness
(deduplication by transaction_id) are enforced in the Silver `foreachBatch`
logic. Invalid records are never silently dropped — they are written to
`silver_quarantine` with an explicit `quarantine_reason`, preserving full
auditability.

## 14. Security

### Credentials
No hardcoded passwords, API keys, tokens, or connection strings exist
anywhere in this project. This is possible because Unity Catalog Volumes
provide storage access natively through workspace identity — unlike the
reference notebook's `abfss://` approach, which would have required an
Azure service principal or storage account key configured separately.

### Access control
Access to `workspace.sentinel` and its tables/volume is governed by Unity
Catalog's standard grant model. Grants were reviewed directly via
`SHOW GRANTS ON SCHEMA workspace.sentinel`, which returned an empty result
— confirming no explicit grants exist beyond the workspace/owner defaults
(i.e. only the schema owner has access; nothing was accidentally opened up
during development). In a real multi-user deployment, the next step would
be scoping read access to Silver/Gold to a specific analytics group and
restricting write access to the pipeline's own service identity only.

### PII
The schema was reviewed column-by-column (`DESCRIBE TABLE
silver_transactions`): `transaction_id`, `user_id`, `amount`,
`transaction_time`, plus ingestion metadata. `user_id` is a pseudonymous
integer with no name, email, phone number, or address anywhere in the
pipeline — the synthetic dataset was deliberately designed this way. A real
production version handling actual customer data would need to additionally
mask or tokenize any genuinely identifying fields (e.g. account numbers,
UPI IDs) and apply Unity Catalog column-level security / row filters
rather than relying on schema design alone.

### Least privilege
This project runs entirely within the `workspace.sentinel` schema, created
specifically for it rather than reusing the shared `default` schema — this
limits the blast radius of any misconfiguration to a self-contained,
clearly-scoped area of the catalog.

## 15. Anomaly Detection

Two explainable, rule-based signals, deliberately not ML-based:
1. **High-value transaction**: amount exceeds a configurable threshold
   (currently 5000).
2. **Frequency spike**: a user's transaction count exceeds
   `mean + 2×stddev` across all users — computed from the data itself each
   run, not a hardcoded number.

## 16. Monitoring

`pipeline_audit_log` records every stage of every run: `run_id`, `stage`,
`start_time`, `end_time`, `duration_seconds`, `row_count_before`,
`row_count_after`, `status`, `error_message`. This is the first table to
query when diagnosing a failed or slow run.

## 17. Error Handling

`run_stage_with_audit` wraps every pipeline stage: on exception, it logs
`status=FAILED` with the error message to the audit table, then re-raises
the exception so failures are never silently swallowed. Auto Loader's
built-in `_rescued_data` column catches malformed records at the parsing
level (e.g. a non-numeric value in a numeric column) without crashing the
stream — these rows flow through to Silver, where the quality filter routes
them to quarantine.

## 18. Testing

Eight test cases were designed and executed against the real workspace
(not simulated): incremental ingestion, invalid record handling,
within-batch and cross-batch deduplication, two-layer schema evolution
(reader schema + table schema), late-arriving data, and a malformed record.
All 8 passed. See README.md Section 7 for the full results table, including
one real bug found and fixed during testing (a monitoring-layer type
inference failure on the success path).

## 19. Results

- Bronze: 155 raw rows ingested across incremental file arrivals (initial
  5-file debugging round + a 140-row realistic synthetic bulk load)
- Silver: 151 valid, deduplicated transactions
- Quarantine: 2 invalid records (negative amount, malformed amount),
  fully preserved and auditable
- Schema evolved once (added `payment_method`) without manual table
  redefinition
- Anomaly detection verified meaningfully at scale: 6 high-value
  transactions flagged (threshold > 5000), 1 frequency-spike user
  correctly isolated (15 transactions vs. ~5 average, threshold computed
  as mean + 2×stddev from the data itself)
- `OPTIMIZE ... ZORDER BY (user_id)` run on Silver: file count reduced
  4 → 1, table size reduced 14,276 → 5,841 bytes (59% reduction),
  confirmed via `DESCRIBE DETAIL` and `DESCRIBE HISTORY`. Query latency on
  a `user_id` filter improved 1.609s → 1.044s (35% faster) — a real but
  modest gain at this row count; the mechanism (fewer files, better data
  skipping via Z-order) is what compounds at production volume, not the
  raw number at 151 rows.
- Security review executed directly, not just described: `SHOW GRANTS ON
  SCHEMA workspace.sentinel` returned zero explicit grants (owner-only
  access via UC defaults — no broadened permissions were ever added), and
  `DESCRIBE TABLE silver_transactions` was walked column-by-column to
  confirm no name/email/phone/address fields exist anywhere in the
  pipeline.
- Zero data loss, zero silent failures across all test scenarios

## 20. Challenges

1. **Serverless session-state loss**: Python variables and functions do
   not survive a compute detach, even though Delta tables and Volume files
   do. This caused several `NameError` failures during testing until a
   standard "run setup first" discipline was adopted.
2. **Two-layer schema evolution**: Auto Loader's reader schema log and the
   target Delta table's own schema evolve independently — a new column
   requires both a stream restart (`UnknownFieldException`) and
   `mergeSchema=true` on the writer.
3. **Type inference on all-null columns**: `spark.createDataFrame` cannot
   infer a type from a single row where a field is `None` — fixed with an
   explicit `StructType` schema for the audit log writer.

## 21. Solutions

Each challenge above was traced to root cause using the actual error
message and workspace state (never patched blindly), then fixed and
re-verified with a fresh test run. See README.md Section 6 for the full
list of fixes made relative to the internship's reference notebook.

## 22. Limitations

Demonstration-scale data (tens of rows, not millions). Anomaly detection's
frequency-spike signal needs more data variance to be statistically
meaningful. Gold layer recomputes from scratch each run rather than
incrementally aggregating — acceptable at this scale.

## 23. Future Scope

Delta Live Tables for declarative pipeline + built-in expectations;
partitioning by event date at higher volume; scheduled Jobs trigger instead
of manual runs; configurable anomaly thresholds via a config table.

## 24. Conclusion

Project Sentinel demonstrates a working, tested, and honestly-documented
incremental ingestion pipeline adapted from a generic internship reference
template to the specific constraints of a real Databricks Free Edition / AWS
/ Unity Catalog workspace. Every capability claimed in this report was
verified through direct execution against real data, including deliberate
failure scenarios, with bugs found and fixed rather than assumed away.
