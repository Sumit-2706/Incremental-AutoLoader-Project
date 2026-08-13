# Testing — Project Sentinel

Every test below was executed against the real workspace (Databricks Free
Edition, AWS, Serverless, Unity Catalog) — not simulated or reasoned about
in the abstract. This doc expands README.md Section 7 with methodology and
what to look for in each check.

## Test 1 — Incremental ingestion (the core value proposition)

**Method**: Ingest `transactions_batch1.csv` (5 rows) via `run_bronze_ingestion()`.
Confirm Bronze = 5. Add `transactions_batch2.csv` (3 rows). Run
`run_bronze_ingestion()` again with the *same* checkpoint location.

**Pass condition**: Bronze = 8, and `source_file` shows both filenames
distinctly — proving the second run only read the new file, not a full
re-scan of the landing zone.

**Actual result**: 5 → 8 rows. Confirmed via
`SELECT transaction_id, source_file FROM bronze_transactions ORDER BY transaction_id`.

## Test 2 — Invalid record (negative amount)

**Method**: Include a row with `amount = -50` in an ingested batch.

**Pass condition**: Row appears in `silver_quarantine` with
`quarantine_reason = 'invalid amount: must be > 0'`, never in
`silver_transactions`.

**Actual result**: Quarantine count = 1 after that batch.

## Test 3 — Within-batch duplicate

**Method**: `transactions_batch3.csv` contains `transaction_id 1009` twice
in the same file.

**Pass condition**: Silver has exactly 1 row for `1009` — `dropDuplicates`
collapses in-file duplicates before the `MERGE`.

**Actual result**: Confirmed 1 row for `1009` in Silver.

## Test 4 — Cross-batch duplicate / update

**Method**: The same batch also re-sends `transaction_id 1002`, already
present in Silver from an earlier batch.

**Pass condition**: `MERGE INTO ... WHEN MATCHED THEN UPDATE` means the
existing row is updated in place — Silver count does not increase for this
ID, no second row is created.

**Actual result**: Confirmed exactly 1 row for `1002` after the batch.

## Test 5 & 6 — Schema evolution (two independent layers)

**Method**: `transactions_batch4.csv` introduces a new column,
`payment_method`, not present in any earlier file.

**Layer 1 (reader schema)**: First call to `run_bronze_ingestion()` after
this file lands raises `UnknownFieldException:
[UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_FILE]`. This is Auto Loader logging
the new field to its schema log and requiring an explicit re-run — expected,
not a bug.

**Layer 2 (table schema)**: Simply re-running still fails, with
`DELTA_METADATA_MISMATCH`, because the *target Delta table's* schema hasn't
been told it's allowed to evolve. Fixed by adding
`.option("mergeSchema", "true")` to the writer.

**Pass condition**: After both fixes are in place, the batch succeeds; the
Delta table gains a `payment_method` column; old rows show `NULL` for it,
new rows are populated.

**Actual result**: Bronze count 8 → 13 after the schema-evolution batch
succeeded. `DESCRIBE TABLE` confirmed the new column. This sequence (fail →
understand why → fix → re-verify) is the single most instructive part of
the project for an interview — see `Interview_Prep_and_Final_Audit.md`.

## Test 7 — Late-arriving data

**Method**: `transaction_id 1012` has `transaction_time` older than records
already processed, but arrives (physically) in a later batch.

**Pass condition**: Because the `MERGE` key is `transaction_id` (business
identity), not arrival order or event time, the record still lands
correctly in Silver regardless of its timestamp.

**Actual result**: `1012` present in Silver with `amount = 175.25`.

## Test 8 — Malformed record

**Method**: Same batch as Test 7 includes `transaction_id 1013` with
`amount = "BADVALUE"` — a non-numeric value in a numeric column.

**Pass condition**: Auto Loader's `_rescued_data` column captures the
original unparseable value at the Bronze layer without crashing the stream.
Since `amount` becomes `NULL` after failing to parse, the Silver quality
filter (`amount IS NULL` check) routes it to quarantine with reason
`'missing amount'`.

**Actual result**: `_rescued_data` captured `"BADVALUE"`; `1013` appears in
`silver_quarantine`, not `silver_transactions`. Bronze/Silver/Gold all
completed without crashing.

## Real bugs found *during* testing (not staged)

1. **Monitoring-layer type inference failure**: `spark.createDataFrame`
   couldn't infer a schema from a dict where `error_message` was `None` on
   the success path — `PySparkValueError`. Fixed with an explicit
   `StructType` for the audit log writer. This is arguably more valuable
   evidence of real engineering than a clean first-try run — see
   `Screenshot_Evidence_Plan.md` item 14.
2. **Serverless session-state loss**: multiple `NameError` failures
   (`run_stage_with_audit`, `run_bronze_ingestion`, `uuid` not defined) from
   re-entering the notebook after a compute detach without re-running setup
   cells. Not a pipeline bug — a workflow discipline issue, documented in
   `docs/troubleshooting.md`.

## Final verified counts (as of the last full run)

Bronze = 155, Silver = 151, Quarantine = 2 — see
`Internship_Project_Report.md` Section 19 for the full results narrative,
including the anomaly-detection and `OPTIMIZE`/`ZORDER` numbers.
