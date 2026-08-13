# Data Dictionary — Project Sentinel

All tables live in the `workspace.sentinel` schema, backed by Delta Lake,
in `/Volumes/workspace/sentinel/files/`.

## `bronze_transactions`

Raw, append-only, unfiltered. One row per record ever received, including
invalid ones — this table is never filtered or cleaned, by design.

| Column | Type | Notes |
|---|---|---|
| transaction_id | INT | business key (nullable here — validity is a Silver concern, not Bronze) |
| user_id | INT | |
| amount | DOUBLE | may be null/invalid at this layer |
| transaction_time | TIMESTAMP | event time, can arrive late |
| payment_method | STRING | added via schema evolution — nullable, only populated from the file it first appeared in onward |
| _rescued_data | STRING | Auto Loader's catch-all for values that don't parse into the inferred type (e.g. `"BADVALUE"` in an amount column) |
| source_file | STRING | full path of the file this row came from |
| file_arrival_time | TIMESTAMP | file's modification time, from `_metadata` |
| ingestion_time | TIMESTAMP | when Auto Loader actually processed the row |

## `silver_transactions`

Validated, deduplicated, business-ready. One row per unique `transaction_id`
that passed the quality gate.

| Column | Type | Notes |
|---|---|---|
| transaction_id | INT | primary business key, `MERGE` target key |
| user_id | INT | |
| amount | DOUBLE | guaranteed > 0 and non-null in this table |
| transaction_time | TIMESTAMP | |
| source_file | STRING | carried from Bronze |
| file_arrival_time | TIMESTAMP | carried from Bronze |
| ingestion_time | TIMESTAMP | carried from Bronze |
| processed_time | TIMESTAMP | when the Silver `foreachBatch` processed this row |

## `silver_quarantine`

Invalid records, preserved (never discarded) with an explicit reason.
Append-only.

| Column | Type | Notes |
|---|---|---|
| transaction_id | INT | may be null (that's often the reason it's here) |
| user_id | INT | |
| amount | DOUBLE | may be null or ≤ 0 |
| transaction_time | TIMESTAMP | |
| source_file | STRING | |
| file_arrival_time | TIMESTAMP | |
| ingestion_time | TIMESTAMP | |
| quarantine_reason | STRING | one of: `missing transaction_id`, `missing amount`, `invalid amount: must be > 0`, `unknown` |
| quarantine_time | TIMESTAMP | when it was routed here |

## `gold_transaction_summary`

Single-row overall KPI snapshot, `overwrite` each run.

| Column | Type | Notes |
|---|---|---|
| total_transactions | BIGINT | |
| total_transaction_value | DOUBLE | |
| avg_transaction_amount | DOUBLE | rounded to 2 decimals |
| max_transaction_amount | DOUBLE | |
| min_transaction_amount | DOUBLE | |
| unique_users | BIGINT | |

## `gold_high_value_transactions`

Every Silver transaction above the high-value threshold (currently 5000).

| Column | Type | Notes |
|---|---|---|
| transaction_id | INT | |
| user_id | INT | |
| amount | DOUBLE | |
| transaction_time | TIMESTAMP | |
| source_file | STRING | |

## `gold_user_metrics`

Per-user rollup.

| Column | Type | Notes |
|---|---|---|
| user_id | INT | |
| transaction_count | BIGINT | |
| total_spent | DOUBLE | |
| avg_transaction_amount | DOUBLE | rounded to 2 decimals |

## `gold_anomalies_high_value`

Same rows as `gold_high_value_transactions`, tagged as an anomaly signal.

| Column | Type | Notes |
|---|---|---|
| transaction_id | INT | |
| user_id | INT | |
| amount | DOUBLE | |
| transaction_time | TIMESTAMP | |
| source_file | STRING | |
| anomaly_type | STRING | constant `'HIGH_VALUE'` |
| anomaly_reason | STRING | e.g. `'amount > 5000'` |

## `gold_anomalies_frequency`

Only written when there's enough variance in the data (`stddev > 0`) for the
threshold to mean anything. Absent at very low row counts — this is
intentional, not a bug.

| Column | Type | Notes |
|---|---|---|
| user_id | INT | |
| transaction_count | BIGINT | |
| total_amount | DOUBLE | |
| anomaly_type | STRING | constant `'FREQUENCY_SPIKE'` |
| anomaly_reason | STRING | e.g. `'transaction_count > mean + 2*stddev (10.96)'` |

## `pipeline_audit_log`

One row per pipeline stage per run. Append-only, the first table to query
when diagnosing a failure.

| Column | Type | Notes |
|---|---|---|
| run_id | STRING | UUID, shared across all stages of one pipeline run |
| stage | STRING | `bronze_ingestion`, `silver_transformation`, `gold_layer`, or `anomaly_detection` |
| start_time | TIMESTAMP | |
| end_time | TIMESTAMP | |
| duration_seconds | DOUBLE | |
| row_count_before | BIGINT | nullable — null on a table's first-ever run |
| row_count_after | BIGINT | |
| status | STRING | `SUCCESS` or `FAILED` |
| error_message | STRING | null on success; `"{ExceptionType}: {message[:500]}"` on failure |
