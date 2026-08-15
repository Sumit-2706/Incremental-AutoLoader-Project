# Testing

All 8 test cases below were run directly against the live pipeline in
`00_environment_discovery.py` / `project_sentinel_pipeline.py`, not just
described. Screenshot references live in `docs/screenshot_plan.md` — this
file is the results record.

| # | Test | Input | Expected | Actual | Status |
|---|---|---|---|---|---|
| 1 | Incremental ingestion | File 1 (5 rows), then File 2 (3 rows) | Total 8, no reprocessing of File 1 | Total 8, `source_file` correctly attributed | PASS |
| 2 | Invalid record (negative amount) | 1 row, `amount = -50` | Routed to quarantine, not Silver | Quarantine count = 1 | PASS |
| 3 | Within-batch duplicate | Same `transaction_id` twice in one file | Collapses to 1 row in Silver | 1 row | PASS |
| 4 | Cross-batch duplicate / update | Same `transaction_id` re-sent in a later file | MERGE updates in place, no new row | 1 row (unchanged count) | PASS |
| 5 | Schema evolution (reader) | New column `payment_method` in a new file | Auto Loader detects, requires restart | `UnknownFieldException`, then succeeded on retry | PASS |
| 6 | Schema evolution (target table) | Same as above | Delta table schema adds column via `mergeSchema` | Column added, old rows `NULL` | PASS |
| 7 | Late-arriving data | `transaction_id` 1012, `transaction_time` older than already-processed rows | Correctly upserted regardless of timestamp | Present in Silver, `amount=175.25` | PASS |
| 8 | Malformed record (bad data type) | `transaction_id` 1013, `amount = "BADVALUE"` | Caught by `_rescued_data` / quarantine, pipeline doesn't crash | `_rescued_data` captured original value; routed to quarantine, reason `"missing amount"`; Bronze/Silver/Gold all completed without crashing | PASS |

**Final verified counts**: Bronze = 15, Silver = 11 (valid, unique
`transaction_id`s), Quarantine = 2 (1005: negative amount, 1013:
malformed amount).

## Performance test

`OPTIMIZE workspace.sentinel.silver_transactions ZORDER BY (user_id)`,
benchmarked before/after via `DESCRIBE DETAIL` and `DESCRIBE HISTORY`:

| Metric | Before | After | Change |
|---|---|---|---|
| Files | 4 | 1 | -75% |
| Table size | 14,276 bytes | 5,841 bytes | -59% |
| Query latency (`user_id` filter) | 1.609s | 1.044s | -35% |

At 151 rows the absolute latency gain is modest — expected at this scale.
The mechanism (fewer files, Z-order data skipping) is what compounds at
production volume, not the raw number here.

## Security review

- `SHOW GRANTS ON SCHEMA workspace.sentinel` → empty result. No explicit
  grants beyond Unity Catalog's owner-only defaults; nothing was
  accidentally broadened during development.
- `DESCRIBE TABLE silver_transactions` → walked column-by-column, confirmed
  no PII (no name/email/phone/address; only a pseudonymous `user_id`).
- Manual code read-through: zero hardcoded credentials or access keys — UC
  Volumes handle storage access natively, no secrets needed in code.

## How to re-run

1. Import both notebooks into a Databricks workspace with Unity Catalog
   enabled.
2. Run `00_environment_discovery.py` once to confirm the environment and
   create `workspace.sentinel`.
3. Run `project_sentinel_pipeline.py`'s SETUP cell, then drop test CSV
   files into `/Volumes/workspace/sentinel/files/raw_landing/` matching
   the scenarios above, running the Bronze → Silver → Gold cells after
   each drop.
