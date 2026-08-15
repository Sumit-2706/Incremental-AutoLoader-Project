# Data Dictionary

Schema for `workspace.sentinel.silver_transactions` (the canonical, cleaned
table). Bronze carries the same business fields plus ingestion metadata;
Gold is derived aggregates, not raw fields, so it isn't repeated here.

## silver_transactions

| Field | Type | Nullable | Notes |
|---|---|---|---|
| `transaction_id` | INT | No | Business key. Deduplicated on this column — a repeat `transaction_id` in a later batch is treated as an update (MERGE), not a new row. |
| `user_id` | INT | No | Pseudonymous identifier. No name/email/phone/address is stored anywhere in the pipeline. |
| `amount` | DOUBLE | No | Must be `> 0` to be considered valid. `<= 0` or non-numeric values are routed to `silver_quarantine` instead. |
| `transaction_time` | TIMESTAMP | No | Business/event time — when the transaction actually happened. Can arrive out of order or late; the pipeline upserts correctly regardless of arrival order. |
| `payment_method` | STRING | Yes | Added later via schema evolution. Optional — rows ingested before this column existed have it as `NULL`. |
| `source_file` | STRING | No | Bronze metadata carried through to Silver. Full path of the file the row came from — the audit trail for "where did this row come from." |
| `file_arrival_time` | TIMESTAMP | No | Bronze metadata — file modification time as seen by Auto Loader. |
| `ingestion_time` | TIMESTAMP | No | Bronze metadata — when the row was written to Bronze. |
| `processed_time` | TIMESTAMP | No | Silver metadata — when the row was written/updated in Silver. |

## silver_quarantine

Same shape as `silver_transactions`, plus:

| Field | Type | Notes |
|---|---|---|
| `quarantine_reason` | STRING | Why the row was rejected, e.g. `"negative amount"`, `"missing amount"`. |
| `_rescued_data` | STRING | Auto Loader's rescued-data column — preserves the original raw value(s) that didn't fit the inferred schema, so nothing is silently dropped. |

## Design notes

- **Business key vs. surrogate key**: `transaction_id` is trusted as the
  business key from the source system rather than generating a surrogate,
  since the source guarantees uniqueness per transaction.
- **Why no PII**: the dataset was deliberately scoped to only
  transaction-level fields. This was verified directly (not assumed) via
  `DESCRIBE TABLE silver_transactions` — see `docs/testing.md`.
- **Type choices**: `amount` is `DOUBLE` rather than `DECIMAL` for
  simplicity at demonstration scale; a production financial pipeline would
  likely use `DECIMAL(18,2)` to avoid floating-point rounding on currency.
