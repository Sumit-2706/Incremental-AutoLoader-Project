# Troubleshooting — Project Sentinel

Every error below actually happened during development (visible in the
executed notebook) — this isn't a generic "common Spark errors" list, it's
what this specific project hit, in the order it hit them.

---

### `UnknownFieldException: [UNKNOWN_FIELD_EXCEPTION.NEW_FIELDS_IN_FILE]`

**When**: A new CSV file contains a column Auto Loader hasn't seen before
(e.g. `payment_method` appearing for the first time).

**Why**: Auto Loader tracks a schema log per `cloudFiles.schemaLocation`.
The first time it sees an unrecognized column, it logs the change and
intentionally kills the current stream rather than silently guessing.

**Fix**: Just re-run the ingestion cell. The schema log has already been
updated; the retry succeeds against the new schema. The error message
itself says `which can be fixed by an automatic retry: true`.

---

### `DELTA_METADATA_MISMATCH`

**When**: Immediately after the above — Auto Loader now knows about the new
column, but the *target Delta table* doesn't.

**Why**: Two separate schemas are in play: the streaming reader's schema
(managed by the schema log) and the Delta table's actual column set.
Fixing one doesn't fix the other.

**Fix**: Add `.option("mergeSchema", "true")` to the `writeStream` writer.
This tells Delta the table itself is allowed to gain new columns. Existing
rows backfill the new column as `NULL`.

---

### `PySparkValueError` on `spark.createDataFrame([{...}])`

**When**: Building the audit-log row as a plain Python dict and letting
Spark infer the schema, on a run where `error_message` was `None`
(the success path).

**Why**: Spark's type inference from a list of dicts can't always determine
a type when a field's only sample value is `None` — there's nothing to
infer *from*.

**Fix**: Define an explicit `StructType` (see
`02_security_and_performance_review.py` / the pipeline's monitoring layer)
and pass it as the `schema=` argument. Don't rely on inference for any
column that can legitimately be null on some rows.

---

### `NameError: name 'X' is not defined` (recurring)

**When**: Re-entering the notebook after a compute detach, an idle
timeout, or simply running cells out of order.

**Why**: On serverless / detachable compute, all Python-side state —
functions, variables, imports — is lost when the session resets. The
notebook's cell *history* on screen doesn't mean those cells actually ran
in the current session.

**Fix**: Re-run the setup cells (imports, path variables, function
definitions) at the start of every session before touching pipeline logic.
In production this project runs as an orchestrated job (`00` → main
pipeline → `02`) specifically so this can't happen — job runs are always a
single fresh session executing scripts top to bottom, never a human
re-entering an old notebook.

---

### `SyntaxError: invalid syntax` from a stray copy-pasted fragment

**When**: A line like `[bronze_ingestion] status=SUCCESS duration=X.Xs
rows A -> B` — console *output* text — accidentally pasted into a code
cell instead of a markdown cell.

**Why**: Nothing subtle — copy-paste error during interactive
development.

**Fix**: Delete the cell. Included here only because it's a realistic
signal in the evidence trail (see `Screenshot_Evidence_Plan.md` on why
messy iteration is more convincing than a suspiciously clean run).

---

### Frequency-anomaly table not created

**When**: Running anomaly detection against a small or low-variance batch.

**Why**: Not a bug — `run_anomaly_detection()` only writes
`gold_anomalies_frequency` when `STDDEV(cnt) > 0` across users. At very
small row counts, all users can have equal or near-equal transaction
counts, so `stddev` is 0 or the derived threshold isn't meaningfully
crossed.

**Fix**: Nothing to fix. If you need to see this table populated, run
against a larger/more varied batch. This is what
`transactions_bulk_realistic.csv` (140 rows, including a deliberate
15-transaction spike from `user_id 999`) was built for.

---

### General debugging order for this pipeline

1. Query `pipeline_audit_log` first — it tells you which stage failed,
   the row counts going in/out, and the captured error message, without
   re-running anything.
2. Check `silver_quarantine` before assuming Silver's row count is wrong —
   "missing" rows are very often correctly quarantined, not lost.
3. `DESCRIBE HISTORY <table>` on any Delta table shows every operation
   (including `OPTIMIZE`) with timestamps if you need to reconstruct what
   happened and when.
