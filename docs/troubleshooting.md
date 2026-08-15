# Troubleshooting

Real problems hit while building this pipeline, how each was diagnosed,
and the fix. Kept here so the same debugging doesn't have to happen twice.

## Serverless session-state loss

**Symptom**: `NameError: name 'raw_landing' is not defined` (or similar)
after returning to the notebook following a break.

**Cause**: Databricks serverless compute detaches after a period of
inactivity. Delta tables and files written to the Unity Catalog Volume
persist across a detach — but Python variables and function definitions
in the notebook session do not.

**Fix**: always re-run the **SETUP** cell at the top of
`project_sentinel_pipeline.py` first, every session, before running
anything else. This is called out explicitly in the file's header comment
so it isn't missed.

## Two-layer schema evolution

**Symptom**: a new column (`payment_method`) appeared in a source CSV, and
the pipeline still failed with `DELTA_METADATA_MISMATCH` even after Auto
Loader appeared to detect it.

**Cause**: Auto Loader's reader schema log and the target Delta table's
own schema evolve independently. Auto Loader will throw
`UnknownFieldException` once and require the stream to be restarted to
pick up the new column on the read side — but the Delta *table* schema
still won't accept the new column on write unless the writer explicitly
opts in.

**Fix**: `.option("mergeSchema", "true")` on the Bronze writer, combined
with restarting the stream after the expected `UnknownFieldException`.
Verified by confirming the column appeared with existing rows correctly
backfilled as `NULL`.

## Type inference failure on all-null columns

**Symptom**: `spark.createDataFrame(...)` failed when writing a single-row
audit log record where one field was `None`.

**Cause**: Spark can't infer a column's type from a single row where that
field is `None` — there's no data to infer *from*.

**Fix**: pass an explicit `StructType` schema to `createDataFrame` for the
audit log writer instead of relying on inference.

## Bare string literals instead of `col(...)`

**Symptom**: metadata columns (`source_file`, etc.) contained the literal
text `_metadata.file_path` in every row instead of the actual file path.

**Cause**: `.withColumn("file_name", "_metadata.file_path")` treats the
second argument as a string literal, not a column reference.

**Fix**: `.withColumn("source_file", col("_metadata.file_path"))`.

## `MERGE INTO` on a table that doesn't exist yet

**Symptom**: MERGE failed on first run.

**Cause**: MERGE INTO requires the target table to already exist with a
defined schema — it doesn't create one implicitly.

**Fix**: explicitly `CREATE TABLE IF NOT EXISTS` for both
`silver_transactions` and `silver_quarantine`, with a defined schema,
before the first streaming write.

## Fragile private-API call

**Symptom**: code borrowed from a reference notebook called
`microBatchDF._jdf.sparkSession().sql(...)` — a private/internal API, not
part of the public PySpark contract.

**Fix**: replaced with the standard, public `spark.sql(...)`.

## Environment mismatch vs. reference material

**Symptom**: reference notebook assumed Azure ADLS paths (`abfss://`) and
the legacy Hive Metastore.

**Cause**: this workspace runs on AWS with Unity Catalog enabled — a
different cloud and a different catalog/metastore model.

**Fix**: ran `00_environment_discovery.py` first to confirm the *actual*
environment rather than assuming it, then adapted all paths to Unity
Catalog Volumes and three-level (`catalog.schema.table`) naming
throughout.
