# Screenshot Evidence Plan

For each item: **Screenshot → What it proves → Why the evaluator cares.**
You already captured several of these during our testing — cross-reference
against your chat/notebook history to pull the exact ones.

1. **Environment discovery output** (Spark version, cloud provider, catalog
   list) → Proves you verified the real environment instead of assuming it →
   Shows engineering discipline before writing any code.

2. **Schema + Volume creation** (`SHOW VOLUMES`) → Proves the storage layer
   was deliberately set up, not defaulted to `default` schema → Shows
   organized, intentional design.

3. **Bronze after file 1** (5 rows) → Baseline ingestion works →
   Establishes the "before" state for the incremental proof.

4. **Bronze after file 2** (8 rows, `source_file` column showing both
   filenames) → **This is the single most important screenshot** → Directly
   proves incremental ingestion, not batch reprocessing.

5. **Silver + Quarantine split** (7 valid / 1 quarantined, negative amount) →
   Proves data quality logic runs on real data, not just exists in code →
   Shows "don't silently discard bad data" was actually implemented.

6. **Duplicate test result** (transaction_id 1009 = 1 row, 1002 = 1 row) →
   Proves deduplication works for both within-batch and cross-batch cases →
   A common interview question ("how do you handle duplicates") answered
   with evidence, not just description.

7. **Schema evolution — first failure** (`UnknownFieldException`) →
   Proves you understand Auto Loader's real (non-silent) evolution behavior →
   Shows you didn't just get lucky; you understood *why* it happened.

8. **Schema evolution — after restart** (13 rows, `payment_method` column
   populated for new rows, NULL for old) → Proves the fix worked →
   Completes the schema evolution story end-to-end.

9. **Late-arriving data result** (transaction_id 1012 present in Silver
   despite older `transaction_time`) → Proves MERGE-by-key handles
   out-of-order arrival correctly → Directly answers "how do you handle
   late data?"

10. **Malformed record result** (`_rescued_data` capturing "BADVALUE",
    row routed to quarantine) → Proves the pipeline survives bad data
    without crashing → Directly answers "what happens on a corrupted
    record?"

11. **Gold tables** (`gold_transaction_summary`, `gold_user_metrics`,
    `gold_high_value_transactions`) → Proves business-ready output exists,
    not just raw tables → Evaluators want to see the "so what," not just
    plumbing.

12. **Anomaly detection output** (`gold_anomalies_high_value`) → Proves
    Phase 7 was implemented, and the threshold is explainable/configurable →
    Shows you resisted over-engineering with unnecessary ML.

13. **`pipeline_audit_log` table** (multiple runs, status, durations,
    row counts) → Proves observability was built in, not bolted on →
    Answers "how would you debug a failed production run?"

14. **A caught and fixed error** (e.g. the `CANNOT_DETERMINE_TYPE`
    audit-logger bug, or the session-state `NameError`) → Counterintuitively
    one of your strongest screenshots → Proves real debugging happened;
    evaluators are more impressed by "I found and fixed X" than a
    suspiciously perfect first-try run.

15. **Final full pipeline run** (all stages SUCCESS in one audit log query) →
    The "everything works together" closing shot → Confirms the system is
    coherent end-to-end, not just individually-tested pieces.
