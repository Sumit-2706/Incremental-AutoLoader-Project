# SQL Dashboard Setup

A business-facing dashboard over the Gold layer, built with Databricks'
built-in SQL Dashboards (no external BI tool required).

## Steps

1. In the left sidebar, go to **SQL Editor**.
2. Run each query below once to confirm it returns data, then click
   **"+ Add visualization"** under the results panel for each one and pick a
   chart type (suggested types noted below).
3. Once all 4 visualizations exist, go to **Dashboards** (left sidebar) →
   **Create Dashboard** → name it `Project Sentinel — Overview` → add each
   visualization to it via **Add widget → From existing visualization**.
4. Arrange the 4 widgets in a 2x2 grid, save, and take a screenshot of the
   full dashboard — this is your strongest "so what" screenshot, since it
   shows business-ready output, not raw tables.

## Widget 1 — Transaction volume & value summary (big number / table)

```sql
SELECT * FROM workspace.sentinel.gold_transaction_summary
```
Suggested visualization: **Counter** (single big number) on `total_amount`,
or a simple table if the summary has multiple rows.

## Widget 2 — Top users by spend (bar chart)

```sql
SELECT user_id, total_spent, transaction_count
FROM workspace.sentinel.gold_user_metrics
ORDER BY total_spent DESC
LIMIT 10
```
Suggested visualization: **Bar chart**, x = `user_id`, y = `total_spent`.

## Widget 3 — High-value transactions over time (line/scatter)

```sql
SELECT transaction_time, amount, user_id
FROM workspace.sentinel.gold_high_value_transactions
ORDER BY transaction_time
```
Suggested visualization: **Scatter plot**, x = `transaction_time`,
y = `amount`.

## Widget 4 — Anomalies flagged (table)

```sql
SELECT transaction_id, user_id, amount, anomaly_type, anomaly_reason
FROM workspace.sentinel.gold_anomalies_high_value
ORDER BY amount DESC
```
Suggested visualization: **Table** (this one is meant to be read, not
charted — the point is showing exactly which transactions were flagged and
why).

## Why this instead of an external BI tool

Power BI or Tableau would need a separate connection, refresh schedule, and
credential setup outside this workspace — extra moving parts for a
demonstration-scale project. Databricks SQL Dashboards query Unity Catalog
directly with no additional connection layer, which keeps the whole system
inside the same governed environment described in Section 14 (Security) of
the internship report.
