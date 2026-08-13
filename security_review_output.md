# Security Review — Captured Output

> Fill this in with the REAL output from running
> `02_security_and_performance_review.py` Part 2 in your workspace.
> Do not mark the Final Audit Checklist's "Security reviewed" item complete
> until this file has real captured output, not placeholders.

## 1. Credentials check

Manual source read-through of `project_sentinel_pipeline.py`,
`00_environment_discovery.py`, and this script: confirmed no hardcoded
passwords, API keys, tokens, or connection strings anywhere in the project.
Storage access is entirely via Unity Catalog Volume + workspace identity.

- [ ] Confirmed by: _______________ on _______________

## 2. `SHOW GRANTS ON SCHEMA workspace.sentinel`

```
<paste the real output rows here>
```

**Interpretation**: _(who has what grant, and is it more than the pipeline
actually needs? Note anything broader than expected.)_

## 3. Table location / ownership

```
<paste DESCRIBE DETAIL name + location output here>
```

**Interpretation**: _(confirms tables live under the dedicated
`workspace.sentinel` schema/Volume, not scattered elsewhere)_

## 4. PII column review — `DESCRIBE TABLE workspace.sentinel.silver_transactions`

```
<paste the real column list + types here>
```

**Interpretation**: _(cross-check against Section 14 of the Internship
Report — does every column match the "no PII beyond pseudonymous user_id"
claim? If not, fix the report, not this checklist.)_

## Sign-off

- [ ] All four checks above have real captured output (not placeholders)
- [ ] Any discrepancy against the Internship Report's Security section has
      been reconciled
- [ ] Final Audit Checklist "Security reviewed" item can now be checked off
