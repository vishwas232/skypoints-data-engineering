# Verification evidence — 18 November 2026

This records execution evidence after Phase 6, superseding earlier runbook
statements that warehouse/CI verification had not yet occurred. It is not a
production readiness or throughput claim. No credential values, member payloads,
account identifiers, or database names are included.

## Executed results

| Verification | Result |
|---|---|
| GitHub Actions, commit `02c2f8c` | All four Windows/Linux, Python 3.12/3.13 jobs succeeded |
| Assessment Snowflake connection/schema | Connected; Phase 2/4/5 objects already present |
| Three real member batches | Completed; 9 inputs, 0 accepted, 9 quarantined |
| Real redemption batch | Completed; 1 document, 2 items, 0 accepted items, 2 quarantined items |
| Unchanged real batch replay | All four batches returned SKIPPED |
| Permanent canonical data after verification | 0 members and 0 transactions; no guessed identities inserted |
| Synthetic warehouse acceptance | All seven scenarios passed in unique session-temporary tables |
| Live health report | 71 checks, 62 passed, 9 policy failures; integrity SQL returned zero rows |

CI evidence: [successful GitHub Actions run](https://github.com/vishwas232/skypoints-data-engineering/actions/runs/44150696013).
The workflow exercised the 58-test suite at that commit. Three new isolation
guard tests accompany the acceptance harness and need a new CI run after push.

Assessment staging now has 9 member rows and 3 redemption rows (one DOCUMENT,
two ITEMs). There are 12 quarantine entries: 9 members, one redemption
envelope, and its two items. Quarantine counts deliberately distinguish
documents and items. RAW remains at 9 member rows and 1 redemption document.
Member identity mapping remains empty. All four transformation run audits
were committed; unchanged replay produced no additional runs.

## Actual warehouse success-path tests

`src/snowflake_acceptance.py` executed these scenarios against Snowflake:

1. Load an explicitly synthetic verified member and skip its unchanged replay.
2. Move IND to AUS, remove the old country row, and record exactly one move.
3. Process an older IND record without reversing the current AUS winner.
4. Reject a newer invalid member without replacing valid current state.
5. Link a synthetic redemption through a verified synthetic mapping; skip its
   unchanged batch and deduplicate the same facts from another batch.
6. Quarantine conflicting transaction miles without overwriting the target.
7. Inject an exception after staging/current/country writes but before the run
   audit insert, verify all 19 temporary tables match their pre-run snapshots
   after actual server rollback, then retry successfully.

These runs exercised the existing Python transformations and their actual
Snowflake DML/reconciliation SQL. They did not execute new permanent schema
DDL, simulate session death or network-level commit loss, test parallel
writers, measure production throughput, or verify a restricted monitoring role.

The harness creates 19 uniquely named `CREATE TEMPORARY TABLE ... LIKE ...`
fixtures in its own session. A table allowlist rewrites the transformations'
fully qualified table references to those fixture names. Unknown references
are refused. It does not shadow permanent names, drop permanent objects, or
add synthetic data to permanent assessment tables. Session close removes the
fixtures, including on failure. See [Snowflake temporary-table behavior](https://docs.snowflake.com/en/user-guide/tables-temp-transient).
Permanent canonical/quarantine counts were checked from a separate session.

To reproduce, first install requirements and run existing schema migrations in
your selected assessment database. Then from the repository root:

```cmd
python -m unittest discover -s tests -v
python -m src.snowflake_acceptance --run --as-of 2026-11-18
```

The explicit `--run` flag opts into warehouse activity. Credentials are loaded
privately as in previous phases. The harness prints scenario names/results,
never payloads or credential values. A failed run reports only exception type
and connector error codes. Its transactions affect temporary fixtures only;
the separately executed real-sample transformation results above persist in
the assessment staging/quarantine/audit tables.

## Remaining blockers and limits

All three source namespaces' member item rejection-rate checks fail. Both
redemption document and item rejection-rate checks fail. The provided samples
lack owner-verified global identity mappings; member snapshots also lack
trusted source-update metadata. AUS also includes an invalid enrollment date.
Provide verified facts and corrected source data rather than inventing links.
Real successful sample linkage cannot be claimed until that happens.

All four source transformation timing checks also fail under the example
two-hour policy. Historic RAW timestamps predate the UTC-session change, and
the source batches were transformed later. Verify original timestamp meaning
and elapsed time before interpreting these failures; no old timestamps or
policy thresholds were altered to obtain PASS. This evidence does not show a
healthy end-to-end operational SLA.

Still outstanding: role isolation, scheduler/alert delivery, forced session
termination, network-level uncertain commit recovery, coordinated writers,
restoration/RPO/RTO, and the measured bulk-load benchmarks described in
`docs/productionization.md`. The seven small synthetic scenarios establish
functional warehouse behavior, not billion-row capacity.

## Save verification work

```cmd
git add README.md docs/verification.md src/snowflake_acceptance.py tests/test_acceptance_isolation.py
git diff --cached --check
git diff --cached --stat
git commit -m "test: verify Snowflake acceptance with isolated synthetic fixtures"
git push origin main
```

Review the staged diff and leave unrelated untracked files untouched. This
verification task has not committed or pushed these new files.
