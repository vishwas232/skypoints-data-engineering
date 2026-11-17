# Phase 6: tests, CI, monitoring, and readiness

Implemented locally: ingestion transaction and monitoring policy tests, offline
CI configuration, read-only health CLI, RAW integrity and aggregate monitoring
SQL, UTC sessions, and a production measurement plan. No live Snowflake,
GitHub Actions, load-test execution, or alert deployment is claimed.

## Local checks and CI

From the project root:

```cmd
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python -m src.ingest_raw --feed members --dry-run
python -m src.transform_redemptions --dry-run --json-file data/samples/redemptions/example.json --as-of 2026-11-18
python -m src.check_pipeline --help
```

`.github/workflows/ci.yml` runs on main pushes, pull requests, or manual dispatch.
It configures Windows/Linux with Python 3.12/3.13: install dependencies, run
`pip check`, offline tests, and sample adapters. It has read-only repository
permissions, no persisted checkout credentials, and no Snowflake secrets/steps.
Official actions: [checkout](https://github.com/actions/checkout) and
[setup-python](https://github.com/actions/setup-python). The other OS/Python
combinations remain unverified until the workflow runs. Production releases
should freeze reviewed dependency sets and pin reviewed action revisions.

| Area | Local evidence | Remaining live evidence |
|---|---|---|
| Ingestion | Commit/replay/rollback and duplicate-batch tests | Actual server rollback and interrupted session |
| Members | Validation, identity, ordering, movement tests/doubles | DDL/DML syntax, country tables and audit |
| Redemptions | Flattening, links, conflicts, transactional in-memory double | Actual VARIANT and lineage checks |
| Lost commit response | Simulated durable commit followed by exception | Connector/network failure recovery |
| Monitoring | Explicit clocks, SLA boundaries, safe-output canaries | Actual timestamps, restricted role and alert routing |
| CI | Configuration plus local Windows/Python run | All four GitHub jobs |
| Scale/recovery/concurrency | Design and measurement plan | Benchmarks, restoration and enforced coordination |

## Read-only warehouse health command

After Phase 2/4/5 DDL and scheduled batch processing, use a dedicated monitoring
user with a SELECT-only default role, configured by the account owner. The
connector uses the configured user's default role; no grants are executed here.

```cmd
python -m src.check_pipeline --source MEMBER:SKYPOINTS_IND --source MEMBER:SKYPOINTS_USA --source MEMBER:SKYPOINTS_AUS --source REDEMPTION:SKYPOINTS_REDEMPTIONS --max-age-hours 26 --max-reject-rate 0.05 --max-transform-lag-hours 2
```

Repeat `--source` for every expected feed namespace. Policy values shown are
examples for owner review: 26-hour freshness, 5% rejection rate, and 2-hour
transformation lag. These are not promised assessment SLAs. Historic unchanged
samples eventually fail freshness; do not raise thresholds simply to get PASS.

The CLI executes shipped zero-row SELECT/CTE checks from SQL 08, 05, and 07,
then checks each source's latest attempted RAW batch and its latest transform
run. Missing, incomplete, future-dated, or stale inputs cannot pass. Completion
must follow RAW completion within policy. Member rows have one reject ratio;
redemption documents/items have independent denominators. Empty valid arrays
still require a document batch and transformation audit. Zero input does not
prove that business delivery is complete: reconcile manifests separately.

Output contains only JSON check names and pass/fail flags, not rows returned
by invariants, payloads, SQL parameters, or connector errors. Exit codes:
0 PASS, 1 FAIL, 2 configuration/query error (argument errors also use 2).
Missing schema or connectivity produces ERROR, never an empty successful report.
Record report/exit status in scheduler logs and route nonzero results through
an approved alert channel. No schedule or messages are created by this task.

Run after serialized writes: SQL 08 treats any unfinished/failed batch, or any
completed RAW batch without a transformation, as not ready regardless of age.
It is an end-of-cycle gate, not an in-flight dashboard. Full-history checks
can be costly and separate queries do not share a point-in-time snapshot.
Production monitoring needs completed manifests, bounded watermarks,
incremental checks, and periodic full reconciliation.

## UTC and remediation

New connector sessions set `TIMEZONE=UTC`, giving consistent TIMESTAMP_NTZ
audit defaults. Historic STARTED_AT/FINISHED_AT/COMPLETED_AT may have used the
account/session timezone: verify their original meaning before enabling gates.
Do not relabel ambiguous values as UTC. Use a reviewed migration or a fresh
isolated verification database if needed. The health code interprets naive
audit values as UTC and converts aware values to UTC.

Ingestion now refuses duplicate BATCH_ID rows instead of silently taking the
first status. Standard-table primary keys do not enforce uniqueness. Failure
rolls back and requires investigation; the adapter never guesses or deletes.

For FAIL, inspect the named SQL securely in Snowsight, correct source/verified
metadata, and reprocess with phase commands. Investigate delivery/scheduler
attempts for missing/stale batches. For ERROR, check objects, role/warehouse,
and secured connector logs. A failed check is not evidence of passing data.

SQL 09 supplies nonzero-row aggregate dashboards, and is deliberately excluded
from zero-row checks. Review historical unresolved quarantine daily: latest
source rejection ratios do not measure old backlog. Re-evaluation can reset
REJECTED_AT, so oldest current rejection is not original first-seen age.
Transaction miles are grouped by status and are not a settled member balance.

## Snowflake acceptance and release

Use an isolated database. Complete warehouse scenarios from `docs/phase4.md`
and `docs/phase5.md`, then SQL 08 (zero rows), SQL 09 (inspect aggregates), and
the health CLI. Original unmapped samples should show quarantine and quality
FAIL. Use explicitly synthetic/verified identities for successful fixtures.
Verify absent sources, stale data, late transforms, and duplicate maps fail.
Verify a real monitoring role cannot mutate data. Interrupt before commit,
close the session, retry, and verify exactly-once visible state.

Serialize ingestion, both transforms, mapping/version administration, and
manual writes through one orchestrator. No database lock has been added;
independent concurrent writers remain unsupported. Current local tests do not
prove warehouse syntax, enforced concurrency, throughput, RPO/RTO, or deployed
alerts. Follow `docs/productionization.md` for bulk SQL and benchmark work.

## Save Phase 6

```cmd
git status --short
git add README.md tests/README.md src/ingest_raw.py src/check_pipeline.py tests/test_ingest_transactions.py tests/test_pipeline_health.py sql/08_verify_raw_integrity.sql sql/09_monitoring_metrics.sql docs/phase6.md docs/productionization.md .github/workflows/ci.yml
git diff --cached --check
git diff --cached --stat
git commit -m "feat: add pipeline CI health checks and production readiness runbooks"
git push origin main
```

Review only these files; leave `.env`, nested archives, and unrelated untracked
files untouched. This task does not commit, push, deploy, or run Snowflake.
