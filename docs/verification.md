# Executed verification - 18 November 2026

This is observed development/test evidence. It supersedes the older small-batch
submission notes. Credentials, account/database identifiers and connection
objects are excluded. Exact commands are in [the native runbook](NATIVE_RUNBOOK.md).

## Results

| Check | Observed result |
|---|---|
| Offline regression suite | 77 tests PASS; Python 3.13; dependency compatibility PASS |
| GitHub code CI | All 4 Windows/Linux Python 3.12/3.13 jobs succeeded at `4625a38` |
| Native deployment | 21 tables, internal stage, 3 SQL functions and joined view in approved dev/test namespaces |
| Full file-based Snowflake acceptance | 22 scenarios PASS; 851.32s; 2,066 tracked SQL queries |
| Final full reconciliation | All 23 invariants PASS |
| Clean development member feed | 5 RAW / 5 accepted / 5 current; one in each named country |
| Clean development JSON feed | 1 RAW document / 2 accepted items / 2 transactions; both join Elena |
| Clean development quarantine/health | 0 rejects; PASS with unchanged 26h / 5% / 2h policies |
| Retained acceptance schema | `SKYPOINTS_TEST_8F19CA477730`; includes deliberate negative fixtures |
| Rollback/restart | Actual server rollback leaves business-table snapshots unchanged; retry succeeds |
| Commit uncertainty | Actual server commit followed by simulated lost client response; retry confirms checkpoint |
| Writer concurrency | Actual second Snowflake connection times out on held lock and recovers after release |
| Failed-query audit | Actual failed SQL ID equals attempt QUERY_ID; isolated diagnostic schema cleanup verified |
| Legacy schema | Before/after counts unchanged; no guessed identity rows inserted |

[Successful code CI run](https://github.com/vishwas232/skypoints-data-engineering/actions/runs/44176627511). Aggregate machine-readable reports:
[offline](evidence/offline_verification.json), [native acceptance](evidence/native_acceptance.json),
[development](evidence/development_verification.json), [failed-query diagnostic](evidence/diagnostics_verification.json),
[CI](evidence/github_code_ci.json), [1k benchmark](evidence/benchmark_1000.json),
[10k benchmark](evidence/benchmark_10000.json).

The full suite loaded revision `71332f1`; the subsequent `4625a38` change captures failed
SQL IDs and is verified by its dedicated real diagnostic plus 77 offline tests and
CI. Final benchmarks hash all current Python/SQL source files and reject a code
change during a run. Development reconciliation/health was repeated under `4625a38`.
Reports contain query IDs for secured warehouse inspection; they are not a
production monitoring service or complete query-profile/cost analysis.

## What the live suite proved

Official PDF H/D file ingestion, compact/date/DOB contracts and all five named
countries; original JSON flatten/join; unchanged RAW and completed-run replay;
newer-country movement with old removal; late-arrival protection and equal-version
conflicts; required fields, widths, types, invalid/future dates, explicit versions,
postcode `00123`, optional NULL values, 90/91-day stale and leap birthday boundaries;
optional/unlisted country routing and NULL-safe moves/audit; malformed detail
preservation; all original Excel files remain unresolved; numeric-name/boolean
flag rejection; controlled LOCAL collision mappings/reprocessing/rekey refusal;
cross-batch transaction replay; identical/conflicting duplicate IDs in one document;
unauthorized correction hold and approved newer pending-to-completed change audit;
invalid/empty JSON document/item accounting; actual rollback, commit-response
fault, lock contention, later-date replay and all 23 final invariants.

The retained TEST schema has 50 member RAW observations,
8 redemption documents, 13 current members,
2 transaction facts, 37 current quarantine entries
and 27 committed evaluation runs. These are deliberately
mixed fixtures, not the clean development baseline. The broad validation batch
has 6 accepted / 18 rejected rows; retained original Excel inputs contribute 9 rejected
rows. Quarantine includes both DOCUMENT and ITEM records where appropriate.

## Monitoring investigation

* Clean fresh PDF-member/redemption health passes. Source delivery freshness uses
  actual completed ingestion time; 2024 source business version is a different clock.
* Deliberately invalid NEGATIVE_REDEMPTIONS fails exactly its document and item
  rejection-rate checks. This is correct: both rejected proportions exceed 5%.
  Freshness/lag are not suppressed to conceal quality failures.
* Final valid-source health in the mixed test schema also passes. Selecting all
  negative fixture sources would intentionally produce quality failures.
* Historical legacy health had 71 checks / 9 failures: three member source rejection
  rates, redemption document/item rejection rates, and four transform timing checks.
  Required global mappings/source metadata were absent, and AUS has an invalid
  enrollment date. Old RAW timestamps also predate UTC-session normalization and
  batches were transformed later. Their exact original wall-clock meaning needs
  owner review; timestamps and policy thresholds were not rewritten.

The new native run does not retroactively repair those historical business inputs
or claim old SLA compliance. Legacy counts remained RAW 9 members / 1 document,
stage 9 members / 3 redemption records, 12 rejects, 0 current members and 0 transactions.
The original Excel identity problem remains an explicitly missing business input.

## Measured performance

| Rows | Warehouse | Load+transform seconds | Observed rows/s | Python allocator peak bytes | One-row incremental transform seconds |
|---|---|---|---|---|---|
| 1,000 | X-Small | 24.10 | 41.5 | 1,913,400 | 21.91 |
| 10,000 | X-Small | 28.71 | 348.3 | 2,034,066 | 23.14 |

These are actual single synthetic trials after deployment. Load+transform includes
preparation, PUT/COPY and transform/control/reconciliation work. It excludes schema
provisioning, source generation, final cleanup and the separately measured one-row
update. Each benchmark reconciles all rows, preserves current cardinality on the
one-row country move, and runs full invariants. Test schemas are automatically dropped.
Python tracemalloc excludes native connector memory, process RSS and warehouse
memory. Each report has 203 query IDs including provisioning/control work and hashes
of the executed source revision. Timing varies with service/network/warehouse state.

Fixed per-query/control overhead dominates these small workloads; the one-row
transform remains around22-23seconds. No million / 100 million / billion-row workload,
sustained throughput, peak concurrency, cost budget or operational SLA is claimed.
The existing bounded benchmark generator supports larger approved experiments,
but architecture arithmetic is not capacity evidence.

## Remaining operational boundaries

No production data/database was modified. New namespaces are restricted to the
native DEV/TEST conventions; cleanup requires fresh TEST ownership. Guards do not
replace least-privilege database roles. Production scheduling/alerts, daily derived
refresh, retention/stage cleanup, representative peak benchmarking and tested
RPO/RTO remain deployment work. Real owner-verified identity/version/DOB contracts
remain required before publishing ambiguous external feeds. See
[productionization](productionization.md) and [source decisions](SOURCE_CONTRACT.md).
