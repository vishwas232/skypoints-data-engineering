# Productionization design and measurement plan

This is a proposed next implementation, not an implemented high-volume pipeline.
Current adapters read histories into Python memory, insert individual rows, and
rebuild country mirrors. No warehouse benchmark has been run by this task.

## Volume assumptions

A hypothetical 1 billion input records/day means about 11,574 records/second
on average, or 115,740/second at a 10x burst. Assuming 1 KB uncompressed per
record gives roughly 1 TB/day before staging, history, and retention overhead.
These are arithmetic assumptions, not measured throughput or payload size.
Clarify whether volume means member updates, documents, or nested transaction
items. Agree latency, late-arrival windows, retention, recovery objectives,
transaction update versions, and a cost budget with the owner before design.

## Proposed data path

1. Require immutable source manifests and atomic file handoff into a controlled
   landing stage. Include namespace, schema version, business update metadata,
   and file hashes. Never process a file still being edited. Convert Excel at
   the boundary instead of sending warehouse row inserts at scale.
2. Bulk-load staged files through `COPY INTO`, with file-ledger reconciliation
   and stable batch/record IDs. Start bulk experiments around 100-250 MB
   compressed files and tune against latency requirements. This starting point
   follows [Snowflake file preparation guidance](https://docs.snowflake.com/en/user-guide/data-load-considerations-prepare).
3. Normalize and validate in set-based SQL with explicit date formats and safe
   conversions. Use [LATERAL FLATTEN](https://docs.snowflake.com/en/sql-reference/functions/flatten)
   for arrays while retaining separate envelope/empty-array audit. Prove SQL
   parity with current Python rules using warehouse fixtures.
4. Join only owner-verified, versioned identities. Refuse ambiguous mappings
   before joins to prevent fanout. An identity merge/split must also migrate
   redemption foreign keys through a reviewed rebuild plan.
5. Deduplicate to one candidate per affected global ID before MERGE. Compare
   source business versions against current winners; maintain affected country
   rows and movement audit in the same DML transaction. Reserve full mirrors
   for repairs. Consider country views if physical tables are no longer required.
6. Parallelize landing/RAW separately from coordinated canonical writes. If
   adopting a Snowflake task graph, evaluate `OVERLAP_POLICY=NO_OVERLAP` at its
   root, and coordinate external/manual writers too. Task policy does not lock
   Python commands. See [CREATE TASK](https://docs.snowflake.com/en/sql-reference/sql/create-task).
7. Require authoritative transaction versions before correcting amounts or
   statuses. A later observation date or arrival time is not business recency.

Select warehouse size and clustering from query profiles: inspect pruning for
batch/source/time and member-key lookups, spill, fanout, skew, and cost.
Country alone is not a universal clustering solution. Isolate workloads where
measurement justifies it, and agree budgets/resource policies with the owner.
No warehouse configuration, tasks, grants, or budgets are deployed here.

## Reliability and access

Use bounded backoff retries only for retryable transport/warehouse failures.
Validation/identity/version problems need remediation, not retry storms. Keep
sanitized scheduler attempt logs with stage, batch/run ID, UTC timestamps,
exit status, query IDs, counts, and error category. Successful transactional
audit cannot retain failed-attempt events. Restrict detailed diagnostic access;
exclude payloads, passwords, member names, and SQL parameters from shared logs.

Separate deployment, runtime writer, and SELECT-only monitoring roles. Move
the demo `.env` authentication to an approved managed service authentication
and secret store before production. Define RAW/quarantine retention and access
policies with the owner. Verify restoration in an isolated environment before
claiming RPO/RTO. Keep real customer data out of CI.

## Benchmark and release gates

Generate approved synthetic datasets at 10k, 1m, and 10m records before scaling
further. Include duplicates, skewed member IDs, unresolved/ambiguous mappings,
nulls, invalid dates, oversized fields/arrays, late records, ties, country moves,
and transaction conflicts. Preserve manifests and exact code/SQL revisions.

For each size, record documents/items/bytes, compression/file sizes, warehouse
configuration, dependency versions, load/transform elapsed time, end-to-end
p50/p95 latency, peak memory/spill, bytes scanned, cost, and reconciled counts.
Exercise process death before commit, lost commit acknowledgement, retries,
restore, and actual prevention of conflicting writers. Set acceptance limits
before observing results. Do not extrapolate unit-test runtime into throughput.

Promote only after SQL parity, actual server rollback/restart behavior, workload
performance, role isolation, alert delivery, and recovery have been measured
and reviewed in an isolated Snowflake environment.
