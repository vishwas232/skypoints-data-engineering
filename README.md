# SkyPoints — Incubyte Data Engineering Assessment

A working Snowflake ETL project: source files → immutable RAW → validated staging
→ current members and country targets → flattened redemptions and member joins.
The primary implementation uses staged gzip files, COPY INTO and set-based SQL
MERGE. Python orchestrates bounded file preparation and warehouse transactions.

Start with the [runbook](docs/NATIVE_RUNBOOK.md),
[requirements matrix](docs/ASSESSMENT_COMPLIANCE.md)
and [executed verification evidence](docs/verification.md).

## Run locally

Use Python 3.12/3.13. From the repository directory:

```powershell
python -m pip install -r requirements.txt
python -m pip check
python -m unittest discover -s tests -v
python -m src.pipeline --help
```

Snowflake credentials belong only in the ignored `.env` file (see `.env.example`).
Use an existing development database and warehouse. Never paste credentials into
SQL, reports, screenshots, Git or shared material.

```powershell
python -m src.pipeline --schema SKYPOINTS_DEV deploy
python -m src.demo --keep-schema --report docs/evidence/my_demo.json
python -m src.pipeline --schema SKYPOINTS_DEV verify
```

The demo creates a fresh `SKYPOINTS_TEST_<12hex>` schema and exercises actual
file uploads, COPY, SQL validation, movement, rollback, concurrency and replay.
Omit `--keep-schema` for automatic cleanup of that newly owned test schema.
Deployment and production-style sample commands are in the runbook. SQL 10–16
are templates executed with a validated, fully qualified namespace.

## What is implemented

* Official pipe-delimited H/D member file, original Excel compatibility inputs,
  streamed JSON documents/arrays/JSONL, immutable source fingerprints and bulk COPY.
* Explicit DOB, identity and business-version contracts; invalid rows quarantined
  with lineage and reason arrays; ambiguous local IDs are never guessed.
* SQL dates, AGE, strictly-more-than-90-day stale flags; deterministic latest wins.
* Incremental canonical MERGE and removal from the previous country table in the
  same transaction. Five named country targets plus an extensible OTHER target.
* JSON LATERAL FLATTEN, member join, transaction deduplication, conflict holds and
  explicitly approved versioned corrections with change audit.
* Separate durable RAW and transform checkpoints, DML writer mutex, rollback,
  attempt history, item/document reconciliation and health policies.
* Offline Windows/Linux CI, real isolated Snowflake acceptance tests and measured
  1,000/10,000-row workloads. These do not establish billion-row capacity or SLAs.

The PDF omits DOB interpretation, identity authority and update-version metadata.
The demo supplies these explicitly and documents them in
[source contracts and mapping decisions](docs/decisions.md). The original Excel
samples remain unresolved until reviewed global mappings are supplied. Healthy
valid-source runs and deliberately unhealthy negative fixtures are both tested;
monitoring thresholds are not relaxed to conceal rejected data.

## Repository map

| Location | Purpose |
|---|---|
| `src/pipeline.py` | Primary safe deployment, bulk ingestion, orchestration, audit and health CLI |
| `src/source_contracts.py` | Immutable snapshots, member/JSON adapters and gzip parts |
| `sql/10_native_schema.sql` | Native DDL, internal stage, date/age functions and joined view |
| SQL 11–15 | Member staging/apply, redemption staging/apply, reconciliation |
| `sql/16_native_demo_queries.sql` | Read-only demonstration queries (render namespace first) |
| `src/demo.py`, `src/benchmark.py` | Real isolated integration and measured workloads |
| `tests/` | Offline regression suite, including legacy compatibility behavior |
| `data/samples/` | Official PDF transcription, supplied XLSX and JSON examples |
| `docs/evidence/` | Aggregate execution reports and query IDs; no credentials |

SQL 01–09, `ingest_raw`, `transform_members`, `transform_redemptions` and the older
phase runbooks document the earlier small-batch implementation. They are retained
for compatibility/history. Use the native CLI above for this submission. The
existing legacy SKYPOINTS schema is preserved.

## Relevant ETL experience


My previous hands-on ETL work includes Python and SQL pipelines that extract
relational data from MySQL, prepare CSV/GZIP files in Amazon S3, and load data
into Amazon Redshift through AWS Lambda and Redshift COPY workflows.

That work has involved:

- Full reloads of master/reference data and incremental extraction of
  transactional data using business scope and date filters.
- Chunked extraction of larger parent/child datasets, compressed file parts,
  staging loads, and source-to-target row-count validation.
- Investigating CSV column mismatches, source/target schema differences,
  Redshift load errors, and Lambda communication failures.
- Checking table relationships and extraction filters against existing
  business-report logic to identify missing or inconsistent data.

These experiences inform this assessment's focus on explicit data contracts,
staging, reconciliation, recoverable batches, and careful handling of ambiguous
data. My earlier warehouse work used Redshift; this repository demonstrates
those ETL principles in Snowflake, with its own tests and execution evidence.

## AI assistance and measured claims

AI assisted implementation, review and documentation. Changes were verified
using offline regression tests and isolated Snowflake execution.
Commit history records actual changes.
No production deployment, alert integration or billion-row benchmark is claimed.
