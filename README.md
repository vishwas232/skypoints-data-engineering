# SkyPoints — Data Engineering Assessment

ETL assessment project with profiling, raw ingestion, member transformations,
country movement handling, redemption processing, and operational checks.
See [current execution evidence](docs/verification.md) for successful CI and
Snowflake acceptance tests, sample quarantine results, and remaining blockers.

Start with the [submission and demonstration guide](docs/submission.md) for
the architecture, reviewer navigation, reproduction steps, and open assumptions.

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

## Current implementation status

| Phase | Status | Contents |
|---|---|---|
| 1 — Profile incoming files | Implemented; run locally | `src/profile_inputs.py` and three assessment sample workbooks |
| 2 — Snowflake schema | Schema present during live verification | `sql/01_create_tables.sql`, additive Phase 4/5 migrations |
| 3 — Raw ingestion | Implemented; loaded RAW counts confirmed live | 9 member rows and 1 redemption document observed |
| 4 — Staging / latest-wins / country movements | Implemented; warehouse scenarios verified | Validation, derived fields, canonical member and country updates |
| 5 — Redemptions | Implemented; warehouse scenarios verified | JSON flattening, verified member join, replay/conflict handling |
| 6 — Tests and productionization | Implemented; CI and functional warehouse tests passed | Sample-data prerequisites, timing validation, and scale benchmarks remain open |

## Requirements

- Python 3.13 or compatible Python 3; pip
- Snowflake account for Phase 2 execution
- No `artifact-tool` dependency (the script uses `openpyxl`)

## Phase 1: run source profiling (Windows CMD)

From the project root:

```cmd
python -m pip install -r requirements.txt
python src\profile_inputs.py --data-dir data\samples --output profiling_report.json
```

Expected summary for the supplied nine sample records:

```text
Profiled 9 records; report: profiling_report.json
  IND.xlsx: 3 records, 0 issues
  USA.xlsx: 3 records, 3 issues
  AUS.xlsx: 3 records, 2 issues
Cross-file ID collisions: 6
```

`profiling_report.json` is generated locally and excluded from Git. The source workbook files are provided for this assessment only; do not replace them with confidential customer data.

## Phase 2: run Snowflake schema

1. Sign in to Snowsight and open a worksheet. Choose an appropriate role and warehouse.
2. Select **an existing database** by running `USE DATABASE YOUR_DATABASE_NAME;`, substituting your database name.
3. Run the whole `sql/01_create_tables.sql` file.
4. Run `sql/02_schema_checks.sql` and verify that 14 tables exist in the `SKYPOINTS` schema.
5. The tables should be empty until a future ingestion phase runs.

The scripts do not create a Snowflake account or database, and have **not** been run against your Snowflake instance from this package.

## Folder layout

```text
skypoints-data-engineering/
├── README.md
├── .gitignore
├── requirements.txt
├── data/
│   └── samples/
│       ├── IND.xlsx
│       ├── USA.xlsx
│       └── AUS.xlsx
├── src/
│   └── profile_inputs.py
├── sql/
│   ├── 01_create_tables.sql
│   └── 02_schema_checks.sql
├── docs/
│   ├── decisions.md
│   └── git-guide.md
└── tests/
    └── README.md
```

## Design notes

See `docs/decisions.md` for the member identity, data quality, and source-ordering assumptions. See `docs/git-guide.md` to publish to GitHub.

## AI usage

AI-assisted code and design are reviewed and tested by the submitting engineer. Record actual decisions, tests, and changes in subsequent commits. Do not represent conceptual scalability design as a measured billion-row benchmark.

## Phase 3: ingest data to Snowflake RAW (small assessment demo)

Phase 3 adds `src/ingest_raw.py`: an Excel + JSON adapter that preserves each
source row as a `VARIANT` payload in the raw layer. This is intentionally **raw ingestion only**:
it does not populate `STG_MEMBER_PROFILE`, `MEMBER_CURRENT`, country tables, or
flatten redemptions. Those transformations follow in later phases.

### Windows Command Prompt

From the project root, run:

```cmd
python -m pip install -r requirements.txt
python -m unittest discover -s tests -v
python -m src.ingest_raw --feed members --dry-run
python -m src.ingest_raw --feed redemptions --json-file data/samples/redemptions/example.json --dry-run
```

Copy `.env.example` to `.env` and enter your Snowflake account, username,
password, warehouse and database. Keep `.env` private. After Phase 2 DDL is
already run in that database:

```cmd
python -m src.ingest_raw --feed members
python -m src.ingest_raw --feed redemptions --json-file data/samples/redemptions/example.json
```

Then execute `sql/03_verify_raw_load.sql` in Snowsight. On a fresh environment
you should find 9 member raw rows (3 for each workbook) and one redemption
raw document. Re-running the same unchanged file skips that completed batch.

**Semantics and limits:** Batch IDs are determined by source system, feed type,
and SHA-256 of file bytes; raw record IDs use the batch ID and row number.
A batch writes its records and completion status in one Snowflake transaction;
failures roll back. This is a simple synchronous demonstration suitable for the
sample files, **not** a billion-row loading strategy. Production ingestion would
stage compressed columnar/JSON files and use Snowflake `COPY INTO` (with file
tracking, partitioning, schema contracts, and monitoring). Raw JSON contains a
member ID that is not automatically resolvable to the Excel files' local IDs;
we must not invent a member linkage.

Commit Phase 3 after validating: `git add . && git commit -m "feat: ingest member and redemption feeds into raw Snowflake tables"`.


## Phase 4: transform members and maintain country tables

See [the Phase 4 runbook](docs/phase4.md) for contracts, verified identity and source-version prerequisites, Snowflake verification, recovery, limitations, and Git commands.

Run `sql/04_member_transform_schema.sql` before the transformation CLI:

```cmd
python -m unittest discover -s tests -v
python -m src.transform_members --batch-id ACTUAL_COMPLETED_MEMBER_BATCH_ID --as-of 2026-11-17
```

All queries in `sql/05_verify_member_transform.sql` must return zero rows.

Live sample batches have now been processed and quarantined because they lack
verified global identities and source update times. Synthetic success-path
tests also passed in isolated temporary tables. See `docs/verification.md`.
Do not guess mappings to make the real samples pass.

## Phase 5: redemption processing and member join

See the [Phase 5 runbook](docs/phase5.md) for input contracts, identity mapping,
quarantine, transaction conflicts, audit, recovery, Snowflake verification, and
Git commands. Run `sql/06_redemption_transform_schema.sql` after Phase 2/4 DDL.

```cmd
python -m unittest discover -s tests -v
python -m src.transform_redemptions --dry-run --json-file data/samples/redemptions/example.json --as-of 2026-11-17
python -m src.transform_redemptions --batch-id ACTUAL_COMPLETED_REDEMPTION_BATCH_ID --as-of 2026-11-17
```

All queries in `sql/07_verify_redemption_transform.sql` must return zero rows.
The supplied JSON has two transactions but no verified identity link: both
remain quarantined until a real mapping and canonical member exist. Exact
transaction replays deduplicate; conflicting facts are quarantined because the
feed has no authoritative update-version field. `REDEMPTION_MEMBER_DETAIL`
joins loaded transactions to current member attributes. Run one writer for the
entire schema. Phase 5 has now passed isolated Snowflake acceptance scenarios;
the actual sample remains unlinked. See `docs/verification.md` for evidence.

## Phase 6: CI, monitoring, and production readiness

See the [Phase 6 runbook](docs/phase6.md) for validation, monitoring policies,
UTC prerequisites, recovery, and Git commands. The
[productionization plan](docs/productionization.md) separates this bounded
assessment implementation from proposed bulk loading and benchmarks.

```cmd
python -m unittest discover -s tests -v
python -m src.check_pipeline --help
```

`.github/workflows/ci.yml` defines offline Windows/Linux checks for Python
3.12/3.13. SQL 08 adds zero-row readiness checks; SQL 09 provides aggregate
dashboards. `src/check_pipeline.py` checks configured source delivery,
integrity, rejection rates, and transform lag with JSON and nonzero failures.
New Snowflake sessions use UTC; review historic audit timezone assumptions
before freshness checks. GitHub CI and functional Snowflake scenarios have now
passed; live health reports rejection/timing failures. Alert deployment and
production-scale performance remain unverified. See `docs/verification.md`.
