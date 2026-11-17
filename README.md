# SkyPoints — Data Engineering Assessment

ETL assessment project with source profiling (Phase 1) and Snowflake database design (Phase 2). The long-term target is a working file/JSON → Snowflake raw → staging → canonical member → country tables pipeline, with validation, audit, and safe incremental updates.

## Current implementation status

| Phase | Status | Contents |
|---|---|---|
| 1 — Profile incoming files | Implemented; run locally | `src/profile_inputs.py` and three assessment sample workbooks |
| 2 — Snowflake schema | SQL prepared; execute in your own account | `sql/01_create_tables.sql`, `sql/02_schema_checks.sql` |
| 3 — Raw ingestion | Implemented; connect to Snowflake to verify | Excel and JSON ingestion into raw Snowflake tables |
| 4 — Staging / latest-wins / country movements | Not implemented | Validation, derived fields, canonical member and country updates |
| 5 — Redemptions | Not implemented | JSON flattening and member join |
| 6 — Tests and productionization | Not implemented | Automated checks, monitoring, scalability plan |

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
