# SkyPoints — Data Engineering Assessment

ETL assessment project with source profiling (Phase 1) and Snowflake database design (Phase 2). The long-term target is a working file/JSON → Snowflake raw → staging → canonical member → country tables pipeline, with validation, audit, and safe incremental updates.

## Current implementation status

| Phase | Status | Contents |
|---|---|---|
| 1 — Profile incoming files | Implemented; run locally | `src/profile_inputs.py` and three assessment sample workbooks |
| 2 — Snowflake schema | SQL prepared; execute in your own account | `sql/01_create_tables.sql`, `sql/02_schema_checks.sql` |
| 3 — Ingestion and transformation | Not implemented | Excel / pipe-delimited / JSON ingestion into Snowflake |
| 4 — Latest-wins and country movements | Not implemented | Canonical member and country updates |
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
