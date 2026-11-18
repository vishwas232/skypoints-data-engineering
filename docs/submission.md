# Reviewer entry point

Use the native implementation (`src.pipeline`, SQL 10–16). The original phase
scripts and SQL 01–09 remain available as earlier compatibility work.

* [PDF requirement-by-requirement matrix](ASSESSMENT_COMPLIANCE.md)
* [Source field mappings, identity and version decisions](SOURCE_CONTRACT.md)
* [Run, recover and demonstrate](NATIVE_RUNBOOK.md)
* [Executed tests, Snowflake and performance evidence](verification.md)
* [Production deployment roadmap and limits](productionization.md)

Run `python -m unittest discover -s tests -v` offline. For live reproduction use
`python -m src.demo --keep-schema --report docs/evidence/my_demo.json` with private
Snowflake development configuration. The command creates a fresh isolated test
schema and exercises actual files, stage uploads, COPY, SQL and recovery. It does
not overwrite the existing legacy SKYPOINTS schema or production objects.

For review, inspect the clean development 5 members / 2 transactions, then the
retained test schema's movement/reject/audit evidence. Negative fixtures are
supposed to fail quality monitoring. Explain declared demo controls and measured
limits before making any production-readiness claim.
