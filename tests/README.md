# Automated tests

Run `python -m unittest discover -s tests -v` from the project root. No Snowflake
account or `.env` is required. Tests cover samples, ingestion transactions,
date/age/staleness boundaries, identity collisions, deterministic winners,
country movements, redemption validation/replays/conflicts, quarantine
reprocessing, rollback, lost commit response, and monitoring policies/redaction.

Database doubles test Python control flow and bound writes; they do not execute
Snowflake SQL or establish server transactions/concurrency. See `docs/phase6.md`
for remaining warehouse acceptance tests. `.github/workflows/ci.yml` configures
offline Windows/Linux checks for Python 3.12/3.13, with no Snowflake secrets.
A workflow definition is not evidence of a completed GitHub Actions run.
