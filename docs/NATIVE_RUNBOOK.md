# Run and demonstrate the native pipeline

Run commands from the repository root in PowerShell. Use an existing development
database and warehouse. `.env.example` lists configuration names; create your
ignored `.env` privately. The connector sets UTC. The CLI sets a query tag,
30-second lock timeout and nondeterministic-MERGE errors. No account/database or
production role changes are made.

## Install and test without Snowflake

```powershell
python -m pip install -r requirements.txt
python -m pip check
python -m unittest discover -s tests -v
python -m src.pipeline --help
```

## Deploy and process the official PDF examples

The metadata below are explicit demo controls; see [source contract](SOURCE_CONTRACT.md).
Replace the processing date when intentionally refreshing derived values.

```powershell
python -m src.pipeline --schema SKYPOINTS_DEV deploy
python -m src.pipeline --schema SKYPOINTS_DEV ingest --file data/samples/members_pdf.txt --feed MEMBER --source PDF_MEMBERS --format PDF --snapshot-ts 2024-01-15T00:00:00 --as-of 2026-11-18 --dob-format DDMMYYYY --identity-scope GLOBAL --identity-authority SKYPOINTS_PDF
```

Copy the returned `batch_id` into the next command. Never substitute a guessed ID.

```powershell
python -m src.pipeline --schema SKYPOINTS_DEV process --batch-id MEMBER_BATCH_ID --as-of 2026-11-18
python -m src.pipeline --schema SKYPOINTS_DEV ingest --file data/samples/redemptions/example.json --feed REDEMPTION --source PDF_REDEMPTIONS --format JSON --snapshot-ts 2024-01-15T00:00:00 --as-of 2026-11-18 --identity-scope GLOBAL --identity-authority SKYPOINTS_PDF
python -m src.pipeline --schema SKYPOINTS_DEV process --batch-id REDEMPTION_BATCH_ID --as-of 2026-11-18
python -m src.pipeline --schema SKYPOINTS_DEV verify
python -m src.pipeline --schema SKYPOINTS_DEV health --source MEMBER:PDF_MEMBERS --source REDEMPTION:PDF_REDEMPTIONS
```

Expected on a clean development schema: five canonical members, one each in
India/USA/Australia/Canada/Philippines, two joined transactions for Elena, zero
quarantine for these valid feeds. AGE 41 and stale true are specific to the stated
as-of/DOB contract. Replaying unchanged commands skips completed checkpoints.
Health freshness is based on actual delivery time, not the 2024 business version;
it will correctly fail when this demonstration is no longer freshly delivered.

## Full isolated acceptance and measured workloads

```powershell
python -m src.demo --keep-schema --report docs/evidence/my_demo.json
python -m src.snowflake_acceptance --run --report docs/evidence/my_acceptance.json
python -m src.benchmark --rows 1000 --report docs/evidence/my_benchmark_1000.json
python -m src.benchmark --rows 10000 --report docs/evidence/my_benchmark_10000.json
```

Every run creates its own fresh `SKYPOINTS_TEST_<12hex>` schema. Cleanup drops
only a test schema newly created by that Pipeline instance; it cannot adopt an
existing test schema. `--keep-schema` retains it for inspection. Inspect the
reported schema, not the older legacy SKYPOINTS objects. Never rename production
objects to make them pass a guard. Prefix restrictions complement database grants;
use a least-privilege development role.

The SQL templates contain `{{ns}}`, intentionally. `Pipeline.script` renders the
validated fully qualified namespace and splits statements, preserving strings and
SQL UDF bodies. Do not paste unresolved templates into a worksheet. For manual
inspection choose the reported development/test schema and execute read-only
queries such as:

```sql
SELECT COUNT(*) FROM MEMBER_CURRENT;
SELECT MEMBER_NAME, COUNTRY_CODE, DOB, AGE, STALE_MEMBER FROM MEMBER_CURRENT;
SELECT MEMBER_NAME, TXN_ID, MILES_REDEEMED, STATUS FROM REDEMPTION_MEMBER_DETAIL;
SELECT * FROM COUNTRY_MOVEMENT_AUDIT ORDER BY RECORDED_AT;
SELECT ERROR_CODES, COUNT(*) FROM REJECTED_RECORD GROUP BY ERROR_CODES;
SELECT INPUT_ROWS, ACCEPTED_ROWS, REJECTED_ROWS, WINNING_ROWS, REPLAY_ROWS FROM PIPELINE_RUN;
SELECT PHASE, STATUS, ERROR_CATEGORY FROM PIPELINE_ATTEMPT ORDER BY RECORDED_AT;
```

See SQL16 for more read-only examples. In a worksheet fully qualify tables or
explicitly select the exact database and schema returned by the CLI.

## Recover failures

1. Inspect the sanitized category/query ID and secured warehouse query history.
   Do not paste connection objects, `.env`, or raw confidential payloads publicly.
2. If upload/preparation failed, fix the contract/file and rerun ingestion. RAW
   and the ledger are atomic. Attempt-specific upload paths prevent stale parts
   from being copied when retries use a different part size. Unreferenced uploaded
   files may remain; implement a retention cleanup policy before production.
3. If transform failed before commit, rerun the same batch/as-of. Business tables,
   rejects and run checkpoint roll back together. Attempt history persists.
4. If the client lost the commit response, reconnect and retry. The durable
   checkpoint decides whether work already committed. A failed client attempt
   alone does not prove that Snowflake rolled back.
5. If records are quarantined, fix an authoritative mapping or source contract;
   never edit current rows to make checks green. Mapping imports are reviewed
   small JSON arrays, for example `[{"source_system":"LOCAL_A","source_member_id":"1","global_member_id":"VERIFIED_123"}]`.
   Import with `python -m src.pipeline --schema SKYPOINTS_DEV map --file mappings.json`.
   Rerun process with the latest allowed as-of date; relevant metadata changes
   invalidate the checkpoint. Use
   `--retry-quarantine` only when an intentional reevaluation is needed.
6. A new processing date cannot precede completed processing. Already-completed
   older checkpoints still replay safely. Schedule later-as-of refreshes explicitly.

No scheduler, notification channel or production alert integration is deployed.
Schedule `health` with the actual expected source list; its nonzero exit signals
failure. Default policy: delivery <=26h old, rejection <=5%, transform lag <=2h.
An invalid/absent/failed latest attempt is investigated, not hidden by old success.

## Diagnose numbered reconciliation errors

`Native reconciliation violations: 16` means statement16 of SQL15 found offending
rows. The pipeline counts offenders in Snowflake; investigate privately with a
read-only rendered query and the batch/query IDs. Never delete a row simply to
hide an invariant failure.

| Statement | Invariant |
|---|---|
| 1 | One ledger row per batch |
| 2 | Unique RAW IDs across both feeds |
| 3 | Unique canonical global member |
| 4 | Unique source-scoped transaction |
| 5 | Unique member-stage RAW observation |
| 6 | Unique redemption document/item stage ID |
| 7 | Unique committed run ID |
| 8 | Audit status and count equations/nonnegative counts |
| 9 | Ledger input count equals RAW count |
| 10 | Latest audit equals current staging totals |
| 11 | Exactly one quarantine entry per rejected stage record, none for valid |
| 12 | Member winner lineage, fact hash, AGE and stale agree |
| 13 | Current member is not older than latest valid staged business version |
| 14 | No member duplicated across country targets |
| 15 | Country union equals canonical members in all columns |
| 16 | Each physical table contains the correct country, including NULL handling |
| 17 | Redemption has canonical member, valid stage lineage and matching full facts |
| 18 | One envelope per staged document and array item count agrees with RAW |
| 19 | Identity map has one non-null global per source-local key |
| 20 | Strict-name-key current members have unique names |
| 21 | No orphan member-stage RAW pointer |
| 22 | Every processed redemption RAW document has a DOCUMENT stage record |
| 23 | Transaction target is not older than latest valid staged version |

Use full `verify` for scheduled reconciliation. During transformation, many checks
are restricted to affected batch keys; a few remain intentionally global. This
cost belongs in production query profiling.

## Commit and push safely

```powershell
git status --short
git diff --check
git diff
git add src sql tests docs README.md
git diff --cached --stat
git commit -m "docs: explain verified native ETL and execution evidence"
git fetch origin main
git log --oneline --left-right HEAD...origin/main
# If remote has advanced, inspect and merge normally before pushing.
git push origin main
git rev-parse HEAD
git ls-remote origin refs/heads/main
```

Stage only reviewed project files. Preserve unrelated changes. Never add `.env`,
credentials, force-push, reset shared history or bypass a failing check.
