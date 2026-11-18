> Historical compatibility runbook. For the current native submission use [NATIVE_RUNBOOK.md](NATIVE_RUNBOOK.md) and [verification.md](verification.md).

# Phase 5: redemption JSON processing and verified member linkage

`src/transform_redemptions.py` transforms completed Phase 3 REDEMPTION batches.
It preserves RAW, validates the envelope and every nested array element, joins
verified current members, and populates the existing `REDEMPTION_TRANSACTION`.
Snowflake DDL is additive in `sql/06_redemption_transform_schema.sql`.
`sql/07_verify_redemption_transform.sql` provides pre-commit and manual checks.

## Input contract and identity

Each RAW document is an object with `member_id`, `feed_date`, and a
`redemptions` array. Each item requires `txn_id`, `txn_date`, `partner`,
`miles_redeemed`, and `status`. Phase 3 already supports one document, an array
of documents, and JSON-lines input. An empty redemptions array is valid.
Malformed envelopes and invalid elements remain auditable, including null
elements and zero-item documents.

Member linkage uses exactly one `MEMBER_IDENTITY_MAP` row for
`(RAW.SOURCE_SYSTEM, payload.member_id)` and exactly one corresponding
`MEMBER_CURRENT` row. Phase 3 currently labels this feed `SKYPOINTS_REDEMPTIONS`.
The source's member ID is not assumed to be global, and it is not matched to
the country workbooks by numeric ID or name. Duplicate mapping rows are
ambiguous even if their global IDs match. Mapped members not yet in canonical
current are held for review and can be reprocessed after Phase 4 resolves them.

The sample's `223457` has no supplied verified link. A data owner must provide
that link; this task does not seed a guessed mapping. A reviewed mapping can
be inserted only after confirming the true canonical identity:

```sql
INSERT INTO SKYPOINTS.MEMBER_IDENTITY_MAP
  (SOURCE_SYSTEM,SOURCE_MEMBER_ID,GLOBAL_MEMBER_ID,MAPPING_METHOD)
SELECT 'SKYPOINTS_REDEMPTIONS','<verified source member id>',
       '<verified existing global id>','OWNER_VERIFIED';
```

Replace placeholders with actual verified facts. This is not a runnable sample
identity assertion. A separate feed namespace must have separate mappings.

## Validation and transaction identity

- Dates accept `YYYYMMDD` (as string or integer) or ISO `YYYY-MM-DD`; impossible,
  missing, or ambiguous dates are rejected. Require transaction date <= feed
  date <= explicit `--as-of` processing date. Store dates as Snowflake DATE.
- IDs may be strings or integer member IDs; booleans, containers, blanks,
  and oversized values fail validation. Transaction IDs, partner, and status
  must be strings. Values that cannot fit staging columns are stored as null
  with a reason code; the full original remains in RAW.
- Miles must be a positive JSON integer fitting NUMBER(18,0). Zero, negatives,
  decimal values, numeric strings, booleans, and overflow are rejected.
  This feed is a redemption fact contract, not a signed refund ledger.
- Supported statuses are PENDING, COMPLETED, CANCELLED, FAILED, normalized to
  uppercase. Extend the explicit contract for other source-supported values.
- Transaction key is SHA-256 of an unambiguous serialized pair
  `(SOURCE_SYSTEM, txn_id)`. This assumes transaction IDs are unique within a
  feed source; ownership changes for the same key are conflicts. Global ID and
  member name are not transaction keys.
- Exact normalized facts can replay within one document, across documents,
  and across batches. Store one transaction while retaining each staging item.
  Its facts are source, transaction ID, verified member, source member ID,
  transaction date, partner, miles, and status. Feed date is observation
  provenance, not a source update version. Identical later observations retain
  the original target's feed date and source-record lineage.
- The feed supplies no reliable transaction version. Conflicting valid facts
  for one key in a batch are all quarantined. A conflict against an existing
  transaction quarantines the new candidate and leaves the target unchanged.
  An invalid sibling does not suppress a valid candidate. PENDING -> COMPLETED
  is also a conflict: status progression cannot be guessed from arrival order.
  An authoritative update-version contract is required before implementing
  transaction corrections or status transitions.

The `REDEMPTION_MEMBER_DETAIL` view joins transaction facts to the current
member name, country, and tier. Country reflects the member's current state,
not a country asserted at transaction time. Inactive members remain linkable;
there is no supplied rule excluding them. No mileage balance is calculated.

## Run and verify

From the project root, first run local checks (no Snowflake required):

```cmd
python -m unittest discover -s tests -v
python -m src.transform_redemptions --dry-run --json-file data/samples/redemptions/example.json --as-of 2026-11-17
```

Dry-run prints only aggregate counts and reason codes: 1 document, 2 items,
2 rejected items, `UNRESOLVED_OR_AMBIGUOUS_IDENTITY`. It deliberately has no
connection, member data, or identity mappings. It does not preview the live
warehouse result, and does not print member payloads.

In Snowsight, select your assessment database, role, and warehouse, then:

1. Run `sql/06_redemption_transform_schema.sql` after the Phase 2/4 DDL. Run DDL
   separately from the transformation transaction. Review the view definition
   if a view of that name already exists; the script replaces that view only.
2. If not already ingested, run Phase 3 ingestion:

   ```cmd
   python -m src.ingest_raw --feed redemptions --json-file data/samples/redemptions/example.json
   ```

3. Find the completed raw batch:

   ```sql
   SELECT BATCH_ID,SOURCE_FILE,INPUT_ROWS FROM SKYPOINTS.INGEST_BATCH
   WHERE FEED_TYPE='REDEMPTION' AND STATUS='COMPLETED';
   ```

4. Run Phase 5 with its actual batch ID:

   ```cmd
   python -m src.transform_redemptions --batch-id ACTUAL_BATCH_ID --as-of 2026-11-17
   ```

   With the original sample and no verified map, expect one rejected DOCUMENT
   stage row, two rejected ITEM stage rows, three quarantine entries, and zero
   target transactions. DOCUMENT errors and ITEM errors have separate counts:
   a valid envelope can still contain invalid or conflicting items.
5. Inspect aggregate results and run **all** of
   `sql/07_verify_redemption_transform.sql`: every statement returns zero rows.

   ```sql
   SELECT RECORD_KIND,VALIDATION_STATUS,COUNT(*)
   FROM SKYPOINTS.STG_REDEMPTION_RECORD GROUP BY RECORD_KIND,VALIDATION_STATUS;
   SELECT * FROM SKYPOINTS.REDEMPTION_TRANSFORM_RUN ORDER BY COMPLETED_AT;
   SELECT COUNT(*) FROM SKYPOINTS.REDEMPTION_TRANSACTION;
   SELECT COUNT(*) FROM SKYPOINTS.REDEMPTION_MEMBER_DETAIL;
   ```

6. Supply a real owner-verified mapping and process the canonical member through
   Phase 4 if necessary. Re-run the affected redemption batch. Changed mapping
   or current-member availability changes the fingerprint, replacing that
   batch's quarantine with newly validated staging and inserting valid facts.
   If the sample truly maps to an existing canonical member, expect 2 new
   transactions, subject to all validations and pre-existing transaction facts.
7. Re-run identical arguments with unchanged input/mapping/member availability:
   expect SKIPPED with unchanged run and transaction counts. In a separate
   fixture database, verify another batch containing the same transactions
   yields zero new transactions and accepted replay items. Change one amount
   or status in a **new** batch and verify quarantine without target changes.
8. In that fixture database, test malformed documents, empty arrays, invalid
   elements, unknown members, duplicate mappings, and a batch interrupted before
   commit. Close the interrupted session so its open transaction rolls back,
   then retry. Confirm there are no partial staging/quarantine/target/audit writes.
   Local tests use an in-memory transaction double: they do not execute or
   validate Snowflake SQL, server rollback, or warehouse concurrency behavior.

## Audit, recovery, and limits

One DOCUMENT staging row is retained for each RAW document (item index -1),
plus an ITEM row for every array position (zero-based). Record IDs hash RAW ID
and index. Quarantine uses the same deterministic record ID and includes kind,
index, and all reason codes, so bad items are distinguishable within a document.

The run fingerprint covers a transformation-version token, batch, processing
date, immutable RAW snapshot, mapping snapshot, and current global-ID
availability. Successful audit distinguishes documents from flattened items:

```text
DOCUMENT_ROWS = ACCEPTED_DOCUMENTS + REJECTED_DOCUMENTS
ITEM_ROWS = ACCEPTED_ITEMS + REJECTED_ITEMS
ACCEPTED_ITEMS = NEW_TRANSACTIONS + REPLAY_ITEMS
```

Replay items include duplicate facts within the current batch and observations
of transactions already loaded. Raw ingestion audit counts remain document
counts; they are not overwritten with flattened transaction counts.

All staging, quarantine replacement, new transaction inserts, audit counts,
and reconciliation checks are one DML transaction. No DDL is executed by the
adapter. Failure rolls back the transaction. If a commit response is lost,
retry the same arguments: the committed fingerprint skips, otherwise processing
restarts. The CLI suppresses connector tracebacks and payloads. Successful
attempts are audited; retain sanitized failed-attempt events in scheduler logs.

Processed RAW is immutable. Editing or removing it, rekeying a previously
accepted identity, or invalidating accepted staging requires a reviewed rebuild,
not a silent overwrite. Pre-existing target rows without Phase 5 lineage are
also refused. Canonical member and transaction tables must not be manually
edited. Direct deletions, refunds, transaction corrections, and versioned
status updates require additional business contracts.

Schedule one writer for the **entire SKYPOINTS schema**, including Phase 4,
Phase 5, ingestion, and metadata administration. This is an operational
single-writer assumption, not an enforced database lock or a guarantee from
Snowflake standard-table primary keys. The bounded adapter reads current
transactions/mappings into memory and inserts rows; it is not a measured
production-volume implementation. Bulk COPY, set-based FLATTEN and MERGE,
incremental lookups, writer coordination, and performance measurement belong
to productionization. No Snowflake execution was performed by this task.

## Save Phase 5 separately

```cmd
git status --short
git add README.md src/transform_redemptions.py sql/06_redemption_transform_schema.sql sql/07_verify_redemption_transform.sql docs/phase5.md tests/test_transform_redemptions.py
git diff --cached --check
git diff --cached --stat
git commit -m "feat: flatten redemption feeds with verified member links and replay protection"
```

Review the staged diff before committing. Leave `.env` and unrelated untracked
files out of the commit. This implementation task does not commit or push.
