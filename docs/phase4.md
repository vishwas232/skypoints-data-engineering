# Phase 4: member transformation and country loading

Implemented by `src/transform_members.py`, with additive DDL in
`sql/04_member_transform_schema.sql` and executable invariants in
`sql/05_verify_member_transform.sql`. Python performs the small-batch date and
validation work, then writes Snowflake staging and targets transactionally.
No Snowflake execution is implied by local tests.

## Contracts and decisions

- Identity is `(SOURCE_SYSTEM, SOURCE_MEMBER_ID)`, resolved only through exactly
  one `MEMBER_IDENTITY_MAP` row. Duplicate map rows are ambiguous even when the
  global IDs match. Names, DOBs, and overlapping numeric IDs never establish a
  mapping. Do not insert guessed mappings to make a demo pass.
- `SKYPOINTS_IND`, `SKYPOINTS_USA`, and `SKYPOINTS_AUS` currently identify source
  namespaces, not a person's current residence. `CountryCode` or `Country` in
  the payload overrides the workbook country. If an upstream transfer creates
  a new namespace, verify both local identities against the same global ID.
- Required: local ID, member name, enrollment date, supported country, verified
  identity and source update timestamp. DOB and flight date may be unknown
  (`NULL` or blank); malformed populated dates are rejected. Never invent DOB,
  active status, member type, or flight dates.
- Dates: ISO `YYYY-MM-DD`, or workbook midnight ISO datetimes. USA additionally
  accepts its documented `MMDDYYYY` integers, padded to eight digits (6152022
  becomes June 15, 2022). Ambiguous slash dates and numeric Excel serials are
  rejected. Dates are stored as Snowflake DATE, not formatted text.
- Country aliases: IN/IND, US/USA, AU/AUS, CA/CAN, PH/PHL, GB/GBR, NZ/NZL.
  The first five have dedicated targets; GBR/NZL use `TABLE_OTHER_COUNTRIES`.
  Extend the explicit country contract before accepting other country codes.
- Future dates, DOB after enrollment, flight before enrollment, age outside
  0..130, invalid supplied member types/statuses, and oversized fields fail
  validation. Invalid narrow-column values are null in staging with an error
  code; their full originals remain in RAW.
- Age is completed calendar years on required `--as-of YYYY-MM-DD`. Leap-day
  birthdays advance March 1 in non-leap years. Staleness is strictly **>90**
  calendar days, false at day 90, null when flight date is unknown. Staging
  keeps its validation date; every successful new run refreshes current and
  country age/staleness for its processing date. Processing dates cannot regress.
- Row-level RAW source timestamps/sequences take precedence. For a legacy file
  without timestamps, a data owner can approve a batch-level business snapshot
  time in `MEMBER_BATCH_VERSION`. Never use ingestion time, flight date, or an
  arbitrary timestamp as a substitute. Naive timestamps are UTC by contract;
  aware timestamps normalize to UTC. Missing sequence defaults to zero.
- Newest **valid** row wins globally, ordered descending by source timestamp,
  nonnegative sequence, then lexical SHA-256 raw record ID. The last component
  makes equal business versions deterministic, not evidence of business recency.
  Duplicate local/global updates remain visible in staging; the run counts
  losing valid rows as superseded, not as invalid. Exact duplicate RAW IDs abort.
  Invalid newer rows do not displace valid history. Late older batches cannot
  revert a newer winner. Equal timestamps should carry meaningful source
  sequences whenever the source provides them.

## Snowflake verification

Use an isolated assessment database with a backup of any existing Phase 4
targets. Configure `.env` privately as in Phase 3. Nothing reads or prints its
contents during local tests. Run only one Phase 4 writer for this schema and
prevent simultaneous RAW/mapping/version edits. This implementation relies on
an orchestrator's single-writer policy, not enforced Snowflake primary keys.
Do not run concurrent CLI commands, including different batches.

1. Run `sql/04_member_transform_schema.sql` in Snowsight after Phase 2 DDL.
   DDL is intentionally separate: Snowflake DDL implicitly commits transactions.
2. List completed member batches:

   ```sql
   SELECT BATCH_ID,SOURCE_SYSTEM,INPUT_ROWS FROM SKYPOINTS.INGEST_BATCH
   WHERE FEED_TYPE='MEMBER' AND STATUS='COMPLETED';
   ```

3. First run with no fabricated mappings or versions:

   ```cmd
   python -m src.transform_members --batch-id ACTUAL_BATCH_ID --as-of 2026-11-17
   ```

   Repeat for each batch. With the supplied samples and no approved metadata,
   all nine records are quarantined and no canonical members are created.
   AUS row 3 also has an invalid enrollment date. Inspect securely:

   ```sql
   SELECT SOURCE_SYSTEM,VALIDATION_STATUS,COUNT(*) FROM SKYPOINTS.STG_MEMBER_PROFILE
   GROUP BY SOURCE_SYSTEM,VALIDATION_STATUS;
   SELECT RAW_RECORD_ID,VALIDATION_ERRORS FROM SKYPOINTS.STG_MEMBER_PROFILE
   WHERE VALIDATION_STATUS='REJECTED';
   SELECT * FROM SKYPOINTS.MEMBER_TRANSFORM_RUN ORDER BY COMPLETED_AT;
   ```

4. Once a data owner provides real mappings and source version approvals,
   insert them using reviewed SQL. Templates (replace placeholders, never run
   literally or infer a mapping from the sample names):

   ```sql
   INSERT INTO SKYPOINTS.MEMBER_IDENTITY_MAP
     (SOURCE_SYSTEM,SOURCE_MEMBER_ID,GLOBAL_MEMBER_ID,MAPPING_METHOD)
   SELECT '<verified source>','<local id>','<verified global id>','OWNER_VERIFIED';
   INSERT INTO SKYPOINTS.MEMBER_BATCH_VERSION
     (BATCH_ID,SOURCE_UPDATE_TS,SOURCE_SEQUENCE,APPROVED_BY)
   SELECT '<batch id>','<source snapshot UTC timestamp>'::TIMESTAMP_NTZ,0,'<approver>';
   ```

   Re-run the affected batch. Changed metadata creates a new run fingerprint and
   re-evaluates quarantine. A source correction must arrive as new immutable RAW
   data, not an edit to a previously accepted record.
5. Execute `sql/05_verify_member_transform.sql`: **every query returns zero
   rows**. The CLI executes the same checks before commit. Confirm
   `INPUT_ROWS = ACCEPTED_ROWS + REJECTED_ROWS` and
   `ACCEPTED_ROWS = WINNING_ROWS + SUPERSEDED_ROWS` for each run. Winning means
   that batch's valid rows selected globally at that run, not newly inserted rows.
6. Re-run identical arguments with unchanged metadata: expect `SKIPPED`, no
   changes to staging, quarantine, movement audit, or run counts. A later as-of
   date produces a new run and refreshes derived current fields.
7. In a separate fixture database, submit two valid records for one **verified**
   global ID: first IND, then AU with a later source timestamp and a new raw ID.
   Run each batch. Expect one current AUS row, no India row for that global ID,
   one Australia row, and one IND-to-AUS movement audit row. Re-run, then submit
   an older IND record and verify there is no reversal or second movement.
   Initial country assignment is not logged as movement. Try an invalid newer
   update, timestamp ties with different sequences, and a rejected identity
   later resolved by a verified mapping.
8. Test actual warehouse failure recovery in that fixture environment: interrupt
   a running batch before commit, close the session so its transaction rolls
   back, then retry. Verify unchanged targets after rollback and exactly one
   successful run after retry. Local doubles test rollback calls, not server
   transaction behavior or Snowflake syntax.

## Restart, audit, and operational limits

The run fingerprint covers batch, as-of date, RAW contents, identity mapping
snapshot, and version approval. Each successful run writes staging, one reject
per invalid raw row (with all reason codes), canonical winners, movements,
country mirrors, and reconciliation counts in one DML transaction. Rejected
records do not delete older accepted members. Full country mirror refresh also
removes a member's old country row and includes inactive members. RAW ingestion
audit counts retain their Phase 3 meaning; transformation counts live separately.

On failure, everything rolls back; retry the same command. There is no persisted
STARTED marker to strand a batch. Failures are reported with a sanitized CLI
message; successful run audit is persisted, while failed-attempt logging belongs
in the scheduler's secured logs. Do not expose SQL parameters, payloads, or
connector tracebacks in shared logs. After an uncertain commit response, rerun:
the committed fingerprint skips, or the rolled-back batch processes again.

Do not edit accepted RAW/version data or rekey previously accepted identity
mappings. The adapter rejects such changes. A legitimate identity merge/split
requires a reviewed, backed-up rebuild of staging, current, targets, and audit
with all affected batches, outside this incremental command. Pre-existing
current members must have valid staging provenance. Direct manual target edits
are unsupported. Authoritative source deletions/tombstones are outside Phase 4.

This adapter reads staging history and mapping into memory, inserts rows, and
refreshes all country mirrors. It is appropriate for this assessment, not
billions of rows. Production work requires staged bulk COPY, set-based SQL
transforms, incremental winner and country updates, scheduler serialization,
restricted write roles, immutable metadata versions, operational failure logs,
and measured load tests. Snowflake PK/UNIQUE constraints on standard tables do
not provide writer synchronization or deduplication.

References: [Snowflake transactions](https://docs.snowflake.com/en/sql-reference/transactions),
[constraints](https://docs.snowflake.com/en/sql-reference/constraints).

## Local tests and Git

From the project root:

```cmd
python -m unittest discover -s tests -v
python -m src.transform_members --help
git status --short
git diff -- README.md src/transform_members.py sql/04_member_transform_schema.sql sql/05_verify_member_transform.sql docs/phase4.md tests/test_transform_members.py
git add README.md src/transform_members.py sql/04_member_transform_schema.sql sql/05_verify_member_transform.sql docs/phase4.md tests/test_transform_members.py
git diff --cached --stat
git commit -m "feat: transform member batches with verified identities and country movements"
```

Review the staged diff before committing. Do not stage `.env`, the nested
archived project copy, or unrelated user changes. No commit or push is performed
by the implementation task.
