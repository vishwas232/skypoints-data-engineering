# Source contracts and mapping decisions

These are explicit demonstration controls where the assessment does not provide
a complete business contract. The primary executable path is `src.pipeline` and
SQL 10–15. Older SQL/CLIs remain available for compatibility.

## Identity, keys and versions

The PDF member file and its JSON example share program member ID `223457`.
The demo explicitly declares GLOBAL authority `SKYPOINTS_PDF`; the canonical
key becomes `SKYPOINTS_PDF:223457`. Declaring this authority is a business input,
not an algorithm for recognizing people. The three Excel files reuse local IDs
across IND, USA and AUS. They use LOCAL contracts. A reviewed mapping from
`(source_system, source_member_id)` to global member ID is required. Names,
DOBs and matching integers never establish identity. Those original Excel rows
remain quarantined when the mapping is absent. Existing mappings cannot be rekeyed
through the CLI; a real identity correction requires a separately reviewed migration.

Member Name is mandatory and by default checked as a case-insensitive key across
distinct canonical identities, matching the PDF's stated key requirement. Names
are never used to merge people. Real membership systems usually permit shared
names; `--allow-duplicate-names` is an explicit contract deviation, not the default.

The PDF has no update timestamp. The demo supplies a business snapshot timestamp
`2024-01-15T00:00:00`, separately from ingestion time. Production needs an approved
feed manifest or authoritative row version. Never infer this from upload time,
flight date, DOB or arrival order. Winner order is business timestamp, then
nonnegative business sequence. Conflicting facts at the same version are
quarantined. A stable raw ID orders equivalent candidates deterministically;
it never legitimizes a conflict. Older arrivals cannot undo newer movements.

Redemption identity is scoped by source plus transaction ID. Exact facts replay
safely. Changed facts require an updates-enabled contract and later trusted
business version. Ownership changes are always refused. A new source namespace
does not establish equivalence with transactions from an old namespace.

## Field mapping

| Source | Canonical field/type | Validation and normalization |
|---|---|---|
| Member_Name / Name | MEMBER_NAME VARCHAR(255) | Mandatory string, trim, max255, default name-key check |
| Member_Id / ID | SOURCE_MEMBER_ID VARCHAR(18) | Mandatory string/integer representation, source-scoped identity |
| Enrollment_Date / EnrollmentDate | ENROLLMENT_DATE DATE | Mandatory calendar date; compact YYYYMMDD or explicit Excel contract |
| Last_Flight_Date / FlightDate | LAST_FLIGHT_DATE DATE | Optional; calendar, chronology and future checks |
| Tier_Code / TierCode | TIER_CODE VARCHAR(5) | Optional, trim, max5 |
| Agent_Name / AgentName | AGENT_NAME VARCHAR(255) | Optional, trim, max255 |
| State | STATE_CODE VARCHAR(5) | Optional, trim, max5 |
| Country | COUNTRY_CODE VARCHAR(5) | Optional; AU→AUS, PHIL/PH→PHL, US→USA, IN→IND; other 2–5 uppercase letters retained; NULL/unlisted→OTHER |
| Post_Code / PostCode | POST_CODE VARCHAR(5) | Optional 1–5 digits, preserve leading zeros |
| DOB | DOB DATE | Optional; explicit DDMMYYYY/MMDDYYYY/YYYYMMDD for compact input |
| Is_Active / IsActive | IS_ACTIVE VARCHAR(1) | Optional A/I; Y/N aliases normalize to A/I; invalid types/flags reject |
| Derived | AGE INTEGER | Completed birthdays on AS_OF_DATE; missing DOB→NULL |
| Derived | STALE_MEMBER BOOLEAN | Days since flight >90; exactly90 false; missing flight→NULL |

The PDF's `03051985` is ambiguous. The demo explicitly chooses DDMMYYYY (3 May
1985); it does not claim the PDF proves this interpretation. Postcode is described
as INT length5 in the assessment; storing validated digits as VARCHAR(5)
deliberately preserves `00123`. The sample omits this optional column. Leap-day
birthdays advance on March1 in non-leap years. Dates are Snowflake DATE values;
display format does not change storage. Sessions use UTC; naive business timestamps
are interpreted as UTC.

Additional explicit quality policies: age must be 0–130, DOB cannot follow
enrollment, a flight cannot predate enrollment, and dates/business versions
cannot exceed the processing date. A populated country must be 2–5 letters;
this validates shape rather than membership in an exhaustive ISO registry.
Redemption miles must be a positive JSON integer fitting 18 digits; statuses are
PENDING/COMPLETED/CANCELLED/FAILED and transaction dates cannot follow feed/as-of.

Blank strings and reserved missing literals become NULL. RAW preserves payload
values. Bad header contracts and malformed JSON syntax fail a file before COPY.
Malformed member details with recognizable headers are retained with source
fields and quarantined. No truncation or invented replacement values are used.

## Processing boundaries

An immutable disk snapshot is hashed while copying; parsing uses those exact
bytes. Deterministic gzip JSON parts are uploaded under an attempt-specific
prefix. COPY and the batch ledger commit together under a Snowflake DML mutex.
Transforms commit staging, rejects, canonical/country changes, reconciliation and
checkpoint atomically. File and transform replay have separate durable keys.
Relevant mapping/member-availability metadata changes invalidate a transform
checkpoint. `--retry-quarantine` explicitly requests another evaluation.
Code edits alone do not invalidate an already completed checkpoint. Use a reviewed
contract/version migration, a later as-of refresh, or deliberate retry flag after
a transformation correction; do not assume replay silently applies new SQL.
The retry flag deliberately creates a new evaluation and should not be used for
routine idempotent replay.
HASH_AGG is a compact metadata change detector, not a collision-proof identity
or a substitute for a production versioned master-data registry.

All native ingest/map/transform writers honor one lock row. Direct worksheet
writes and legacy CLIs do not; do not run them against native tables. Standard
Snowflake PK/UNIQUE declarations are descriptive; logic and reconciliation enforce
uniqueness. AGE/stale are snapshots: later as-of reprocessing refreshes affected
members. No automatic daily refresh scheduler is deployed.

File preparation needs temporary disk. One JSON document is limited to 8 Mi
characters. XLSX streaming retains openpyxl overhead; partitioned flat/JSON feeds
are the preferred large-volume delivery. Warehouse histories stay in Snowflake.
Incremental joins/MERGEs affect batch identities; full scheduled reconciliation
can still scan large tables and must be costed.
