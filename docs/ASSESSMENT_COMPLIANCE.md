# Assessment compliance and review outcome

Source: the supplied `DE_Assessment_file.pdf`, reviewed with its example member
file, JSON and field metadata. The PDF has six deliverables; the project's earlier
phase labels are implementation milestones, not eight independent PDF requirements.

| PDF requirement | Implementation | Verification |
|---|---|---|
| 1. Analyze source structures, transformations and billion-record considerations | `source_contracts.py`, original workbook profiler, SOURCE_CONTRACT, productionization plan | Offline parser/validation tests; actual bounded benchmark reports; no billion-row capacity claim |
| 2. RAW, staging and country DDL | SQL10: two RAWs, typed staging, current, six country targets, identity/audit/control objects | Real fresh-schema deployment and ownership tests; original legacy objects preserved |
| 3. Standardized dates, AGE and stale member >90days | SQL11, MEMBER_DATE/AGE_ON in SQL10; explicit DOB contract | PDF five-row flow,90/91day and leap/null/date/type/length negative tests |
| 4. Latest wins and correct country movement | SQL12 and Pipeline.sync_countries: source business version, conflict rejection, incremental affected-ID targets | Newer move removes USA, late older arrival cannot reverse, equal-version conflict quarantines |
| 5. Flatten JSON and join with members | SQL13/14, REDEMPTION_MEMBER_DETAIL; declared/reviewed identity links | Actual PDF JSON→2 Elena transactions; replay, duplicate IDs, correction policy, missing identity, empty envelope |
| 6. Validate/cleanse, uniqueness and live demo | Reason arrays/quarantine, SQL15 invariants, demo/acceptance and offline tests | Full file-based live suite including server rollback, commit-response loss, mutex contention and restart |
| AI use, incremental work and repository submission | README attribution, actual commits, CI and documentation | Git history and remote CI; no invented development timeline |

## Defects addressed during completion

| Earlier issue | Correction | Regression evidence |
|---|---|---|
| Official H/D pipe format absent from primary ingestion | Strict header/detail adapter and official five-row transcription | Source contract tests plus native file-to-country demo |
| PDF aliases/compact dates/A-I/PHIL not supported consistently | Explicit aliases, field-format dates, canonical flags/countries | Offline canonicalization and actual SQL assertions |
| Source could change between hashing and parsing | Immutable streamed snapshot; legacy fingerprint guard | Snapshot mutation and mixed-hash rollback regressions |
| Completed replay checked after global date guard | Check durable completed run first | Older completed replay after later-as-of test |
| Existing Python path retrieved warehouse state/used row loops and target rewrites | Primary gzip PUT/COPY plus SQL stages and incremental MERGE | Real bulk loads/benchmarks; legacy path explicitly marked compatibility |
| Identity/name/version assumptions could be hidden | Explicit GLOBAL authority or reviewed LOCAL mappings; default name uniqueness; source-version contract | Excel IDs stay held, controlled distinct LOCAL mappings, equal-version conflicts |
| Flat validation could silently coerce Excel values | Field type guards, width and chronology checks | Numeric-name/boolean-flag workbook and broad negative fixtures |
| Optional country incorrectly rejected/NULL comparisons missed movement | NULL/unlisted geography routes to OTHER; null-safe target removal, audit and placement checks | Unknown→USA→unknown movement, unlisted DEU and invalid numeric country fixtures |
| Retry could copy old chunk parts under same batch prefix | UUID physical attempt prefix stored in ledger | Code path/part tests and restart/replay integration |
| Fragile acceptance SQL rewriting/shadow tables | Fresh fully qualified namespaces and owned cleanup guards | Isolation tests plus live deployment/cleanup |
| Empty JSON envelope could vanish in item accounting | Separate DOCUMENT/ITEM stage records | Empty/invalid JSON document and item reconciliation |
| Transaction corrections lacked an explicit business authorization path | Opt-in policy, strictly newer version, no ownership change, change audit | Unauthorized change held; approved pending→completed correction |
| Old successes could hide later failed attempts in monitoring | Latest source/feed/phase attempt check; replay confirmation | Healthy/negative reports and uncertain-commit recovery |
| Historical timing/sample rejects were reported as overall readiness | Separate positive/negative source evidence and unchanged policies | Verification report explains old failures and current results |
| No measured native workload | Reproducible1k/10k driver and code-hash reports | Executed benchmark JSON, with explicit measurement scope |

## Explicit boundaries

Unresolved Excel identity mappings and authoritative real-feed update metadata
are missing business inputs. The demo's declared PDF authority, DOB interpretation
and snapshot metadata are visible controls. They must not be silently adopted as
confirmed production facts. Postcode's validated VARCHAR representation is a
documented deviation from the PDF's INT notation to preserve leading zeros.

The submission demonstrates functional requirements in the configured development
environment. It does not claim production SLA/capacity, deployed scheduling/alerts,
least-privilege role provisioning, or billion-row tests. These are the concrete
roadmap in [productionization](productionization.md), not hidden incomplete phases.
See [verification](verification.md) for exact executed counts, timings and schemas.
