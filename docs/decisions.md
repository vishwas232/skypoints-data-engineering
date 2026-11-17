# Architecture decisions and unresolved assumptions

1. **Raw fidelity:** Store original source rows as Snowflake `VARIANT` to preserve source formatting and malformed records for diagnosis.
2. **Identity:** The `IND.xlsx`, `USA.xlsx` and `AUS.xlsx` samples reuse member IDs. Do not assume the same ID across files is the same person. A verified global member ID is required to demonstrate a genuine cross-country move.
3. **Keys:** The assessment marks Member Name as a key, but a stable verified Member ID is more appropriate as the canonical business identity. Clarify this assumption.
4. **Dates and country codes:** Input date representations vary and source examples use `AU` and `AUS`. Normalize at staging time; keep raw values unchanged.
5. **Rejects:** Missing mandatory fields, invalid dates and ambiguous IDs must be quarantined or explicitly held for review instead of silently corrected.
6. **Derived values:** Calculate `AGE` relative to a documented processing date. `STALE_MEMBER` is true when flight date is more than 90 days before that date; null when unknown.
7. **Latest wins:** Use a source update timestamp and deterministic source sequence/tie-breaking. Ingestion timestamp alone cannot reliably resolve late-arriving business updates.
8. **Snowflake limitations:** PK/UNIQUE constraints on standard Snowflake tables are not enforced. Enforce member and transaction uniqueness through transformation logic and tests.
9. **Scale:** The assessment requests a design for billions of daily rows. The provided small-file demo does not prove that throughput. Future phases will describe COPY bulk loading, file sizing, parallelism, partition pruning/clustering where appropriate, and test results.
10. **Country target choice:** Five physical country tables match assessment examples; an extensible country-partitioned alternative should be compared in the architecture writeup.
