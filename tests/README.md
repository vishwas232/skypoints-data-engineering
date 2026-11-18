# Verification layers

Run `python -m unittest discover -s tests -v` from the repository root. Offline
regressions cover source adapters, immutable file bytes, legacy compatibility,
member/redemption validation, replay and rollback guards, health policy and
native namespace/isolation/contract safety. CI runs Python3.12/3.13 on Windows/Linux.

Offline tests cannot prove Snowflake SQL compiles or transaction locks work.
`python -m src.demo --report docs/evidence/my_demo.json` performs actual file-based
acceptance in a fresh owned Snowflake test schema and cleans it afterward. Add
`--keep-schema` to inspect it. The compatibility acceptance CLI invokes that same
safe harness. See the runbook for credentials and exact commands.

The benchmark CLI performs real staged loads and incremental transforms with
reconciliation and frozen-code hashes. Its sample measurements are not capacity
or SLA claims. Committed evidence and limitations are in docs/verification.md.
