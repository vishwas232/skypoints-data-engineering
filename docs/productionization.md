# Productionization: implemented foundations and remaining deployment work

The native assessment path already uses immutable streamed file preparation,
gzip PUT/COPY, warehouse-side validation/MERGE, affected-ID country synchronization,
atomic checkpoints, writer serialization, quarantine, reconciliation and health.
It is implemented and exercised, not merely a proposed future bulk loader.

## Before serving production

1. Obtain approved identity authority/mappings, DOB interpretation, business
   snapshot/version metadata, name-key policy and transaction-update permissions.
   Missing business facts cannot be solved by guessing from sample data.
2. Define delivery windows, latency percentiles, rejection policy, backlog recovery,
   availability, RPO/RTO and budget. Small functional tests do not establish SLAs.
3. Run representative million/100million/sustained peak tests with realistic widths,
   file distributions, source skew, collisions and correction rates. Measure cost,
   query profiles, partition scans, queueing/spill and end-to-end throughput.
4. Tune file count/size and warehouse resources from measurements. Revisit global
   integrity/name scans. The current mutex intentionally serializes native writers;
   redesign ownership/partitioned concurrency before removing it.
5. Deploy scheduler, expected-source health checks and a real alert destination.
   Configure daily derived-field refresh explicitly. None is deployed by this repo.
6. Provision a least-privilege service role/database isolation, secret management,
   data classification/masking/access policies and private operational logs.
7. Set RAW/stage/audit retention, remove verified expired unreferenced uploads,
   and test recovery/restore and uncertain-commit scenarios under real infrastructure.
8. Decide whether physical country targets remain necessary versus views; add SCD2
   only if historical geography/member attributes are a business requirement.

Actual 1k/10k reports are linked in [verification](verification.md). Python
tracemalloc is not warehouse memory or process RSS. There is no claimed billion-row
capacity, production deployment, alert integration or cost guarantee.
