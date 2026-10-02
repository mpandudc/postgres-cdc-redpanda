# Verification evidence

Verified on 2026-10-02 (Asia/Jakarta) with `make check` and `make verify`.

- Host: Proxmox LXC 206 `data` (4 vCPU, 8 GB RAM), Docker 29.1.3.
- PostgreSQL 16.15, Debezium 3.6.3.Final, Redpanda v26.2.3, Redpanda Console
  v3.12.0, ClickHouse 26.8.15.10.
- Offline: 8 unit tests passed (workload determinism and invariants,
  connector config contract with the sink schema).

Scenario: 500 rows seeded before the connector existed, then 1,400
transactions (1,857 inserts, 1,782 updates, 440 deletes, some rows updated
twice in one transaction). Kafka Connect was killed with SIGKILL mid-stream
and stayed down 28.5 s while writes continued.

- ClickHouse `cdc.orders FINAL` equalled Postgres row for row (1,417 rows,
  every column including microsecond `updated_at`) 4.6 s after the last write.
- Postgres equalled the workload generator's own model.
- The crash landed before Connect's first offset commit, so Debezium took a
  second snapshot on restart (2 snapshots, 1,659 snapshot events). The first
  run of this test failed here: 135 rows deleted during the gap survived in
  ClickHouse because a snapshot never emits deletes. With the snapshot
  reconciliation view, 129 ghost rows got tombstones and the tables matched.
- 0 records in `cdc.orders_errors`; 4,028 events, all distinct (no duplicate
  deliveries this run; duplicates would collapse on the LSN version).
- Source-to-sink lag p50 4.2 s, p95 6.0 s, dominated by the ClickHouse Kafka
  engine flush interval.
- Redpanda Console (web UI) listed the `orders-cdc` connector through Kafka
  Connect.
- Replication slot `dbz_orders` active, ~0.9 MB behind the WAL head at the end.

Raw numbers: [results/verification.json](results/verification.json).
