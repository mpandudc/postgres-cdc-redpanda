# postgres-cdc-redpanda

Change data capture from PostgreSQL 16 into ClickHouse through Debezium and
Redpanda, proven correct under a Kafka Connect crash: after the dust settles
the ClickHouse current-state table equals Postgres row for row.

Part of the homelab data platform (Proxmox LXC 206). Matches the ingestion
layer of the roadmap: Postgres WAL → Debezium → Redpanda → ClickHouse.

```
Postgres 16  app.orders (REPLICA IDENTITY FULL, publication dbz_orders)
   │ pgoutput logical replication slot
   ▼
Debezium 3.6 on Kafka Connect ── ExtractNewRecordState: __op, __lsn,
   │                              __source_ts_ms, __source_snapshot, __deleted
   ▼
Redpanda  topic cdc.app.orders            Redpanda Console (web UI :18088)
   │
   ▼ ClickHouse Kafka engine (cdc.orders_queue)
   ├─ orders_mv           → cdc.orders            ReplacingMergeTree(_lsn, _is_deleted)
   ├─ orders_changelog_mv → cdc.orders_changelog  every event as received
   ├─ orders_errors_mv    → cdc.orders_errors     unparseable records, never block
   └─ orders_snapshot_reconcile (refreshable, 30 s) → tombstones for ghost rows
```

## Correctness model

- **Ordering and replays.** The Postgres LSN is the row version. Replays and
  multiple updates of one row inside a transaction collapse onto the highest
  LSN; deletes are rows with `_is_deleted = 1` that `FINAL` hides.
- **Re-snapshot ghosts.** If Kafka Connect dies before its first offset
  commit, Debezium has no offset on restart and takes a fresh snapshot. A
  snapshot only emits rows that exist, so rows deleted in the gap never get a
  delete event and would live on in the sink forever. The verification run hit
  exactly this. The fix: once a snapshot's `last` record arrives, any live row
  whose version is older than that snapshot's LSN did not exist at snapshot
  time and gets a tombstone (`cdc.orders_snapshot_reconcile`).
- **Bounded WAL.** `max_slot_wal_keep_size=1GB` stops a dead connector from
  filling the Postgres disk.
- **Least privilege.** Debezium connects as a `REPLICATION` role with read-only
  grants; the publication is pre-created so the connector needs no DDL rights.
  The connector JSON carries `${DEBEZIUM_PASSWORD}`, substituted at registration.

## Run

Requires Docker with Compose and Python 3.12 on the host.

```bash
make check       # offline: workload + connector-config unit tests, compose config
make up          # postgres, redpanda, connect, clickhouse, console
make register    # PUT connector config (idempotent), wait for RUNNING
make workload    # 200 random transactions into app.orders
make status      # connector status
make verify      # crash test end to end, writes results/verification.json
make clean
```

| Port | Service |
|---|---|
| 15432 | Postgres (`app` / `app`) |
| 19092 | Redpanda Kafka API (external listener) |
| 18083 | Kafka Connect REST |
| 18088 | **Redpanda Console web UI**: topics, consumer lag, Debezium connector |
| 18124 | ClickHouse HTTP (`cdc` / `cdc`) |

Credentials are lab defaults and overridable through environment variables.

## Observed numbers

The ClickHouse Kafka engine flushes blocks every ~7.5 s by default
(`stream_flush_interval_ms`), which dominates the measured source-to-sink lag
(p50 ≈ 3.5 s). Lower it on the queue table if freshness matters more than
part count.

See [VERIFICATION.md](VERIFICATION.md).

Related: [dbt-clickhouse-marts](https://github.com/mpandudc/dbt-clickhouse-marts) models
the analytical side; [homelab-data-platform](https://github.com/mpandudc/homelab-data-platform)
hosts shared Redpanda and ClickHouse.
