CREATE DATABASE IF NOT EXISTS cdc;

-- Kafka engine consumer over the Debezium topic. Decimals arrive as strings
-- (decimal.handling.mode=string) and timestamps as ISO-8601 strings; both are
-- parsed in the materialized views so a bad record lands in orders_errors
-- instead of stalling the consumer.
CREATE TABLE cdc.orders_queue
(
    order_id        Int64,
    account_id      String,
    symbol          String,
    side            String,
    quantity        String,
    limit_price     String,
    filled_quantity String,
    status          String,
    updated_at      String,
    __op            String,
    __lsn           Nullable(UInt64),
    __source_ts_ms  Int64,
    __source_snapshot String,
    __deleted       String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'cdc.app.orders',
    kafka_group_name = 'clickhouse-orders',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

-- Current state. The LSN orders every change to a row, so replays after a
-- connector restart collapse onto the same version; deletes become rows with
-- _is_deleted = 1 that FINAL hides.
CREATE TABLE cdc.orders
(
    order_id        Int64,
    account_id      String,
    symbol          LowCardinality(String),
    side            LowCardinality(String),
    quantity        Decimal(38, 8),
    limit_price     Decimal(38, 8),
    filled_quantity Decimal(38, 8),
    status          LowCardinality(String),
    updated_at      DateTime64(6, 'UTC'),
    _op             LowCardinality(String),
    _lsn            UInt64,
    _source_ts      DateTime64(3, 'UTC'),
    _ingested_at    DateTime64(3, 'UTC'),
    _is_deleted     UInt8
)
ENGINE = ReplacingMergeTree(_lsn, _is_deleted)
ORDER BY order_id;

-- Append-only audit of every event as received, including replays.
CREATE TABLE cdc.orders_changelog
(
    order_id     Int64,
    _op          LowCardinality(String),
    _lsn         UInt64,
    _source_ts   DateTime64(3, 'UTC'),
    _ingested_at DateTime64(3, 'UTC'),
    _is_deleted  UInt8,
    _snapshot    LowCardinality(String),
    status       LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (order_id, _lsn);

CREATE TABLE cdc.orders_errors
(
    raw_message String,
    error       String,
    received_at DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY received_at;

CREATE MATERIALIZED VIEW cdc.orders_mv TO cdc.orders AS
SELECT
    order_id,
    account_id,
    symbol,
    side,
    toDecimal128(quantity, 8) AS quantity,
    toDecimal128(limit_price, 8) AS limit_price,
    toDecimal128(filled_quantity, 8) AS filled_quantity,
    status,
    parseDateTime64BestEffort(updated_at, 6, 'UTC') AS updated_at,
    __op AS _op,
    ifNull(__lsn, 0) AS _lsn,
    fromUnixTimestamp64Milli(__source_ts_ms, 'UTC') AS _source_ts,
    now64(3) AS _ingested_at,
    toUInt8(__deleted = 'true') AS _is_deleted
FROM cdc.orders_queue
WHERE length(_error) = 0;

CREATE MATERIALIZED VIEW cdc.orders_changelog_mv TO cdc.orders_changelog AS
SELECT
    order_id,
    __op AS _op,
    ifNull(__lsn, 0) AS _lsn,
    fromUnixTimestamp64Milli(__source_ts_ms, 'UTC') AS _source_ts,
    now64(3) AS _ingested_at,
    toUInt8(__deleted = 'true') AS _is_deleted,
    __source_snapshot AS _snapshot,
    status
FROM cdc.orders_queue
WHERE length(_error) = 0;

-- Snapshot reconciliation. If Kafka Connect dies before its first offset
-- commit, Debezium re-snapshots on restart. A snapshot only emits rows that
-- exist, so rows deleted in the gap would never get a delete event and would
-- live on in the sink. Every row present in a snapshot carries that snapshot's
-- LSN (or a later one if changed since), so once the snapshot's 'last' record
-- has arrived, any live row whose latest version is older than that LSN did
-- not exist at snapshot time: write a tombstone for it.
-- The tombstone version is snapshot_lsn - 1, so if the snapshot row itself is
-- still in flight (changelog MV ran before orders MV) the real row still wins.
-- The snapshot LSN is joined in rather than bound with WITH (...) AS x: a
-- scalar WITH in a refreshable view was observed to write nothing.
CREATE MATERIALIZED VIEW cdc.orders_snapshot_reconcile
REFRESH EVERY 30 SECOND APPEND TO cdc.orders AS
SELECT
    live.order_id AS order_id,
    live.account_id AS account_id,
    live.symbol AS symbol,
    live.side AS side,
    live.quantity AS quantity,
    live.limit_price AS limit_price,
    live.filled_quantity AS filled_quantity,
    live.status AS status,
    live.updated_at AS updated_at,
    'reconcile' AS _op,
    snap.snapshot_lsn - 1 AS _lsn,
    now64(3) AS _source_ts,
    now64(3) AS _ingested_at,
    toUInt8(1) AS _is_deleted
FROM (SELECT * FROM cdc.orders FINAL WHERE _is_deleted = 0) AS live
CROSS JOIN (
    SELECT max(_lsn) AS snapshot_lsn
    FROM cdc.orders_changelog
    WHERE _op = 'r' AND _snapshot = 'last'
) AS snap
-- snapshot_lsn is 0 until a complete snapshot has arrived; guard the UInt64 subtraction.
WHERE snap.snapshot_lsn > 0 AND live._lsn < snap.snapshot_lsn;

CREATE MATERIALIZED VIEW cdc.orders_errors_mv TO cdc.orders_errors AS
SELECT _raw_message AS raw_message, _error AS error, now64(3) AS received_at
FROM cdc.orders_queue
WHERE length(_error) > 0;
