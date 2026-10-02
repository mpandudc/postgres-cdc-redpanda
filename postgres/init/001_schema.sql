-- Operational OLTP schema captured by Debezium through pgoutput.
CREATE SCHEMA app;

CREATE TABLE app.orders
(
    order_id        bigint PRIMARY KEY,
    account_id      text           NOT NULL,
    symbol          text           NOT NULL,
    side            text           NOT NULL CHECK (side IN ('buy', 'sell')),
    quantity        numeric(38, 8) NOT NULL CHECK (quantity > 0),
    limit_price     numeric(38, 8) NOT NULL CHECK (limit_price > 0),
    filled_quantity numeric(38, 8) NOT NULL DEFAULT 0 CHECK (filled_quantity >= 0),
    status          text           NOT NULL CHECK (status IN ('new', 'partially_filled', 'filled', 'cancelled')),
    updated_at      timestamptz    NOT NULL DEFAULT clock_timestamp()
);

-- FULL so a DELETE event carries the whole old row, not only the key; the
-- sink then writes a complete tombstone row with _is_deleted = 1.
ALTER TABLE app.orders REPLICA IDENTITY FULL;

-- Least-privilege capture user: replication + read on the captured schema.
CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
GRANT USAGE ON SCHEMA app TO debezium;
GRANT SELECT ON ALL TABLES IN SCHEMA app TO debezium;

-- Created up front so the connector never needs CREATE privileges
-- (publication.autocreate.mode = disabled).
CREATE PUBLICATION dbz_orders FOR TABLE app.orders;
