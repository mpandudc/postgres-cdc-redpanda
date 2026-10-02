"""End-to-end CDC verification: Postgres -> Debezium -> Redpanda -> ClickHouse.

1. Seed rows before the connector exists (initial snapshot path).
2. Register the connector and stream inserts, updates, deletes and
   multi-update transactions.
3. SIGKILL Kafka Connect mid-stream, keep writing while it is down, restart it.
4. Wait until ClickHouse FINAL state equals Postgres row for row, and Postgres
   equals the workload's own model.
"""

from __future__ import annotations

import base64
import csv
import io
import json
import subprocess
import sys
import time
import urllib.request
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from register_connector import register, wait_running  # noqa: E402
from workload import Workload  # noqa: E402

CH_URL = "http://localhost:18124/"
CH_AUTH = "Basic " + base64.b64encode(b"cdc:cdc").decode()
PG_COLUMNS = (
    "order_id, account_id, symbol, side, quantity, limit_price, filled_quantity, status, "
    "(extract(epoch from updated_at) * 1000000)::bigint"
)
CH_COLUMNS = (
    "order_id, account_id, symbol, side, quantity, limit_price, filled_quantity, status, "
    "toUnixTimestamp64Micro(updated_at)"
)


def sh(*args: str, stdin: str | None = None) -> str:
    result = subprocess.run(args, cwd=ROOT, text=True, capture_output=True, input=stdin)
    if result.returncode != 0:
        print(result.stdout[-3000:], result.stderr[-3000:], sep="\n")
        raise SystemExit(f"command failed: {' '.join(args[:6])}")
    return result.stdout


def psql(sql: str) -> None:
    sh("docker", "compose", "exec", "-T", "postgres", "psql", "-U", "app", "-d", "app",
       "-v", "ON_ERROR_STOP=1", "-q", stdin=sql)


def pg_query(sql: str) -> str:
    return sh("docker", "compose", "exec", "-T", "postgres", "psql", "-U", "app", "-d", "app", "-At", "-c", sql)


def ch(query: str) -> str:
    req = urllib.request.Request(CH_URL, data=query.encode(), headers={"Authorization": CH_AUTH}, method="POST")
    with urllib.request.urlopen(req, timeout=30) as response:
        return response.read().decode()


def normalise(text: str) -> list[tuple]:
    return [
        (int(r[0]), r[1], r[2], r[3], Decimal(r[4]), Decimal(r[5]), Decimal(r[6]), r[7], int(r[8]))
        for r in csv.reader(io.StringIO(text))
    ]


def pg_rows() -> list[tuple]:
    return normalise(pg_query(f"COPY (SELECT {PG_COLUMNS} FROM app.orders ORDER BY order_id) TO STDOUT WITH CSV"))


def ch_rows() -> list[tuple]:
    return normalise(
        ch(f"SELECT {CH_COLUMNS} FROM cdc.orders FINAL WHERE _is_deleted = 0 ORDER BY order_id FORMAT CSV")
    )


def expect(condition: bool, message: str) -> None:
    print(("PASS " if condition else "FAIL ") + message, flush=True)
    if not condition:
        raise SystemExit(message)


def wait_converged(timeout: float = 240) -> tuple[float, list[tuple]]:
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        # Run snapshot reconciliation now instead of waiting for its 30 s schedule.
        ch("SYSTEM REFRESH VIEW cdc.orders_snapshot_reconcile")
        ch("SYSTEM WAIT VIEW cdc.orders_snapshot_reconcile")
        source = pg_rows()
        if source == ch_rows():
            return time.monotonic() - started, source
        time.sleep(2)
    source, sink = pg_rows(), ch_rows()
    missing = sorted(set(source) - set(sink))[:3]
    extra = sorted(set(sink) - set(source))[:3]
    raise SystemExit(f"not converged: pg={len(source)} ch={len(sink)} missing={missing} extra={extra}")


def main() -> None:
    evidence: dict = {"started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z")}
    sh("docker", "compose", "down", "--volumes", "--remove-orphans")
    sh("docker", "compose", "up", "-d", "--wait")
    plugin_dir = sh("docker", "compose", "exec", "-T", "connect", "ls", "/kafka/connect/debezium-connector-postgres")
    evidence["versions"] = {
        "postgres": pg_query("show server_version").strip(),
        "clickhouse": ch("select version()").strip(),
        "redpanda": sh("docker", "compose", "exec", "-T", "redpanda", "rpk", "version").strip().splitlines()[0],
        "debezium": next(
            (n.removeprefix("debezium-connector-postgres-").removesuffix(".jar")
             for n in plugin_dir.split() if n.startswith("debezium-connector-postgres-")),
            "unknown",
        ),
    }

    workload = Workload(seed=7)
    psql(workload.seed_rows(500))
    register()
    print("connector RUNNING after snapshot of 500 rows", flush=True)

    for _ in range(6):
        psql(workload.batch(100))

    # Crash: SIGKILL gives the worker no chance to flush offsets, so events
    # since the last 10 s offset commit are re-emitted after restart.
    sh("docker", "compose", "kill", "-s", "SIGKILL", "connect")
    killed_at = time.monotonic()
    for _ in range(4):
        psql(workload.batch(100))  # written while capture is down; WAL held by the slot
    sh("docker", "compose", "start", "connect")
    wait_running()
    evidence["connect_downtime_seconds"] = round(time.monotonic() - killed_at, 1)
    for _ in range(4):
        psql(workload.batch(100))

    converge_seconds, source = wait_converged()
    expect(True, f"ClickHouse FINAL equals Postgres row for row ({len(source)} rows)")
    expect([row[:8] for row in source] == workload.expected_rows(), "Postgres equals the workload model")

    changelog = json.loads(ch(
        "SELECT count() AS events, uniqExact(order_id, _lsn) AS distinct_events, "
        "countIf(_op = 'r') AS snapshot_events, countIf(_is_deleted = 1) AS delete_events, "
        "uniqExactIf(_lsn, _op = 'r') AS snapshots, "
        "quantileExactIf(0.5)(dateDiff('millisecond', _source_ts, _ingested_at), _op != 'r') AS lag_p50_ms, "
        "quantileExactIf(0.95)(dateDiff('millisecond', _source_ts, _ingested_at), _op != 'r') AS lag_p95_ms "
        "FROM cdc.orders_changelog FORMAT JSONEachRow"
    ))
    errors = int(ch("SELECT count() FROM cdc.orders_errors"))
    expect(errors == 0, "no records routed to cdc.orders_errors")
    first_snapshot = int(ch(
        "SELECT count() FROM cdc.orders_changelog WHERE _op = 'r' "
        "AND _lsn = (SELECT min(_lsn) FROM cdc.orders_changelog WHERE _op = 'r')"
    ))
    expect(first_snapshot == 500, "initial snapshot delivered all 500 seeded rows")
    reconciled = int(ch("SELECT count() FROM cdc.orders WHERE _op = 'reconcile'"))
    print(f"deletes: workload={workload.counts['delete']} events={changelog['delete_events']} "
          f"reconcile_tombstones={reconciled} snapshots={changelog['snapshots']}", flush=True)
    # Row-for-row equality above is the correctness proof. Delete events alone
    # cannot add up to the workload's deletes: orders inserted *and* deleted
    # while capture was down are never seen by the sink at all. What must hold
    # is that a re-snapshot, if one happened, was reconciled.
    if int(changelog["snapshots"]) > 1:
        expect(reconciled > 0, "re-snapshot after crash: ghost rows tombstoned by reconciliation")
    else:
        print("offsets survived the crash; no re-snapshot, reconciliation not exercised this run")
    replayed = int(changelog["events"]) - int(changelog["distinct_events"])

    with urllib.request.urlopen("http://localhost:18088/api/kafka-connect/clusters/debezium/connectors",
                                timeout=15) as response:
        console_connectors = json.loads(response.read())
    names = json.dumps(console_connectors)
    expect("orders-cdc" in names, "Redpanda Console web UI sees the orders-cdc connector")

    slot = pg_query("SELECT slot_name, active, pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) "
                    "FROM pg_replication_slots").strip()
    evidence.update({
        "workload": workload.counts,
        "rows_in_postgres": len(source),
        "converge_seconds_after_last_write": round(converge_seconds, 1),
        "changelog": changelog,
        "replayed_events_after_crash": replayed,
        "reconcile_tombstones": reconciled,
        "error_records": errors,
        "replication_slot": slot,
        "finished_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    })
    print(f"replayed events collapsed by ReplacingMergeTree: {replayed}")
    out = ROOT / "results" / "verification.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(evidence, indent=2) + "\n")
    print(f"evidence written to {out.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
