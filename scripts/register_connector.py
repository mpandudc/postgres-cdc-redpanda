"""Create or update the Debezium connector through the Kafka Connect REST API.

PUT /connectors/<name>/config is idempotent: it creates the connector on the
first call and updates it afterwards.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

CONFIG = Path(__file__).resolve().parents[1] / "connect" / "orders-connector.json"
NAME = "orders-cdc"


def connect_url() -> str:
    return os.environ.get("CONNECT_URL", "http://localhost:18083")


def request(method: str, path: str, body: dict | None = None) -> dict | list:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        connect_url() + path, data=data, method=method, headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        raw = response.read()
        return json.loads(raw) if raw else {}


def render_config() -> dict:
    text = CONFIG.read_text(encoding="utf-8")
    text = text.replace("${DEBEZIUM_PASSWORD}", os.environ.get("DEBEZIUM_PASSWORD", "debezium"))
    return json.loads(text)


def wait_for_connect(timeout: float = 180) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            request("GET", "/connectors")
            return
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            time.sleep(2)
    raise SystemExit("Kafka Connect REST API did not come up")


def status() -> dict:
    try:
        return request("GET", f"/connectors/{NAME}/status")  # type: ignore[return-value]
    except (urllib.error.URLError, ConnectionError, TimeoutError):
        return {}


def wait_running(timeout: float = 180) -> dict:
    deadline = time.monotonic() + timeout
    current: dict = {}
    while time.monotonic() < deadline:
        current = status()
        tasks = current.get("tasks", [])
        if current.get("connector", {}).get("state") == "RUNNING" and tasks and all(
            t["state"] == "RUNNING" for t in tasks
        ):
            return current
        if any(t.get("state") == "FAILED" for t in tasks):
            raise SystemExit(f"connector task failed: {json.dumps(current)[:2000]}")
        time.sleep(2)
    raise SystemExit(f"connector not RUNNING: {current}")


def register() -> dict:
    wait_for_connect()
    request("PUT", f"/connectors/{NAME}/config", render_config())
    return wait_running()


if __name__ == "__main__":
    print(json.dumps(register(), indent=2))
