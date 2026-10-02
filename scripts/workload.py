"""Deterministic OLTP workload for app.orders.

Produces SQL transactions mixing inserts, fills, cancels, deletes and
multi-update transactions on the same row. The generator keeps its own model
of the expected table state, so the database can be checked against it.
"""

from __future__ import annotations

import argparse
import random
import sys
from decimal import ROUND_DOWN, Decimal

SYMBOLS = {"BTCUSDT": Decimal("95000"), "ETHUSDT": Decimal("3300"), "BBCA": Decimal("9800"), "TLKM": Decimal("2800")}
ACCOUNTS = [f"acc-{i:03d}" for i in range(1, 21)]
OPEN = ("new", "partially_filled")
Q8 = Decimal("0.00000001")


def q8(value: Decimal) -> Decimal:
    return value.quantize(Q8, rounding=ROUND_DOWN)


class Workload:
    def __init__(self, seed: int = 7, start_id: int = 1):
        self.rng = random.Random(seed)
        self.next_id = start_id
        self.orders: dict[int, dict] = {}
        self.counts = {"insert": 0, "update": 0, "delete": 0, "transactions": 0}

    # Each op mutates the model and returns one SQL statement, or None if the
    # op does not apply to the current state.
    def _insert(self) -> str:
        order_id = self.next_id
        self.next_id += 1
        symbol = self.rng.choice(list(SYMBOLS))
        price = q8(SYMBOLS[symbol] * Decimal(str(self.rng.uniform(0.95, 1.05))))
        qty = q8(Decimal(str(self.rng.uniform(0.01, 5))))
        row = {
            "order_id": order_id, "account_id": self.rng.choice(ACCOUNTS), "symbol": symbol,
            "side": self.rng.choice(["buy", "sell"]), "quantity": qty, "limit_price": price,
            "filled_quantity": Decimal(0).quantize(Q8), "status": "new",
        }
        self.orders[order_id] = row
        self.counts["insert"] += 1
        return (
            "INSERT INTO app.orders (order_id, account_id, symbol, side, quantity, limit_price, status) "
            f"VALUES ({order_id}, '{row['account_id']}', '{symbol}', '{row['side']}', {qty}, {price}, 'new');"
        )

    def _pick(self, statuses: tuple[str, ...]) -> dict | None:
        ids = [i for i, o in self.orders.items() if o["status"] in statuses]
        return self.orders[self.rng.choice(ids)] if ids else None

    def _fill(self) -> str | None:
        order = self._pick(OPEN)
        if order is None:
            return None
        remaining = order["quantity"] - order["filled_quantity"]
        fill = remaining if self.rng.random() < 0.4 else q8(remaining * Decimal(str(self.rng.uniform(0.1, 0.9))))
        order["filled_quantity"] += fill
        order["status"] = "filled" if order["filled_quantity"] == order["quantity"] else "partially_filled"
        self.counts["update"] += 1
        return (
            f"UPDATE app.orders SET filled_quantity = {order['filled_quantity']}, status = '{order['status']}', "
            f"updated_at = clock_timestamp() WHERE order_id = {order['order_id']};"
        )

    def _cancel(self) -> str | None:
        order = self._pick(OPEN)
        if order is None:
            return None
        order["status"] = "cancelled"
        self.counts["update"] += 1
        return (
            "UPDATE app.orders SET status = 'cancelled', updated_at = clock_timestamp() "
            f"WHERE order_id = {order['order_id']};"
        )

    def _delete(self) -> str | None:
        order = self._pick(("cancelled",))
        if order is None:
            return None
        del self.orders[order["order_id"]]
        self.counts["delete"] += 1
        return f"DELETE FROM app.orders WHERE order_id = {order['order_id']};"

    def _op(self) -> str | None:
        roll = self.rng.random()
        if roll < 0.40:
            return self._insert()
        if roll < 0.72:
            return self._fill()
        if roll < 0.88:
            return self._cancel()
        return self._delete()

    def batch(self, transactions: int) -> str:
        lines: list[str] = []
        for _ in range(transactions):
            statements = [s for s in (self._op() for _ in range(self.rng.randint(1, 4))) if s]
            # Same row touched twice in one transaction: the sink must keep the later LSN.
            if self.rng.random() < 0.05:
                statements += [s for s in (self._fill(), self._fill()) if s]
            if statements:
                lines += ["BEGIN;", *statements, "COMMIT;"]
                self.counts["transactions"] += 1
        return "\n".join(lines) + "\n"

    def seed_rows(self, count: int) -> str:
        return "BEGIN;\n" + "\n".join(self._insert() for _ in range(count)) + "\nCOMMIT;\n"

    def expected_rows(self) -> list[tuple]:
        """Expected table content without updated_at, ordered by order_id."""
        return [
            (o["order_id"], o["account_id"], o["symbol"], o["side"], o["quantity"], o["limit_price"],
             o["filled_quantity"], o["status"])
            for _, o in sorted(self.orders.items())
        ]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--start-id", type=int, default=1_000_000)
    parser.add_argument("--transactions", type=int, default=100)
    args = parser.parse_args()
    workload = Workload(args.seed, args.start_id)
    sys.stdout.write(workload.seed_rows(20) + workload.batch(args.transactions))


if __name__ == "__main__":
    main()
