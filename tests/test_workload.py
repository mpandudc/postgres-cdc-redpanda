import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from workload import Workload  # noqa: E402


class WorkloadTest(unittest.TestCase):
    def test_same_seed_same_sql(self):
        self.assertEqual(Workload(3).batch(50), Workload(3).batch(50))

    def test_transactions_are_balanced(self):
        sql = Workload(1).batch(200).splitlines()
        self.assertEqual(sql.count("BEGIN;"), sql.count("COMMIT;"))

    def test_mix_covers_all_operations(self):
        workload = Workload(5)
        workload.seed_rows(50)
        workload.batch(300)
        for op in ("insert", "update", "delete"):
            self.assertGreater(workload.counts[op], 0, op)

    def test_model_invariants(self):
        workload = Workload(9)
        workload.seed_rows(100)
        workload.batch(500)
        for row in workload.expected_rows():
            _, _, _, _, qty, _, filled, status = row
            self.assertLessEqual(filled, qty)
            if status == "filled":
                self.assertEqual(filled, qty)
            if status == "new":
                self.assertEqual(filled, 0)

    def test_deleted_orders_leave_the_model(self):
        workload = Workload(11)
        sql = workload.seed_rows(100) + workload.batch(500)
        deleted = {int(line.rsplit(" ", 1)[1].rstrip(";")) for line in sql.splitlines() if line.startswith("DELETE")}
        self.assertTrue(deleted)
        self.assertFalse(deleted & {row[0] for row in workload.expected_rows()})


class ConnectorConfigTest(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((ROOT / "connect" / "orders-connector.json").read_text(encoding="utf-8"))

    def test_no_plaintext_password(self):
        self.assertEqual(self.config["database.password"], "${DEBEZIUM_PASSWORD}")

    def test_publication_is_preprovisioned(self):
        self.assertEqual(self.config["publication.autocreate.mode"], "disabled")
        schema = (ROOT / "postgres" / "init" / "001_schema.sql").read_text(encoding="utf-8")
        self.assertIn(f"CREATE PUBLICATION {self.config['publication.name']}", schema)

    def test_unwrap_emits_fields_the_sink_reads(self):
        sink = (ROOT / "clickhouse" / "init" / "001_cdc.sql").read_text(encoding="utf-8")
        for field in ("__op", "__lsn", "__source_ts_ms", "__source_snapshot", "__deleted"):
            self.assertIn(field, sink)
        self.assertEqual(self.config["transforms.unwrap.add.fields"], "op,lsn,source.ts_ms,source.snapshot")
        self.assertEqual(self.config["transforms.unwrap.delete.tombstone.handling.mode"], "rewrite")


if __name__ == "__main__":
    unittest.main()
