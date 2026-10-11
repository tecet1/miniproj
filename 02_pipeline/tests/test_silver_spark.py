"""Optional local Spark checks. Delta writes are verified in the Databricks notebook."""

import json
import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "02_pipeline/silver"))
RUN_SPARK = os.environ.get("RUN_SPARK_TESTS") == "1"


@unittest.skipUnless(RUN_SPARK, "Set RUN_SPARK_TESTS=1 on a machine with PySpark and supported Java")
class SparkSilverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        import silver_pipeline

        cls.pipeline = silver_pipeline
        os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
        os.environ["PYTHONPATH"] = os.pathsep.join(
            [str(ROOT / "02_pipeline/silver"), os.environ.get("PYTHONPATH", "")])
        cls.spark = (SparkSession.builder.master("local[2]").appName("silver-contract-checks")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.mapping = json.loads((ROOT / "01_contracts/data/reference/sku_mapping.json").read_text(encoding="utf-8"))
        cls.source = (ROOT / "01_contracts/data/fixtures/channel_a/events.jsonl").read_text(encoding="utf-8").splitlines()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def bronze(self, raw):
        now = datetime(2026, 10, 12, tzinfo=timezone.utc)
        rows = [(line, "fixture-event-hub", 0, index, now, now) for index, line in enumerate(raw)]
        return self.spark.createDataFrame(rows, self.pipeline.SOURCE_SCHEMA)

    def writes(self, raw, existing=None):
        normalized = self.pipeline.normalize_bronze(self.bronze(raw), self.mapping)
        target = existing if existing is not None else self.spark.createDataFrame([], self.pipeline.SILVER_SCHEMA)
        return self.pipeline.prepare_writes(normalized, target)

    def test_normal_types_arrays_and_duplicate_deliveries(self):
        from pyspark.sql.types import DecimalType, TimestampType

        normal, errors = self.writes(self.source * 2)
        self.assertEqual(normal.count(), 6)
        self.assertEqual(errors.count(), 0)
        self.assertIsInstance(normal.schema["event_time"].dataType, TimestampType)
        self.assertEqual(normal.schema["data"].dataType["paid_amount"].dataType, DecimalType(18, 0))
        by_id = {row.event_id: row for row in normal.collect()}
        self.assertEqual(len(by_id["a-poc-v1-03"].data.items), 2)
        self.assertEqual(by_id["a-poc-v1-05"].data.paid_amount, 80000)
        self.assertEqual(by_id["a-poc-v1-06"].data.refund_amount, 25000)

    def test_error_fixtures_have_independent_rule_codes(self):
        cases = json.loads((ROOT / "01_contracts/data/fixtures/channel_a/quality_cases.json").read_text(encoding="utf-8"))
        normal, errors = self.writes([case["raw_payload"] for case in cases])
        self.assertEqual(normal.count(), 0)
        actual = sorted((row.rule_id, row.error_code) for row in errors.collect())
        self.assertEqual(actual, sorted((case["rule_id"], case["error_code"]) for case in cases))

    def test_new_key_conflicts_reject_both_versions(self):
        first = json.loads(self.source[0])
        changed = {**first, "payload": {**first["payload"], "total": 123}}
        normal, errors = self.writes([json.dumps(first), json.dumps(changed)])
        self.assertEqual(normal.count(), 0)
        self.assertEqual(errors.count(), 2)
        self.assertEqual({row.error_code for row in errors.collect()}, {"EVENT_KEY_CONFLICT"})

    def test_existing_key_preserves_original_and_quarantines_change(self):
        existing, _ = self.writes(self.source)
        existing = self.spark.createDataFrame(existing.collect(), self.pipeline.SILVER_SCHEMA)
        first = json.loads(self.source[0])
        changed = {**first, "payload": {**first["payload"], "total": 123}}
        normal, errors = self.writes([self.source[0], json.dumps(changed)], existing)
        self.assertEqual(normal.count(), 1)
        self.assertEqual(errors.count(), 1)
        self.assertEqual(normal.first().data.order_amount, 50000)

    def test_v2_matches_existing_without_conflict(self):
        existing, _ = self.writes(self.source)
        existing = self.spark.createDataFrame(existing.collect(), self.pipeline.SILVER_SCHEMA)
        raw_v2 = (ROOT / "01_contracts/data/fixtures/channel_a/events_v2.jsonl").read_text(encoding="utf-8").splitlines()
        normal, errors = self.writes(raw_v2, existing)
        self.assertEqual(normal.count(), 6)
        self.assertEqual(errors.count(), 0)

    def test_error_retransmissions_share_quarantine_identity(self):
        cases = json.loads((ROOT / "01_contracts/data/fixtures/channel_a/quality_cases.json").read_text(encoding="utf-8"))
        event = json.loads(next(case["raw_payload"] for case in cases if case["case_id"] == "unmapped_sku"))
        replay = {**event, "test_run_id": "another-delivery"}
        _, errors = self.writes([json.dumps(event), json.dumps(replay)])
        self.assertEqual(errors.count(), 1)


if __name__ == "__main__":
    unittest.main()
