"""Independent failure fixtures and canonical business/error identities."""

import json
import sys
import unittest
from copy import deepcopy
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "02_pipeline/silver"))
from channel_a import canonical_json, inspect_raw_event, normalize_event


FIXTURES = ROOT / "01_contracts/data/fixtures/channel_a"
MAPPING = json.loads((ROOT / "01_contracts/data/reference/sku_mapping.json").read_text(encoding="utf-8"))


class SilverQualityTests(unittest.TestCase):
    def test_independent_error_cases(self):
        cases = json.loads((FIXTURES / "quality_cases.json").read_text(encoding="utf-8"))
        for case in cases:
            with self.subTest(case=case["case_id"]):
                actual = inspect_raw_event(case["raw_payload"], MAPPING)
                self.assertIsNone(actual["common_json"])
                self.assertEqual(actual["rule_id"], case["rule_id"])
                self.assertEqual(actual["error_code"], case["error_code"])

    def test_valid_events_equal_independent_common_results(self):
        expected = json.loads((FIXTURES / "expected_common_events.json").read_text(encoding="utf-8"))
        for filename in ["events.jsonl", "events_v2.jsonl"]:
            actual = [inspect_raw_event(line, MAPPING) for line in
                      (FIXTURES / filename).read_text(encoding="utf-8").splitlines()]
            self.assertTrue(all(row["error_code"] is None for row in actual))
            self.assertEqual([json.loads(row["common_json"]) for row in actual], expected)

    def test_replay_does_not_change_common_identity(self):
        event = json.loads((FIXTURES / "events.jsonl").read_text(encoding="utf-8").splitlines()[0])
        first = inspect_raw_event(canonical_json(event), MAPPING)
        replay = inspect_raw_event(canonical_json({**event, "test_run_id": "new-run",
                                                   "published_at": "2026-10-12T03:00:00Z"}), MAPPING)
        self.assertEqual(first["common_json"], replay["common_json"])
        self.assertEqual(first["input_hash"], replay["input_hash"])

    def test_wrong_input_replay_preserves_error_identity(self):
        event = {"channel_id": "A", "schema_version": "1.0", "event_name": "order_created",
                 "event_id": "a-invalid", "payload": {}}
        first = inspect_raw_event(canonical_json(event), MAPPING)
        replay = inspect_raw_event(canonical_json({**event, "test_run_id": "other"}), MAPPING)
        for field in ["input_hash", "rule_id", "error_code"]:
            self.assertEqual(first[field], replay[field])

    def test_payment_headers_do_not_require_items_or_sku(self):
        event = json.loads((FIXTURES / "events.jsonl").read_text(encoding="utf-8").splitlines()[4])
        result = inspect_raw_event(canonical_json(event), {})
        self.assertIsNone(result["error_code"])
        self.assertNotIn("items", json.loads(result["common_json"])["data"])

    def test_customer_id_is_nullable_but_not_an_empty_string(self):
        event = json.loads((FIXTURES / "events.jsonl").read_text(encoding="utf-8").splitlines()[0])
        event.pop("customer_no")
        self.assertIsNone(normalize_event(event, MAPPING)["uid"])
        event["customer_no"] = " "
        self.assertEqual(inspect_raw_event(canonical_json(event), MAPPING)["rule_id"], "DQ-02")

    def test_utc_offsets_normalize_to_the_same_business_values(self):
        event = json.loads((FIXTURES / "events.jsonl").read_text(encoding="utf-8").splitlines()[0])
        equivalent = {**event, "occurred_at": "2026-10-10T12:00:00+09:00"}
        self.assertEqual(inspect_raw_event(canonical_json(event), MAPPING)["common_json"],
                         inspect_raw_event(canonical_json(equivalent), MAPPING)["common_json"])

    def test_business_change_changes_canonical_values(self):
        event = json.loads((FIXTURES / "events.jsonl").read_text(encoding="utf-8").splitlines()[0])
        changed = deepcopy(event)
        changed["payload"]["total"] += 1
        self.assertNotEqual(inspect_raw_event(canonical_json(event), MAPPING)["common_json"],
                            inspect_raw_event(canonical_json(changed), MAPPING)["common_json"])

    def test_unrepresentable_utc_time_is_an_input_error(self):
        event = json.loads((FIXTURES / "events.jsonl").read_text(encoding="utf-8").splitlines()[0])
        event["occurred_at"] = "0001-01-01T00:00:00+14:00"
        result = inspect_raw_event(canonical_json(event), MAPPING)
        self.assertEqual((result["rule_id"], result["error_code"]), ("DQ-03", "INVALID_TIMESTAMP"))

    def test_programming_errors_are_not_quarantined(self):
        event = json.loads((FIXTURES / "events.jsonl").read_text(encoding="utf-8").splitlines()[0])
        with self.assertRaises(TypeError):
            inspect_raw_event(canonical_json(event), None)


if __name__ == "__main__":
    unittest.main()
