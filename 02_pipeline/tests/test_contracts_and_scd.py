"""Run from project root: python -m unittest discover -s 02_pipeline/tests -v"""

import importlib.util
import json
import unittest
from copy import deepcopy
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]


def load_module(name: str, relative_path: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


generator = load_module("fixture", "01_contracts/src/generate_channel_a.py")
adapter = load_module("channel_a", "02_pipeline/silver/channel_a.py")
scd = load_module("product_scd", "02_pipeline/gold/product_scd.py")


class ContractAndSCDTests(unittest.TestCase):
    def setUp(self):
        self.mapping = json.loads((ROOT / "01_contracts/data/reference/sku_mapping.json").read_text(encoding="utf-8"))
        self.changes = json.loads((ROOT / "01_contracts/data/reference/product_changes.json").read_text(encoding="utf-8"))

    def test_v1_v2_normalize_to_the_same_six_business_events(self):
        source_v1, source_v2 = generator.build_events("1.0"), generator.build_events("2.0")
        self.assertNotEqual(source_v1[0]["payload"], source_v2[0]["payload"])
        v1 = [adapter.normalize_event(event, self.mapping) for event in source_v1]
        v2 = [adapter.normalize_event(event, self.mapping) for event in source_v2]
        self.assertEqual(v1, v2)
        expected = json.loads((ROOT / "01_contracts/data/fixtures/channel_a/expected_common_events.json").read_text(encoding="utf-8"))
        self.assertEqual(v1, expected)

    def test_unknown_version_and_missing_versioned_field_are_rejected(self):
        event = generator.build_events()[0]
        with self.assertRaisesRegex(ValueError, "UNSUPPORTED_SCHEMA_VERSION"):
            adapter.normalize_event({**event, "schema_version": "3.0"}, self.mapping)
        with self.assertRaisesRegex(ValueError, "MISSING_OR_INVALID_FIELD|INVALID_INTEGER"):
            adapter.normalize_event({**event, "schema_version": "2.0"}, self.mapping)

    def test_no_source_mutation_and_unmapped_sku_rejected(self):
        event = generator.build_events()[0]
        before = deepcopy(event)
        adapter.normalize_event(event, self.mapping)
        self.assertEqual(event, before)
        with self.assertRaisesRegex(ValueError, "UNMAPPED_SKU"):
            adapter.normalize_event(event, {})

    def test_category_change_and_exact_boundary(self):
        history = scd.build_product_history(self.changes)
        self.assertEqual(len(history), 3)
        old = scd.lookup_product(history, "SKU-001", "2026-10-10T03:00:00Z")
        new = scd.lookup_product(history, "SKU-001", "2026-10-11T00:00:00Z")
        self.assertEqual(old["category"], "의류")
        self.assertEqual(new["category"], "아웃도어")
        self.assertNotEqual(old["product_key"], new["product_key"])
        self.assertFalse(old["is_current"])
        self.assertTrue(new["is_current"])
        self.assertEqual(old["valid_to"], new["valid_from"])

    def test_replay_order_and_unchanged_attributes_do_not_add_versions(self):
        expected = scd.build_product_history(self.changes)
        self.assertEqual(expected, scd.build_product_history(list(reversed(self.changes))))
        self.assertEqual(expected, scd.build_product_history(self.changes * 2))
        unchanged = {**self.changes[1], "effective_from": "2026-10-12T00:00:00Z"}
        self.assertEqual(expected, scd.build_product_history([*self.changes, unchanged]))

    def test_product_name_and_brand_changes_also_create_versions(self):
        for field in ("product_name", "brand"):
            with self.subTest(field=field):
                change = {**self.changes[0], field: "변경된 값", "effective_from": "2026-10-03T00:00:00Z"}
                history = scd.build_product_history([self.changes[0], change])
                self.assertEqual(len(history), 2)
                self.assertEqual(history[0][field], self.changes[0][field])
                self.assertEqual(history[1][field], "변경된 값")

    def test_empty_history_cannot_overwrite_dimension(self):
        with self.assertRaisesRegex(ValueError, "EMPTY_PRODUCT_HISTORY"):
            scd.write_delta_dimension(None, [])

    def test_same_effective_time_different_attributes_is_a_conflict(self):
        conflict = {**self.changes[0], "category": "충돌"}
        with self.assertRaisesRegex(ValueError, "CONFLICTING_PRODUCT_CHANGE"):
            scd.build_product_history([*self.changes, conflict])

    def test_conflict_is_detected_even_when_one_change_would_be_unchanged(self):
        unchanged = {**self.changes[0], "effective_from": "2026-10-05T00:00:00Z"}
        conflict = {**unchanged, "category": "충돌"}
        with self.assertRaisesRegex(ValueError, "CONFLICTING_PRODUCT_CHANGE"):
            scd.build_product_history([*self.changes, unchanged, conflict])

    def test_unmapped_time_and_overlapping_versions_are_rejected(self):
        history = scd.build_product_history(self.changes)
        with self.assertRaisesRegex(ValueError, "PRODUCT_VERSION_MATCH_COUNT"):
            scd.lookup_product(history, "SKU-001", "2026-09-01T00:00:00Z")
        with self.assertRaisesRegex(ValueError, "PRODUCT_VERSION_MATCH_COUNT"):
            scd.lookup_product([*history, history[0]], "SKU-001", "2026-10-10T03:00:00Z")

    def test_historical_transaction_keeps_original_category(self):
        history = scd.build_product_history(self.changes)
        payment = adapter.normalize_event(generator.build_events()[4], self.mapping)
        product = scd.lookup_product(history, "SKU-001", payment["event_time"])
        self.assertEqual(product["category"], "의류")
        current = next(row for row in history if row["sku_id"] == "SKU-001" and row["is_current"])
        self.assertEqual(current["category"], "아웃도어")


if __name__ == "__main__":
    unittest.main()
