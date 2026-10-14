"""gold_check의 계산·비교 함수 테스트 (pyspark 없이). Spark 조회 함수는 Databricks에서만 확인한다 (미검증)."""
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "gold"))

import gold_check  # noqa: E402
import gold_tables  # noqa: E402

EXPECTED = json.loads(gold_check.DEFAULT_EXPECTED.read_text(encoding="utf-8"))
RAW = {ch: {key: EXPECTED["per_channel"][ch][key] for key in gold_check.RAW_KEYS} for ch in "ABC"}


class TestDerive(unittest.TestCase):
    def test_metrics_follow_event_spec_formulas(self):
        m = gold_check.derive_metrics({"total_sales": 422000, "refund_amount": 35000, "refund_quantity": 1,
                                       "paid_item_count": 10, "paying_customers": 6})
        self.assertEqual(387000, m["net_sales"])
        self.assertAlmostEqual(35000 / 422000, m["refund_ratio"])
        self.assertEqual(42200, m["unit_price_per_item"])
        self.assertAlmostEqual(70333.3333, m["average_order_value"], places=3)

    def test_zero_denominators_give_none(self):
        m = gold_check.derive_metrics(dict.fromkeys(gold_check.RAW_KEYS, 0))
        self.assertEqual((0, None, None, None), (m["net_sales"], m["refund_ratio"], m["unit_price_per_item"],
                                                 m["average_order_value"]))

    def test_with_total_sums_channels(self):
        total = gold_check.with_total(RAW)["total"]
        self.assertEqual((1266000, 105000, 30, 18), (total["total_sales"], total["refund_amount"],
                                                      total["paid_item_count"], total["paying_customers"]))


class TestCompare(unittest.TestCase):
    def good_metrics(self):
        return {scope: gold_check.derive_metrics(raw) for scope, raw in gold_check.with_total(RAW).items()}

    def test_expected_row_counts(self):
        self.assertEqual({"fact_sales_item": 30, "fact_refund": 3, "dim_channel": 3, "dim_payment_method": 5,
                          "dim_refund_profile": 10, "dim_product": 15},
                         gold_check.expected_row_counts(EXPECTED))

    def test_matching_metrics_have_no_problem(self):
        self.assertEqual([], gold_check.compare_metrics(self.good_metrics(), EXPECTED))

    def test_metric_difference_is_reported_with_scope_and_name(self):
        bad = self.good_metrics()
        bad["B"] = gold_check.derive_metrics({**RAW["B"], "total_sales": RAW["B"]["total_sales"] + 1})
        problems = gold_check.compare_metrics(bad, EXPECTED)
        self.assertTrue(any("지표 B total_sales" in p for p in problems))
        self.assertFalse(any("지표 A" in p for p in problems))

    def test_none_metric_is_not_equal_to_a_number(self):
        bad = self.good_metrics()
        bad["A"] = {**bad["A"], "refund_ratio": None}
        self.assertEqual(1, len(gold_check.compare_metrics({"A": bad["A"]}, EXPECTED)))

    def test_distribution_match_and_mismatch(self):
        name = "sales_item_by_payment_method"
        rows = {(ch, m): n for ch in "ABC" for m, n in EXPECTED["per_channel"][ch][name].items()}
        self.assertEqual([], gold_check.compare_distribution(name, rows, EXPECTED))
        rows[("A", "CARD")] = 5
        problems = gold_check.compare_distribution(name, rows, EXPECTED)
        self.assertEqual(2, len(problems))            # 채널 A와 total
        del rows[("B", "CARD")]
        self.assertEqual(3, len(gold_check.compare_distribution(name, rows, EXPECTED)))

    def test_missing_channel_in_distribution_is_reported(self):
        name = "refund_by_profile"
        rows = {(ch, k): n for ch in "AB" for k, n in EXPECTED["per_channel"][ch][name].items()}
        problems = gold_check.compare_distribution(name, rows, EXPECTED)
        self.assertTrue(any(name + " C" in p for p in problems))

    def test_compare_equal_and_zero(self):
        self.assertEqual([], gold_check.compare_equal({"a": (3, 3)}))
        self.assertEqual(["b: 기대 3, 실제 2"], gold_check.compare_equal({"a": (3, 3), "b": (2, 3)}))
        self.assertEqual([], gold_check.compare_zero("PK 중복", {"x": 0}))
        self.assertEqual(["PK 중복 y: 2건"], gold_check.compare_zero("PK 중복", {"x": 0, "y": 2}))


class TestDefinitions(unittest.TestCase):
    def test_distribution_names_exist_in_expected_and_tables(self):
        for name, (table, column) in gold_check.DISTRIBUTIONS.items():
            self.assertIn(name, EXPECTED["total"])
            self.assertIn(column, gold_tables.column_names(table))

    def test_references_point_at_real_columns_and_dimension_keys(self):
        for fact, column, dim, key in gold_check.REFERENCES:
            self.assertIn(column, gold_tables.column_names(fact))
            self.assertEqual(key, gold_tables.TABLES[dim]["pk"][0])

    def test_every_fact_dimension_column_is_checked(self):
        checked = {(fact, column) for fact, column, _, _ in gold_check.REFERENCES}
        for fact in ("fact_sales_item", "fact_refund"):
            for column in gold_tables.column_names(fact):
                if column in ("channel", "product_id", "payment_method", "refund_profile_key"):
                    self.assertIn((fact, column), checked)


if __name__ == "__main__":
    unittest.main()
