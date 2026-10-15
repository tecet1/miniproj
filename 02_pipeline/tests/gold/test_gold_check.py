"""gold_check의 비교 함수와 정의 테스트 (pyspark 없이). Spark 조회 함수는 Databricks에서만 확인한다 (미검증).

정답(기대값) 비교는 두지 않으므로 이 테스트도 기대값 파일을 읽지 않는다.
"""
import inspect
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "gold"))

import gold_check  # noqa: E402
import gold_tables  # noqa: E402


class FakeSpark:
    """조회마다 정해 둔 한 행을 돌려주는 가짜 Spark. 합계 조회는 환불 합계(amount, quantity)를 돌려준다."""

    def __init__(self, sales=(0, 0), refund=(0, 0), n=0):
        self.sales, self.refund, self.n = sales, refund, n

    def sql(self, statement):
        if "SUM(refund_amount)" in statement:
            amount, quantity = self.sales if "fact_sales_item" in statement else self.refund
            row = {"refund_amount": amount, "refund_quantity": quantity}
        else:
            row = {"n": self.n}
        return mock.Mock(first=lambda: row)


class TestCompare(unittest.TestCase):
    def test_compare_equal_and_zero(self):
        self.assertEqual([], gold_check.compare_equal({"a": (3, 3)}))
        self.assertEqual(["b: 기대 3, 실제 2"], gold_check.compare_equal({"a": (3, 3), "b": (2, 3)}))
        self.assertEqual([], gold_check.compare_zero("PK 중복", {"x": 0}))
        self.assertEqual(["PK 중복 y: 2건"], gold_check.compare_zero("PK 중복", {"x": 0, "y": 2}))


class TestCheckGold(unittest.TestCase):
    def test_no_expected_file_or_path_argument(self):
        self.assertEqual(["spark", "silver_schema", "gold_schema"], list(inspect.signature(gold_check.check_gold).parameters))
        self.assertFalse(hasattr(gold_check, "DEFAULT_EXPECTED"))

    def test_consistent_gold_passes(self):
        self.assertEqual([], gold_check.check_gold(FakeSpark(sales=(35000, 1), refund=(35000, 1)), "s", "g"))

    def test_refund_amount_difference_between_facts_is_reported(self):
        problems = gold_check.check_gold(FakeSpark(sales=(35000, 1), refund=(34000, 1)), "s", "g")
        self.assertEqual(1, len(problems))
        self.assertIn("환불금액", problems[0])

    def test_refund_quantity_difference_between_facts_is_reported(self):
        problems = gold_check.check_gold(FakeSpark(sales=(35000, 2), refund=(35000, 1)), "s", "g")
        self.assertEqual(1, len(problems))
        self.assertIn("환불수량", problems[0])

    def test_pk_dimension_and_payment_violations_are_reported_with_labels(self):
        problems = gold_check.check_gold(FakeSpark(n=3), "s", "g")
        for label in ("PK 중복", "Dimension 키 누락", "대조 불일치"):
            self.assertTrue(any(p.startswith(label) for p in problems), (label, problems))


class TestDefinitions(unittest.TestCase):
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



class TestReportExclusions(unittest.TestCase):
    class Spark:
        def __init__(self, violations, invalid):
            self.values = {"violation": violations, "invalid": invalid}

        def sql(self, statement):
            key = "invalid" if "FROM c.silver.refunds r" in statement else "violation"
            return mock.Mock(first=lambda: {"n": self.values[key]})

    def test_counts_are_reported_by_name(self):
        self.assertEqual({"업무 키 위반 주문": 3, "무효 환불": 5}, gold_check.report_exclusions(self.Spark(3, 5), "c.silver"))

    def test_exclusions_are_a_report_not_a_check_failure(self):
        """check_gold의 문제 목록에는 넣지 않는다 (event_spec.md 6.6절: 제외된 상태로 보고만 한다)."""
        self.assertEqual([], gold_check.check_gold(FakeSpark(sales=(0, 0), refund=(0, 0)), "s", "g"))


if __name__ == "__main__":
    unittest.main()
