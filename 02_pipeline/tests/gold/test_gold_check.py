"""gold_check(보고 전용) 테스트 (pyspark 없이). Spark 조회는 Databricks에서만 확인한다 (미검증).

자체 정합성 검사는 규칙 목록(quality/rules.json)으로 옮겼고 tests/quality가 본다. 여기에는 제외 건수 보고만 남았다.
"""
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "gold"))

import gold_check  # noqa: E402


class TestReportExclusions(unittest.TestCase):
    class Spark:
        def __init__(self, violations, invalid):
            self.values = {"violation": violations, "invalid": invalid}

        def sql(self, statement):
            key = "invalid" if "FROM c.silver.refunds r" in statement and "LEFT JOIN" in statement else "violation"
            return mock.Mock(first=lambda: {"n": self.values[key]})

    def test_counts_are_reported_by_name(self):
        self.assertEqual({"업무 키 위반 주문": 3, "무효 환불": 5, "위반 주문에 붙은 환불": 3}, gold_check.report_exclusions(self.Spark(3, 5), "c.silver"))

    def test_the_moved_checks_are_gone_from_this_module(self):
        for name in ("check_gold", "compare_equal", "compare_zero", "count_pk_duplicates", "refund_totals"):
            self.assertFalse(hasattr(gold_check, name), name)


if __name__ == "__main__":
    unittest.main()
