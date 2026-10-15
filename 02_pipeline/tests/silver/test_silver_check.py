"""silver_check(보고 전용) 테스트 (pyspark 없이). Spark 조회는 Databricks에서만 확인한다 (미검증).

자체 정합성 검사는 규칙 목록(quality/rules.json)으로 옮겼고 tests/quality가 본다. 여기에는 보고 함수만 남았다.
"""
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

import silver_check  # noqa: E402


class FakeSpark:
    """모든 조회가 같은 값 한 건을 돌려주는 가짜 Spark. 실행한 SQL을 모은다."""

    def __init__(self, n=0):
        self.n, self.queries = n, []

    def sql(self, statement):
        self.queries.append(statement)
        return mock.Mock(first=lambda: {"n": self.n})


class TestReportByDesign(unittest.TestCase):
    def test_counts_are_reported_by_name_and_never_fail(self):
        report = silver_check.report_by_design(FakeSpark(7), "s")
        self.assertEqual({"업무 ID 중복 orders", "업무 ID 중복 order_cancellations", "업무 ID 중복 refunds",
                          "참조 누락 refunds->order_items", "참조 누락 refunds->payments(SUCCEEDED)"}, set(report))
        self.assertEqual({7}, set(report.values()))

    def test_clean_silver_reports_zeros(self):
        self.assertEqual({0}, set(silver_check.report_by_design(FakeSpark(0), "s").values()))

    def test_the_moved_checks_are_gone_from_this_module(self):
        for name in ("check_silver", "compare_zero", "count_pk_duplicates", "count_amount_mismatches", "count_quarantine_mismatches"):
            self.assertFalse(hasattr(silver_check, name), name)


if __name__ == "__main__":
    unittest.main()
