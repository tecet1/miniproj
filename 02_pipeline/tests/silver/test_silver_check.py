"""silver_check의 비교 함수 테스트 (pyspark 없이). Spark 조회 함수는 Databricks에서만 확인한다 (미검증).

정답(기대값) 비교는 두지 않으므로 이 테스트도 기대값 파일을 읽지 않는다.
"""
import inspect
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

import silver_check  # noqa: E402
from silver_tables import TABLES  # noqa: E402


class FakeSpark:
    """모든 조회가 같은 값 한 건을 돌려주는 가짜 Spark. 실행한 SQL을 모은다."""

    def __init__(self, n=0):
        self.n, self.queries = n, []

    def sql(self, statement):
        self.queries.append(statement)
        return mock.Mock(first=lambda: {"n": self.n})


class TestCompareZero(unittest.TestCase):
    def test_zero_counts_have_no_problem(self):
        self.assertEqual([], silver_check.compare_zero("PK 중복", {"orders": 0, "payments": 0}))

    def test_non_zero_counts_are_reported(self):
        self.assertEqual(["PK 중복 payments: 2건"], silver_check.compare_zero("PK 중복", {"orders": 0, "payments": 2}))


class TestNoExpectedValues(unittest.TestCase):
    def test_no_expected_file_or_path_argument(self):
        self.assertEqual(["spark", "schema", "bronze_table"], list(inspect.signature(silver_check.check_silver).parameters))
        self.assertFalse(hasattr(silver_check, "DEFAULT_EXPECTED"))

    def test_checks_have_no_time_cutoff(self):
        self.assertNotIn("since", inspect.signature(silver_check.check_silver).parameters)


class TestCheckSilver(unittest.TestCase):
    def test_clean_silver_passes(self):
        self.assertEqual([], silver_check.check_silver(FakeSpark(0), "s", "b.events"))

    def test_every_check_runs_and_a_violation_is_reported_with_its_label(self):
        problems = silver_check.check_silver(FakeSpark(2), "s", "b.events")
        for label in ("PK 중복", "참조 누락", "금액 불일치", "업무 ID 중복", "원본 보존"):
            self.assertTrue(any(p.startswith(label) for p in problems), (label, problems))

    def test_pk_check_covers_every_silver_table(self):
        self.assertEqual(set(TABLES), set(silver_check.count_pk_duplicates(FakeSpark(0), "s")))

    def test_quarantine_check_joins_on_bronze_position(self):
        spark = FakeSpark(0)
        silver_check.count_quarantine_mismatches(spark, "s", "b.events")
        self.assertIn("eh_partition", spark.queries[0])
        self.assertIn("eh_offset", spark.queries[0])


if __name__ == "__main__":
    unittest.main()
