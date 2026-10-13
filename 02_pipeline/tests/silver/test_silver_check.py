"""silver_check의 비교 함수 테스트 (pyspark 없이). Spark 조회 함수는 Databricks에서만 확인한다."""
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

import silver_check  # noqa: E402

EXPECTED = json.loads(silver_check.DEFAULT_EXPECTED.read_text(encoding="utf-8"))["total"]
GOOD = {"orders": 27, "order_items": 33, "payments": 27, "order_cancellations": 3, "refunds": 3, "quarantine": 9}
REASONS = {(ch, r): 1 for ch in "ABC" for r in ("MISSING_REQUIRED", "INVALID_TYPE", "UNKNOWN_EVENT_TYPE")}


class TestCompare(unittest.TestCase):
    def test_default_expected_file_is_found(self):
        self.assertEqual(72, EXPECTED["bronze_rows"])

    def test_good_counts_have_no_problem(self):
        self.assertEqual([], silver_check.compare_counts(GOOD, EXPECTED))

    def test_count_difference_is_reported(self):
        problems = silver_check.compare_counts({**GOOD, "payments": 28}, EXPECTED)
        self.assertEqual(1, len(problems))
        self.assertIn("payments", problems[0])

    def test_bronze_balance(self):
        self.assertEqual([], silver_check.compare_bronze_balance(72, GOOD, EXPECTED))   # 72 = 60 + 9 + 3
        self.assertEqual(1, len(silver_check.compare_bronze_balance(73, GOOD, EXPECTED)))   # 중복이 4건이 된다
        self.assertEqual(1, len(silver_check.compare_bronze_balance(72, {**GOOD, "orders": 26}, EXPECTED)))

    def test_reasons(self):
        self.assertEqual([], silver_check.compare_reasons(REASONS))
        broken = dict(REASONS)
        broken[("B", "INVALID_TYPE")] = 2
        self.assertEqual(1, len(silver_check.compare_reasons(broken)))
        del broken[("B", "INVALID_TYPE")]
        self.assertEqual(1, len(silver_check.compare_reasons(broken)))

    def test_compare_zero(self):
        self.assertEqual([], silver_check.compare_zero("PK 중복", {"orders": 0, "payments": 0}))
        self.assertEqual(["PK 중복 payments: 2건"], silver_check.compare_zero("PK 중복", {"orders": 0, "payments": 2}))


if __name__ == "__main__":
    unittest.main()
