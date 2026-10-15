"""silver_check의 비교 함수 테스트 (pyspark 없이). Spark 조회 함수는 Databricks에서만 확인한다."""
import inspect
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

    def test_checks_have_no_time_cutoff(self):
        for function in (silver_check.check_silver, silver_check.count_bronze):
            self.assertNotIn("since", inspect.signature(function).parameters, function.__name__)

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

    def test_compare_equal(self):
        self.assertEqual([], silver_check.compare_equal({"a": (3, 3), "b": (1, 1)}))
        self.assertEqual(["b: 기대 3, 실제 2"], silver_check.compare_equal({"a": (3, 3), "b": (2, 3)}))

    def test_compare_zero(self):
        self.assertEqual([], silver_check.compare_zero("PK 중복", {"orders": 0, "payments": 0}))
        self.assertEqual(["PK 중복 payments: 2건"], silver_check.compare_zero("PK 중복", {"orders": 0, "payments": 2}))


DUMP_EXPECTED_PATH = ROOT / "04_experiments" / "results" / "expected_dump.json"
DATA_TABLES = ("silver_orders", "silver_order_items", "silver_payments", "silver_order_cancellations",
               "silver_refunds", "quarantine")


class TestCombinedExpected(unittest.TestCase):
    """합친 기대값(픽스처 72건 + 덤프)을 expected_path로 쓸 때 비교 함수가 맞물리는지 본다. Spark 결과는 확인하지 않는다."""

    @classmethod
    def setUpClass(cls):
        # 기준 파일이므로 없으면 건너뛰지 않고 여기서 실패한다
        cls.data = json.loads(DUMP_EXPECTED_PATH.read_text(encoding="utf-8"))
        cls.total = cls.data["total"]
        cls.good = {table: cls.total[key] for table, key in silver_check.EXPECTED_KEY.items()}

    def test_total_has_every_key_check_silver_reads(self):
        for key in ("bronze_rows", *silver_check.EXPECTED_KEY.values()):
            self.assertIn(key, self.total)

    def test_channels_add_up_to_total(self):
        for key in ("bronze_rows", *DATA_TABLES):
            self.assertEqual(self.total[key], sum(self.data["per_channel"][ch][key] for ch in "ABC"), key)

    def test_removed_duplicates_are_not_negative(self):
        dropped = (self.total["bronze_rows"] - sum(self.total[silver_check.EXPECTED_KEY[t]] for t in silver_check.EVENT_TABLES)
                   - self.total["quarantine"])
        self.assertGreaterEqual(dropped, 0)

    def test_total_includes_fixture_expected(self):
        """덤프 구간 값은 파일에 따로 없어 total == 덤프 + 픽스처는 검사하지 못한다. 대신 total이 픽스처 기대값 이상인지 본다."""
        for key in ("bronze_rows", *DATA_TABLES):
            self.assertGreaterEqual(self.total[key], EXPECTED[key], key)

    def test_dump_adds_no_quarantine(self):
        """check_silver의 격리 사유 검사(픽스처 채널마다 3종 1건)가 합친 데이터에서도 성립하려면 덤프 구간 격리가 0이어야 한다."""
        self.assertEqual(EXPECTED["quarantine"], self.total["quarantine"])

    def test_expected_counts_pass(self):
        self.assertEqual([], silver_check.compare_counts(self.good, self.total))
        self.assertEqual([], silver_check.compare_bronze_balance(self.total["bronze_rows"], self.good, self.total))

    def test_wrong_count_is_reported(self):
        self.assertEqual(1, len(silver_check.compare_counts({**self.good, "payments": self.good["payments"] + 1}, self.total)))

    def test_wrong_duplicate_count_is_reported(self):
        self.assertEqual(1, len(silver_check.compare_bronze_balance(self.total["bronze_rows"] + 1, self.good, self.total)))
        self.assertEqual(1, len(silver_check.compare_bronze_balance(self.total["bronze_rows"], {**self.good, "orders": self.good["orders"] - 1}, self.total)))

    def test_wrong_quarantine_is_reported(self):
        self.assertEqual(1, len(silver_check.compare_counts({**self.good, "quarantine": 8}, self.total)))


if __name__ == "__main__":
    unittest.main()
