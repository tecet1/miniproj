"""batch_check(배치별 "들어온 수 = 반영 + 중복 + 격리")를 pyspark 없이, 그리고 로컬 Spark DataFrame으로 확인한다.

[확인하는 것] 균형 식, 결과 행, 배치 집계(격리·정상·중복·반영 수), 반영이 빠졌을 때의 위반, 재처리 시 같은 결과.
[확인하지 못하는 것] foreachBatch 안의 실제 MERGE 결과와 Delta 테이블 조회. Databricks에서 확인한다 (미검증).
"""
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "02_pipeline" / "quality"), str(ROOT / "02_pipeline" / "tests")]

import batch_check as B  # noqa: E402
from spark_support import local_spark  # noqa: E402

NOW = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)
GOOD = {"bronze_in": 10, "quarantine_found": 2, "unique_events": 6, "events_found": 6, "duplicates": 2}


class TestBalance(unittest.TestCase):
    def test_balanced_batch_has_no_violation(self):
        self.assertEqual(0, B.balance_violations(GOOD))

    def test_each_missing_part_is_a_violation(self):
        self.assertEqual(1, B.balance_violations({**GOOD, "events_found": 5}))          # 반영 누락
        self.assertEqual(2, B.balance_violations({**GOOD, "quarantine_found": 0}))      # 격리 누락
        self.assertEqual(1, B.balance_violations({**GOOD, "duplicates": 1}))            # 중복이 설명되지 않음

    def test_result_row_carries_rule_batch_and_counts(self):
        row = B.batch_result({**GOOD, "events_found": 5}, 7, "batch-1", NOW)
        self.assertEqual(("silver_batch_balance", "silver", "batch", "error", 1, 7),
                         (row["rule_id"], row["stage"], row["mode"], row["status"], row["violations"], row["batch_id"]))
        self.assertIn("events_found=5", row["detail"])
        self.assertEqual("pass", B.batch_result(GOOD, 7, "batch-1", NOW)["status"])


SPARK = local_spark()
CONVERTED_DDL = ("channel string, eh_partition int, eh_offset bigint, "
                 "result struct<event_id:string, reason_code:string>")


def converted(rows):
    """rows = (channel, offset, event_id, reason_code). 파티션은 0으로 둔다."""
    return SPARK.createDataFrame([(ch, 0, off, (eid, reason)) for ch, off, eid, reason in rows], CONVERTED_DDL)


def event_keys(pairs):
    return SPARK.createDataFrame(pairs, "channel string, event_id string")


def quarantine_keys(offsets):
    return SPARK.createDataFrame([(0, o) for o in offsets], "eh_partition int, eh_offset bigint")


# 배치: 정상 A/E1 두 번(중복) + A/E2 + B/E1, 격리 두 건(오프셋 4, 5) = 6행
BATCH = [("A", 0, "E1", None), ("A", 1, "E1", None), ("A", 2, "E2", None), ("B", 3, "E1", None),
         ("A", 4, None, "INVALID_VALUE"), ("B", 5, None, "MISSING_REQUIRED")]
SILVER_OK = [("A", "E1"), ("A", "E2"), ("B", "E1")]


@unittest.skipUnless(SPARK, "pyspark/Java가 없으면 로컬 Spark 검증을 건너뛴다")
class TestBatchCountsOnLocalSpark(unittest.TestCase):
    def counts(self, silver=SILVER_OK, quarantine=(4, 5), batch=BATCH):
        return B.batch_counts(converted(batch), event_keys(silver), quarantine_keys(quarantine))

    def test_counts_split_the_batch_into_events_duplicates_and_quarantine(self):
        self.assertEqual({"bronze_in": 6, "quarantine_found": 2, "unique_events": 3, "events_found": 3, "duplicates": 1},
                         self.counts())

    def test_a_fully_applied_batch_balances(self):
        self.assertEqual(0, B.balance_violations(self.counts()))

    def test_event_missing_from_silver_is_caught(self):
        self.assertEqual(1, B.balance_violations(self.counts(silver=[("A", "E1"), ("B", "E1")])))

    def test_quarantine_row_missing_from_quarantine_is_caught(self):
        self.assertEqual(1, B.balance_violations(self.counts(quarantine=(4,))))

    def test_same_event_id_in_another_channel_is_not_a_match(self):
        self.assertEqual(1, B.balance_violations(self.counts(silver=[("A", "E1"), ("A", "E2"), ("C", "E1")])))

    def test_events_applied_by_an_earlier_batch_still_balance(self):
        # 이미 Silver에 있던 이벤트(재처리·배치 사이 중복)는 같은 결과다
        self.assertEqual(0, B.balance_violations(self.counts(silver=SILVER_OK + [("A", "OLD")])))

    def test_unremoved_duplicate_rows_do_not_hide_a_lost_event(self):
        lost = self.counts(silver=[("A", "E1"), ("A", "E2")])        # B/E1이 반영되지 않았다
        self.assertEqual(1, B.balance_violations(lost))


if __name__ == "__main__":
    unittest.main()
