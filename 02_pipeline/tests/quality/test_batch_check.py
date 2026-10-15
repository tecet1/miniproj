"""batch_check(배치별 "들어온 수 = 반영 + 중복 + 격리")를 pyspark 없이, 그리고 로컬 Spark DataFrame으로 확인한다.

[확인하는 것] 균형 식, 결과 행, 배치 집계(격리·정상·중복·반영 수), 반영이 빠졌을 때의 위반, 재처리 시 같은 결과.
[확인하지 못하는 것] foreachBatch 안의 실제 MERGE 결과와 Delta 테이블 조회. Databricks에서 확인한다 (미검증).
"""
import sys
import unittest
from unittest import mock
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "02_pipeline" / "quality"), str(ROOT / "02_pipeline" / "tests")]

import batch_check as B  # noqa: E402
from spark_support import local_spark  # noqa: E402

NOW = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)
GOOD = {"bronze_in": 10, "quarantine_found": 2, "unique_events": 6, "events_found": 6, "duplicates": 2, "amount_in": 5000, "amount_found": 5000}


class TestEmptyBatches(unittest.TestCase):
    def test_empty_batch_is_not_recorded(self):
        self.assertFalse(B.has_rows(dict.fromkeys(GOOD, 0)))

    def test_batch_with_rows_is_recorded_even_when_everything_was_a_duplicate(self):
        self.assertTrue(B.has_rows({**dict.fromkeys(GOOD, 0), "bronze_in": 1}))

    def test_after_batch_writes_nothing_for_an_empty_batch(self):
        saved = []
        with mock.patch.object(B, "batch_counts", return_value=dict.fromkeys(GOOD, 0)), mock.patch.object(B, "silver_amounts"),              mock.patch.object(B, "silver_event_keys"), mock.patch.object(B, "save_results", side_effect=lambda *a: saved.append(a)):
            B.make_after_batch("s")(mock.Mock(), 3)
            self.assertEqual([], saved)
            with mock.patch.object(B, "batch_counts", return_value=GOOD):
                B.make_after_batch("s")(mock.Mock(), 4)
        self.assertEqual(1, len(saved))
        self.assertEqual(4, saved[0][1][0]["batch_id"])


class TestBalance(unittest.TestCase):
    def test_balanced_batch_has_no_violation(self):
        self.assertEqual(0, B.balance_violations(GOOD))

    def test_each_missing_part_is_a_violation(self):
        self.assertEqual(1, B.balance_violations({**GOOD, "events_found": 5}))          # 반영 누락
        self.assertEqual(2, B.balance_violations({**GOOD, "quarantine_found": 0}))      # 격리 누락
        self.assertEqual(1, B.balance_violations({**GOOD, "duplicates": 1}))            # 중복이 설명되지 않음

    def test_amount_result_row_has_its_own_rule_name(self):
        row = B.amount_result({**GOOD, "amount_found": 4990}, 7, "batch-1", NOW)
        self.assertEqual(("silver_batch_amounts", "error", 10, 7), (row["rule_id"], row["status"], row["violations"], row["batch_id"]))
        self.assertIn("amount_in=5000, amount_found=4990", row["detail"])
        self.assertEqual("pass", B.amount_result(GOOD, 7, "batch-1", NOW)["status"])

    def test_after_batch_records_the_balance_and_the_amount_row(self):
        saved = []
        with mock.patch.object(B, "batch_counts", return_value=GOOD), mock.patch.object(B, "silver_event_keys"),              mock.patch.object(B, "silver_amounts"), mock.patch.object(B, "save_results", side_effect=lambda *a: saved.append(a)):
            B.make_after_batch("s")(mock.Mock(), 4)
        self.assertEqual(["silver_batch_balance", "silver_batch_amounts"], [r["rule_id"] for r in saved[0][1]])

    def test_result_row_carries_rule_batch_and_counts(self):
        row = B.batch_result({**GOOD, "events_found": 5}, 7, "batch-1", NOW)
        self.assertEqual(("silver_batch_balance", "silver", "batch", "error", 1, 7),
                         (row["rule_id"], row["stage"], row["mode"], row["status"], row["violations"], row["batch_id"]))
        self.assertIn("events_found=5", row["detail"])
        self.assertEqual("pass", B.batch_result(GOOD, 7, "batch-1", NOW)["status"])


SPARK = local_spark()
CONVERTED_DDL = ("channel string, eh_partition int, eh_offset bigint, received_at timestamp, "
                 "result struct<event_id:string, reason_code:string>, e struct<payment_amount:bigint, refund_amount:bigint>")
BASE_TIME = datetime(2026, 10, 12, tzinfo=timezone.utc)


def converted(rows):
    """rows = (channel, offset, event_id, reason_code[, 결제금액[, 환불금액]]). 파티션은 0, 수신 시각은 오프셋 순이다."""
    full = [tuple(row) + (None,) * (6 - len(row)) for row in rows]
    return SPARK.createDataFrame([(ch, 0, off, BASE_TIME + timedelta(seconds=off), (eid, reason), (pay, ref))
                                  for ch, off, eid, reason, pay, ref in full], CONVERTED_DDL)


def amounts(rows):
    """rows = (channel, event_id, amount): Silver payments·refunds의 금액."""
    return SPARK.createDataFrame(rows, "channel string, event_id string, amount bigint")


def event_keys(pairs):
    return SPARK.createDataFrame(pairs, "channel string, event_id string")


def quarantine_keys(offsets):
    return SPARK.createDataFrame([(0, o) for o in offsets], "eh_partition int, eh_offset bigint")


# 배치: 정상 A/E1 두 번(중복) + A/E2 + B/E1, 격리 두 건(오프셋 4, 5) = 6행
BATCH = [("A", 0, "E1", None, 1000), ("A", 1, "E1", None, 1000), ("A", 2, "E2", None, None, 300), ("B", 3, "E1", None, 700),
         ("A", 4, None, "INVALID_VALUE"), ("B", 5, None, "MISSING_REQUIRED")]
SILVER_OK = [("A", "E1"), ("A", "E2"), ("B", "E1")]
SILVER_AMOUNTS = [("A", "E1", 1000), ("A", "E2", 300), ("B", "E1", 700)]


@unittest.skipUnless(SPARK, "pyspark/Java가 없으면 로컬 Spark 검증을 건너뛴다")
class TestBatchCountsOnLocalSpark(unittest.TestCase):
    def counts(self, silver=SILVER_OK, quarantine=(4, 5), batch=BATCH, silver_amounts=SILVER_AMOUNTS):
        return B.batch_counts(converted(batch), event_keys(silver), quarantine_keys(quarantine), amounts(silver_amounts))

    def test_counts_split_the_batch_into_events_duplicates_and_quarantine(self):
        self.assertEqual({"bronze_in": 6, "quarantine_found": 2, "unique_events": 3, "events_found": 3, "duplicates": 1,
                          "amount_in": 2000, "amount_found": 2000}, self.counts())

    def test_amounts_count_each_unique_event_once_even_when_it_was_sent_twice(self):
        counts = self.counts()
        self.assertEqual(1000 + 300 + 700, counts["amount_in"])        # E1(A)는 두 줄이지만 한 번만 센다
        self.assertEqual(0, B.amount_violations(counts))

    def test_a_changed_amount_in_silver_is_caught_even_though_the_counts_match(self):
        wrong = self.counts(silver_amounts=[("A", "E1", 1000), ("A", "E2", 301), ("B", "E1", 700)])
        self.assertEqual(0, B.balance_violations(wrong))
        self.assertEqual(1, B.amount_violations(wrong))

    def test_a_missing_event_lowers_the_found_amount(self):
        lost = self.counts(silver_amounts=[("A", "E1", 1000), ("A", "E2", 300)])
        self.assertEqual(700, B.amount_violations(lost))

    def test_reprocessing_the_same_batch_gives_the_same_amounts(self):
        self.assertEqual(self.counts(), self.counts())

    def test_the_first_copy_decides_the_amount_when_a_duplicate_conflicts(self):
        batch = [("A", 0, "E1", None, 1000), ("A", 1, "E1", None, 9999)]
        counts = self.counts(batch=batch, silver=[("A", "E1")], silver_amounts=[("A", "E1", 1000)], quarantine=())
        self.assertEqual((1000, 1000), (counts["amount_in"], counts["amount_found"]))

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
