"""고정 입력(fixtures) 72행을 convert_row에 넣고 독립 기대값(expected.json)과 대조한다. Spark 없이 도는 로컬 검증이다.

[검증 범위] 행 단위 변환·격리 + 이벤트 단위 중복 제거(여기서는 파이썬으로 같은 규칙을 흉내 낸다).
Spark 변환·MERGE가 같은 결과를 내는지는 Databricks에서 silver_check로 따로 확인한다 (로컬 테스트 통과 != Databricks 성공).
중복 제거 규칙: (channel, event_id)가 같으면 먼저 도착한 행(여기서는 파일의 앞쪽 행)만 남긴다.
"""
import json
import sys
import unittest
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

from convert_row import convert_row  # noqa: E402
from validate_skus import load_known_skus  # noqa: E402

FIXTURES = ROOT / "01_contracts" / "data_fixtures"
KNOWN = load_known_skus(ROOT / "01_contracts" / "contracts" / "skus.json")
EXPECTED = json.loads((FIXTURES / "expected" / "expected.json").read_text(encoding="utf-8"))

TABLE_OF = {"ORDER_CREATED": "orders", "ORDER_CANCELLED": "order_cancellations",
            "PAYMENT_SUCCEEDED": "payments", "PAYMENT_FAILED": "payments", "REFUND_SUCCEEDED": "refunds"}


def run_channel(channel: str) -> dict:
    """채널 하나의 fixtures를 변환하고 중복을 제거해 {kept: [event], quarantine: [reason], dropped: n}을 돌려준다."""
    kept, quarantine, seen, dropped = [], [], set(), 0
    with open(FIXTURES / channel / "events.jsonl", encoding="utf-8") as f:
        for line in f:
            row = json.loads(line)
            result = convert_row(row["channel"], row["payload"], KNOWN)
            if result["reason_code"]:
                quarantine.append(result["reason_code"])
            elif (channel, result["event_id"]) in seen:
                dropped += 1
            else:
                seen.add((channel, result["event_id"]))
                kept.append(json.loads(result["event_json"]))
    return {"kept": kept, "quarantine": quarantine, "dropped": dropped}


def table_counts(kept: list) -> Counter:
    counts = Counter()
    for event in kept:
        counts[TABLE_OF[event["event_type"]]] += 1
        if event["event_type"] == "ORDER_CREATED":
            counts["order_items"] += len(event["items"])
    return counts


class TestCountsAgainstExpected(unittest.TestCase):
    def test_every_channel_matches_expected_counts(self):
        for channel in "ABC":
            run, expected = run_channel(channel), EXPECTED["per_channel"][channel]
            counts = table_counts(run["kept"])
            with self.subTest(channel=channel):
                self.assertEqual(expected["silver_orders"], counts["orders"])
                self.assertEqual(expected["silver_order_items"], counts["order_items"])
                self.assertEqual(expected["silver_payments"], counts["payments"])
                self.assertEqual(expected["silver_order_cancellations"], counts["order_cancellations"])
                self.assertEqual(expected["silver_refunds"], counts["refunds"])
                self.assertEqual(expected["quarantine"], len(run["quarantine"]))

    def test_bronze_rows_are_fully_accounted_for(self):
        """Bronze = 정상 테이블 행 + 격리 + 제거된 중복. 사라진 행이 없어야 한다."""
        for channel in "ABC":
            run = run_channel(channel)
            with self.subTest(channel=channel):
                self.assertEqual(EXPECTED["per_channel"][channel]["bronze_rows"],
                                 len(run["kept"]) + len(run["quarantine"]) + run["dropped"])

    def test_one_duplicate_dropped_per_channel(self):
        for channel in "ABC":
            self.assertEqual(1, run_channel(channel)["dropped"], channel)

    def test_quarantine_reasons_per_channel(self):
        for channel in "ABC":
            self.assertEqual(Counter({"MISSING_REQUIRED": 1, "INVALID_TYPE": 1, "UNKNOWN_EVENT_TYPE": 1}),
                             Counter(run_channel(channel)["quarantine"]), channel)

    def test_total_is_sum_of_channels(self):
        total = Counter()
        for channel in "ABC":
            total.update(table_counts(run_channel(channel)["kept"]))
        self.assertEqual(EXPECTED["total"]["silver_orders"], total["orders"])
        self.assertEqual(EXPECTED["total"]["silver_order_items"], total["order_items"])
        self.assertEqual(EXPECTED["total"]["silver_payments"], total["payments"])


class TestAmounts(unittest.TestCase):
    def test_success_payment_equals_items_plus_shipping(self):
        """결제 성공 금액 = 해당 주문 상품 결제금액 합 + 배송비 (plans/03 완료 조건). 주문 단위로 맞춘다."""
        for channel in "ABC":
            kept = run_channel(channel)["kept"]
            orders = {e["order_id"]: e for e in kept if e["event_type"] == "ORDER_CREATED"}
            for event in kept:
                if event["event_type"] != "PAYMENT_SUCCEEDED":
                    continue
                order = orders[event["order_id"]]
                items_total = sum(i["unit_price"] * i["quantity"] - i["item_discount"] for i in order["items"])
                with self.subTest(order=event["order_id"]):
                    self.assertEqual(items_total + order["shipping_fee"], event["payment_amount"])


if __name__ == "__main__":
    unittest.main()
