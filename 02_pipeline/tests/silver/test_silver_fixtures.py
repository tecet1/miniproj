"""고정 입력(fixtures) 72행을 convert_row에 넣고 변환·중복 제거하고 주문 단위 금액 정합성을 확인한다. Spark 없이 도는 로컬 검증이다.

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


class TestAmounts(unittest.TestCase):
    def test_success_payment_equals_items_plus_shipping(self):
        """결제 성공 금액 = 해당 주문 상품 결제금액 합 + 배송비 (plans/03 완료 조건). 주문 단위로 맞춘다."""
        for channel in "ABC":
            kept = run_channel(channel)["kept"]
            orders = {e["order_id"]: e for e in kept if e["event_type"] == "ORDER_CREATED"}
            for event in kept:
                if event["event_type"] != "PAYMENT_SUCCEEDED":
                    continue
                if event["order_id"] not in orders:
                    self.assertIn("-S28-", event["order_id"])     # S28은 주문 생성이 격리되어 결제만 Silver에 남는다
                    continue
                order = orders[event["order_id"]]
                items_total = sum(i["unit_price"] * i["quantity"] - i["item_discount"] for i in order["items"])
                with self.subTest(order=event["order_id"]):
                    self.assertEqual(items_total + order["shipping_fee"], event["payment_amount"])


if __name__ == "__main__":
    unittest.main()
