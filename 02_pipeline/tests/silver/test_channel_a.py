"""채널 A 변환(channels/channel_a.py) 테스트.

기대값은 이 파일에 직접 적은 값(이벤트 명세 2절 매핑)과 `expected.json` 건수다. 변환 코드의 상수를 가져다 쓰지 않는다.
"""
import copy
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

from channels import channel_a
from conversion_error import ConversionError

FIXTURE = ROOT / "01_contracts" / "data" / "fixtures" / "A" / "events.jsonl"
EXPECTED = ROOT / "01_contracts" / "data" / "fixtures" / "expected" / "expected.json"


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


ORDER_RAW = {
    "event_id": "A-S01-1-E1", "event_type": "order.created", "occurred_at": "2026-10-12T01:00:00.000Z",
    "order_id": "A-S01-1", "user_id": "U01", "shipping_fee": 0,
    "items": [{"order_item_id": "A-S01-1-1", "sku": "SKU-004", "product_name": "러닝화", "category": "신발",
               "quantity": 1, "unit_price": 89000, "discount_amount": 0}],
}
PAY_OK_RAW = {
    "event_id": "A-S01-1-E2", "event_type": "payment.succeeded", "occurred_at": "2026-10-12T01:00:30.000Z",
    "order_id": "A-S01-1", "user_id": "U01", "payment_id": "A-S01-1-P1", "method": "card", "amount": 89000,
}
PAY_FAIL_RAW = {
    "event_id": "A-S06-1-E2", "event_type": "payment.failed", "occurred_at": "2026-10-12T01:30:20.000Z",
    "order_id": "A-S06-1", "user_id": "U04", "payment_id": "A-S06-1-P1", "method": "card", "amount": 59000,
    "failure_reason": "limit_exceeded",
}
CANCEL_RAW = {
    "event_id": "A-S08-1-E2", "event_type": "order.cancelled", "occurred_at": "2026-10-12T01:40:30.000Z",
    "order_id": "A-S08-1", "user_id": "U05", "reason": "change_of_mind",
}
REFUND_RAW = {
    "event_id": "A-S09-1-E3", "event_type": "refund.succeeded", "occurred_at": "2026-10-12T02:00:30.000Z",
    "order_id": "A-S09-1", "user_id": "U06", "refund_id": "A-S09-1-R1", "payment_id": "A-S09-1-P1",
    "order_item_id": "A-S09-1-1", "refund_quantity": 1, "amount": 35000, "type": "return", "reason": "defect",
}


class TestToEvent(unittest.TestCase):
    def test_order_created(self):
        self.assertEqual(channel_a.to_event(ORDER_RAW), {
            "event_id": "A-S01-1-E1", "event_type": "ORDER_CREATED", "channel": "A",
            "order_id": "A-S01-1", "user_id": "U01", "event_time": utc(2026, 10, 12, 1, 0, 0),
            "shipping_fee": 0,
            "items": [{"order_item_id": "A-S01-1-1", "product_id": "SKU-004", "product_name": "러닝화",
                       "category": "신발", "quantity": 1, "unit_price": 89000, "item_discount": 0}],
        })

    def test_payment_succeeded(self):
        self.assertEqual(channel_a.to_event(PAY_OK_RAW), {
            "event_id": "A-S01-1-E2", "event_type": "PAYMENT_SUCCEEDED", "channel": "A",
            "order_id": "A-S01-1", "user_id": "U01", "event_time": utc(2026, 10, 12, 1, 0, 30),
            "payment_id": "A-S01-1-P1", "payment_method": "CARD", "payment_amount": 89000,
            "fail_reason_code": None,
        })

    def test_payment_failed(self):
        event = channel_a.to_event(PAY_FAIL_RAW)
        self.assertEqual(event["event_type"], "PAYMENT_FAILED")
        self.assertEqual(event["fail_reason_code"], "LIMIT_EXCEEDED")
        self.assertEqual(event["payment_amount"], 59000)

    def test_order_cancelled(self):
        self.assertEqual(channel_a.to_event(CANCEL_RAW), {
            "event_id": "A-S08-1-E2", "event_type": "ORDER_CANCELLED", "channel": "A",
            "order_id": "A-S08-1", "user_id": "U05", "event_time": utc(2026, 10, 12, 1, 40, 30),
            "cancel_reason_code": "CHANGE_OF_MIND",
        })

    def test_refund_succeeded(self):
        self.assertEqual(channel_a.to_event(REFUND_RAW), {
            "event_id": "A-S09-1-E3", "event_type": "REFUND_SUCCEEDED", "channel": "A",
            "order_id": "A-S09-1", "user_id": "U06", "event_time": utc(2026, 10, 12, 2, 0, 30),
            "refund_id": "A-S09-1-R1", "payment_id": "A-S09-1-P1", "order_item_id": "A-S09-1-1",
            "refund_quantity": 1, "refund_amount": 35000, "refund_type": "RETURN",
            "refund_reason_code": "DEFECT",
        })

    def test_all_payment_methods(self):
        expected = {"card": "CARD", "easy_pay": "EASY_PAY", "bank": "BANK_TRANSFER", "vbank": "VIRTUAL_ACCOUNT"}
        for code, common in expected.items():
            raw = {**PAY_OK_RAW, "method": code}
            self.assertEqual(channel_a.to_event(raw)["payment_method"], common)

    def test_logged_out_user_is_none(self):
        raw = {**PAY_OK_RAW, "user_id": None}
        self.assertIsNone(channel_a.to_event(raw)["user_id"])

    def test_unmapped_reason_codes_become_other(self):
        self.assertEqual(channel_a.to_event({**CANCEL_RAW, "reason": "no_such"})["cancel_reason_code"], "OTHER")
        self.assertEqual(channel_a.to_event({**PAY_FAIL_RAW, "failure_reason": "no_such"})["fail_reason_code"], "OTHER")
        self.assertEqual(channel_a.to_event({**REFUND_RAW, "reason": "no_such"})["refund_reason_code"], "OTHER")

    def test_input_is_not_modified(self):
        before = copy.deepcopy(ORDER_RAW)
        channel_a.to_event(ORDER_RAW)
        self.assertEqual(ORDER_RAW, before)


class TestErrors(unittest.TestCase):
    def reason(self, raw):
        with self.assertRaises(ConversionError) as ctx:
            channel_a.to_event(raw)
        return ctx.exception.reason_code

    def test_missing_required(self):
        raw = {k: v for k, v in PAY_OK_RAW.items() if k != "amount"}
        self.assertEqual(self.reason(raw), "MISSING_REQUIRED")

    def test_missing_event_id(self):
        raw = {k: v for k, v in PAY_OK_RAW.items() if k != "event_id"}
        self.assertEqual(self.reason(raw), "MISSING_REQUIRED")

    def test_invalid_type_item_quantity(self):
        raw = copy.deepcopy(ORDER_RAW)
        raw["items"][0]["quantity"] = "N/A"
        self.assertEqual(self.reason(raw), "INVALID_TYPE")

    def test_invalid_type_amount(self):
        self.assertEqual(self.reason({**PAY_OK_RAW, "amount": "89000"}), "INVALID_TYPE")

    def test_invalid_type_time(self):
        self.assertEqual(self.reason({**PAY_OK_RAW, "occurred_at": "yesterday"}), "INVALID_TYPE")

    def test_unknown_event_type(self):
        self.assertEqual(self.reason({**PAY_OK_RAW, "event_type": "shipping.started"}), "UNKNOWN_EVENT_TYPE")


class TestFixtures(unittest.TestCase):
    """고정 입력 A 24건 전체를 변환해 expected.json 건수와 비교한다."""

    @classmethod
    def setUpClass(cls):
        cls.expected = json.loads(EXPECTED.read_text(encoding="utf-8"))["per_channel"]["A"]
        lines = FIXTURE.read_text(encoding="utf-8").splitlines()
        cls.payloads = [json.loads(json.loads(line)["payload"]) for line in lines if line.strip()]
        cls.events, cls.errors = [], []
        for payload in cls.payloads:
            try:
                cls.events.append(channel_a.to_event(payload))
            except ConversionError as exc:
                cls.errors.append(exc.reason_code)

    def count(self, event_type):
        ids = {e["event_id"] for e in self.events if e["event_type"] == event_type}
        return len(ids)

    def test_bronze_row_count(self):
        self.assertEqual(len(self.payloads), self.expected["bronze_rows"])

    def test_quarantine_reasons(self):
        self.assertEqual(len(self.errors), self.expected["quarantine"])
        self.assertEqual(sorted(self.errors), ["INVALID_TYPE", "MISSING_REQUIRED", "UNKNOWN_EVENT_TYPE"])

    def test_counts_by_event_type(self):
        self.assertEqual(self.count("ORDER_CREATED"), self.expected["silver_orders"])
        self.assertEqual(self.count("ORDER_CANCELLED"), self.expected["silver_order_cancellations"])
        self.assertEqual(self.count("REFUND_SUCCEEDED"), self.expected["silver_refunds"])
        payments = self.count("PAYMENT_SUCCEEDED") + self.count("PAYMENT_FAILED")
        self.assertEqual(payments, self.expected["silver_payments"])

    def test_order_item_count(self):
        items = {i["order_item_id"] for e in self.events if e["event_type"] == "ORDER_CREATED" for i in e["items"]}
        self.assertEqual(len(items), self.expected["silver_order_items"])

    def test_duplicate_is_same_event(self):
        """S19 중복 두 줄은 변환 결과가 같다. 제거는 공통 처리의 몫이다."""
        dup = [e for e in self.events if e["event_id"] == "A-S19-1-E2"]
        self.assertEqual(len(dup), 2)
        self.assertEqual(dup[0], dup[1])

    def test_kst_midnight_payment_time(self):
        """S23: 결제 시각은 UTC 2026-10-11T15:30:00Z (KST 10-12 00:30)."""
        event = next(e for e in self.events if e["event_id"] == "A-S23-1-E2")
        self.assertEqual(event["event_time"], utc(2026, 10, 11, 15, 30, 0))


if __name__ == "__main__":
    unittest.main()
