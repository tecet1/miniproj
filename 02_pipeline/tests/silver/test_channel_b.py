"""채널 B 변환(channels/channel_b.py) 테스트.

기대값은 이 파일에 직접 적은 값(이벤트 명세 2절 매핑)이다. 변환 코드의 상수를 가져다 쓰지 않는다.
"""
import copy
import json
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

from channels import channel_b
from conversion_error import ConversionError

FIXTURE = ROOT / "01_contracts" / "data_fixtures" / "B" / "events.jsonl"


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


ORDER_RAW = {
    "eventId": "B-S01-1-E1", "eventName": "ORDER_PLACED", "occurredAtMs": 1791766800000, "orderNo": "B-S01-1",
    "buyer": {"memberId": "U01"}, "delivery": {"fee": 0},
    "orderItems": [{"lineId": "B-S01-1-1", "sellerSku": "SKU-004", "itemName": "러닝화", "categoryName": "신발",
                    "qty": 1, "unitPrice": 89000, "discount": {"total": 0}}],
}
PAY_OK_RAW = {
    "eventId": "B-S01-1-E2", "eventName": "PAY_APPROVED", "occurredAtMs": 1791766830000, "orderNo": "B-S01-1",
    "buyer": {"memberId": "U01"}, "paymentId": "B-S01-1-P1", "payMethod": "CARD", "paidAmount": 89000,
}
PAY_FAIL_RAW = {
    "eventId": "B-S06-1-E2", "eventName": "PAY_DECLINED", "occurredAtMs": 1791768620000, "orderNo": "B-S06-1",
    "buyer": {"memberId": "U04"}, "paymentId": "B-S06-1-P1", "payMethod": "CARD", "paidAmount": 59000,
    "declineCode": "LIMIT",
}
CANCEL_RAW = {
    "eventId": "B-S08-1-E2", "eventName": "ORDER_CANCELED", "occurredAtMs": 1791769230000, "orderNo": "B-S08-1",
    "buyer": {"memberId": "U05"}, "cancelReason": "BUYER_REMORSE",
}
REFUND_RAW = {
    "eventId": "B-S09-1-E3", "eventName": "REFUND_DONE", "occurredAtMs": 1791770430000, "orderNo": "B-S09-1",
    "buyer": {"memberId": "U06"}, "refundId": "B-S09-1-R1", "paymentId": "B-S09-1-P1", "lineId": "B-S09-1-1",
    "refundQty": 1, "refundAmount": 35000, "claimType": "RETURN", "claimReason": "DEFECTIVE",
}


class TestToEvent(unittest.TestCase):
    def test_order_created(self):
        self.assertEqual(channel_b.to_event(ORDER_RAW), {
            "event_id": "B-S01-1-E1", "event_type": "ORDER_CREATED", "channel": "B",
            "order_id": "B-S01-1", "user_id": "U01", "event_time": utc(2026, 10, 12, 1, 0, 0),
            "shipping_fee": 0,
            "items": [{"order_item_id": "B-S01-1-1", "product_id": "SKU-004", "product_name": "러닝화",
                       "category": "신발", "quantity": 1, "unit_price": 89000, "item_discount": 0}],
        })

    def test_payment_succeeded(self):
        self.assertEqual(channel_b.to_event(PAY_OK_RAW), {
            "event_id": "B-S01-1-E2", "event_type": "PAYMENT_SUCCEEDED", "channel": "B",
            "order_id": "B-S01-1", "user_id": "U01", "event_time": utc(2026, 10, 12, 1, 0, 30),
            "payment_id": "B-S01-1-P1", "payment_method": "CARD", "payment_amount": 89000,
            "fail_reason_code": None,
        })

    def test_payment_failed(self):
        event = channel_b.to_event(PAY_FAIL_RAW)
        self.assertEqual(event["event_type"], "PAYMENT_FAILED")
        self.assertEqual(event["fail_reason_code"], "LIMIT_EXCEEDED")
        self.assertEqual(event["payment_amount"], 59000)

    def test_order_cancelled(self):
        self.assertEqual(channel_b.to_event(CANCEL_RAW), {
            "event_id": "B-S08-1-E2", "event_type": "ORDER_CANCELLED", "channel": "B",
            "order_id": "B-S08-1", "user_id": "U05", "event_time": utc(2026, 10, 12, 1, 40, 30),
            "cancel_reason_code": "CHANGE_OF_MIND",
        })

    def test_refund_succeeded(self):
        self.assertEqual(channel_b.to_event(REFUND_RAW), {
            "event_id": "B-S09-1-E3", "event_type": "REFUND_SUCCEEDED", "channel": "B",
            "order_id": "B-S09-1", "user_id": "U06", "event_time": utc(2026, 10, 12, 2, 0, 30),
            "refund_id": "B-S09-1-R1", "payment_id": "B-S09-1-P1", "order_item_id": "B-S09-1-1",
            "refund_quantity": 1, "refund_amount": 35000, "refund_type": "RETURN",
            "refund_reason_code": "DEFECT",
        })

    def test_all_payment_methods(self):
        expected = {"CARD": "CARD", "SIMPLE": "EASY_PAY", "TRANSFER": "BANK_TRANSFER", "VBANK": "VIRTUAL_ACCOUNT"}
        for code, common in expected.items():
            raw = {**PAY_OK_RAW, "payMethod": code}
            self.assertEqual(channel_b.to_event(raw)["payment_method"], common)

    def test_logged_out_user_is_none(self):
        without_buyer = {k: v for k, v in PAY_OK_RAW.items() if k != "buyer"}
        without_member = {**PAY_OK_RAW, "buyer": {}}
        self.assertIsNone(channel_b.to_event(without_buyer)["user_id"])
        self.assertIsNone(channel_b.to_event(without_member)["user_id"])

    def test_unmapped_codes_become_other(self):
        self.assertEqual(channel_b.to_event({**PAY_OK_RAW, "payMethod": "crypto"})["payment_method"], "OTHER")
        self.assertEqual(channel_b.to_event({**CANCEL_RAW, "cancelReason": "no_such"})["cancel_reason_code"], "OTHER")
        self.assertEqual(channel_b.to_event({**PAY_FAIL_RAW, "declineCode": "no_such"})["fail_reason_code"], "OTHER")
        self.assertEqual(channel_b.to_event({**REFUND_RAW, "claimReason": "no_such"})["refund_reason_code"], "OTHER")

    def test_input_is_not_modified(self):
        before = copy.deepcopy(ORDER_RAW)
        channel_b.to_event(ORDER_RAW)
        self.assertEqual(ORDER_RAW, before)


class TestErrors(unittest.TestCase):
    def reason(self, raw):
        with self.assertRaises(ConversionError) as ctx:
            channel_b.to_event(raw)
        return ctx.exception.reason_code

    def test_missing_required(self):
        raw = {k: v for k, v in PAY_OK_RAW.items() if k != "paidAmount"}
        self.assertEqual(self.reason(raw), "MISSING_REQUIRED")

    def test_missing_nested_object(self):
        raw = {k: v for k, v in ORDER_RAW.items() if k != "delivery"}
        self.assertEqual(self.reason(raw), "MISSING_REQUIRED")

    def test_missing_nested_field(self):
        self.assertEqual(self.reason({**ORDER_RAW, "delivery": {}}), "MISSING_REQUIRED")

    def test_missing_item_discount(self):
        raw = copy.deepcopy(ORDER_RAW)
        del raw["orderItems"][0]["discount"]
        self.assertEqual(self.reason(raw), "MISSING_REQUIRED")

    def test_invalid_type_item_quantity(self):
        raw = copy.deepcopy(ORDER_RAW)
        raw["orderItems"][0]["qty"] = "N/A"
        self.assertEqual(self.reason(raw), "INVALID_TYPE")

    def test_invalid_type_nested_object(self):
        self.assertEqual(self.reason({**ORDER_RAW, "delivery": "free"}), "INVALID_TYPE")

    def test_invalid_type_time(self):
        self.assertEqual(self.reason({**PAY_OK_RAW, "occurredAtMs": "1791766830000"}), "INVALID_TYPE")

    def test_unmapped_refund_type_is_invalid_value(self):
        self.assertEqual(self.reason({**REFUND_RAW, "claimType": "EXCHANGE"}), "INVALID_VALUE")

    def test_unknown_event_type(self):
        self.assertEqual(self.reason({**PAY_OK_RAW, "eventName": "SHIP_STARTED"}), "UNKNOWN_EVENT_TYPE")


class TestFixtures(unittest.TestCase):
    """고정 입력 B 24건 전체를 변환한다."""

    @classmethod
    def setUpClass(cls):
        lines = FIXTURE.read_text(encoding="utf-8").splitlines()
        cls.payloads = [json.loads(json.loads(line)["payload"]) for line in lines if line.strip()]
        cls.events, cls.errors = [], []
        for payload in cls.payloads:
            try:
                cls.events.append(channel_b.to_event(payload))
            except ConversionError as exc:
                cls.errors.append(exc.reason_code)

    def count(self, event_type):
        ids = {e["event_id"] for e in self.events if e["event_type"] == event_type}
        return len(ids)

    def test_duplicate_is_same_event(self):
        """S19 중복 두 줄은 변환 결과가 같다. 제거는 공통 처리의 몫이다."""
        dup = [e for e in self.events if e["event_id"] == "B-S19-1-E2"]
        self.assertEqual(len(dup), 2)
        self.assertEqual(dup[0], dup[1])

    def test_kst_midnight_payment_time(self):
        """S23: 결제 시각은 UTC 2026-10-11T15:30:00Z (KST 10-12 00:30)."""
        event = next(e for e in self.events if e["event_id"] == "B-S23-1-E2")
        self.assertEqual(event["event_time"], utc(2026, 10, 11, 15, 30, 0))


if __name__ == "__main__":
    unittest.main()
