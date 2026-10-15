"""채널 C 변환(channels/channel_c.py) 테스트.

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

from channels import channel_c
from conversion_error import ConversionError

FIXTURE = ROOT / "01_contracts" / "data_fixtures" / "C" / "events.jsonl"


def utc(*args):
    return datetime(*args, tzinfo=timezone.utc)


ORDER_RAW = {
    "evt_id": "C-S01-1-E1", "evt_cd": "O01", "evt_dt": "2026-10-12 10:00:00", "ord_no": "C-S01-1", "mem_no": "U01",
    "dlv_amt": 0,
    "lines": [{"seq": 1, "goods_cd": "SKU-004", "goods_nm": "러닝화", "cate_nm": "신발", "qty": 1, "price": 89000, "dc_amt": 0}],
}
PAY_OK_RAW = {
    "evt_id": "C-S01-1-E2", "evt_cd": "P01", "evt_dt": "2026-10-12 10:00:30", "ord_no": "C-S01-1", "mem_no": "U01",
    "pay_no": "C-S01-1-P1", "pay_type": "10", "pay_amt": 89000,
}
PAY_FAIL_RAW = {
    "evt_id": "C-S06-1-E2", "evt_cd": "P02", "evt_dt": "2026-10-12 10:30:20", "ord_no": "C-S06-1", "mem_no": "U04",
    "pay_no": "C-S06-1-P1", "pay_type": "10", "pay_amt": 59000, "fail_cd": "F1",
}
CANCEL_RAW = {
    "evt_id": "C-S08-1-E2", "evt_cd": "O09", "evt_dt": "2026-10-12 10:40:30", "ord_no": "C-S08-1", "mem_no": "U05",
    "rsn_cd": "01",
}
REFUND_RAW = {
    "evt_id": "C-S09-1-E3", "evt_cd": "R01", "evt_dt": "2026-10-12 11:00:30", "ord_no": "C-S09-1", "mem_no": "U06",
    "rfd_no": "C-S09-1-R1", "pay_no": "C-S09-1-P1", "seq": 1, "rfd_qty": 1, "rfd_amt": 35000, "clm_tp": "2",
    "rsn_cd": "02",
}


class TestToEvent(unittest.TestCase):
    def test_order_created(self):
        self.assertEqual(channel_c.to_event(ORDER_RAW), {
            "event_id": "C-S01-1-E1", "event_type": "ORDER_CREATED", "channel": "C",
            "order_id": "C-S01-1", "user_id": "U01", "event_time": utc(2026, 10, 12, 1, 0, 0),
            "shipping_fee": 0,
            "items": [{"order_item_id": "C-S01-1-1", "product_id": "SKU-004", "product_name": "러닝화",
                       "category": "신발", "quantity": 1, "unit_price": 89000, "item_discount": 0}],
        })

    def test_payment_succeeded(self):
        self.assertEqual(channel_c.to_event(PAY_OK_RAW), {
            "event_id": "C-S01-1-E2", "event_type": "PAYMENT_SUCCEEDED", "channel": "C",
            "order_id": "C-S01-1", "user_id": "U01", "event_time": utc(2026, 10, 12, 1, 0, 30),
            "payment_id": "C-S01-1-P1", "payment_method": "CARD", "payment_amount": 89000,
            "fail_reason_code": None,
        })

    def test_payment_failed(self):
        event = channel_c.to_event(PAY_FAIL_RAW)
        self.assertEqual(event["event_type"], "PAYMENT_FAILED")
        self.assertEqual(event["fail_reason_code"], "LIMIT_EXCEEDED")
        self.assertEqual(event["payment_amount"], 59000)

    def test_order_cancelled(self):
        self.assertEqual(channel_c.to_event(CANCEL_RAW), {
            "event_id": "C-S08-1-E2", "event_type": "ORDER_CANCELLED", "channel": "C",
            "order_id": "C-S08-1", "user_id": "U05", "event_time": utc(2026, 10, 12, 1, 40, 30),
            "cancel_reason_code": "CHANGE_OF_MIND",
        })

    def test_refund_succeeded(self):
        self.assertEqual(channel_c.to_event(REFUND_RAW), {
            "event_id": "C-S09-1-E3", "event_type": "REFUND_SUCCEEDED", "channel": "C",
            "order_id": "C-S09-1", "user_id": "U06", "event_time": utc(2026, 10, 12, 2, 0, 30),
            "refund_id": "C-S09-1-R1", "payment_id": "C-S09-1-P1", "order_item_id": "C-S09-1-1",
            "refund_quantity": 1, "refund_amount": 35000, "refund_type": "RETURN",
            "refund_reason_code": "DEFECT",
        })

    def test_refund_types(self):
        self.assertEqual(channel_c.to_event({**REFUND_RAW, "clm_tp": "1"})["refund_type"], "CANCEL")
        self.assertEqual(channel_c.to_event({**REFUND_RAW, "clm_tp": "2"})["refund_type"], "RETURN")

    def test_order_item_id_is_order_no_and_seq(self):
        raw = copy.deepcopy(ORDER_RAW)
        raw["lines"].append({**raw["lines"][0], "seq": 2})
        ids = [i["order_item_id"] for i in channel_c.to_event(raw)["items"]]
        self.assertEqual(ids, ["C-S01-1-1", "C-S01-1-2"])

    def test_all_payment_methods(self):
        expected = {"10": "CARD", "20": "EASY_PAY", "30": "BANK_TRANSFER", "40": "VIRTUAL_ACCOUNT"}
        for code, common in expected.items():
            raw = {**PAY_OK_RAW, "pay_type": code}
            self.assertEqual(channel_c.to_event(raw)["payment_method"], common)

    def test_kst_to_utc_crosses_date(self):
        """KST 08:59:59는 전날 UTC 23:59:59다."""
        event = channel_c.to_event({**PAY_OK_RAW, "evt_dt": "2026-10-12 08:59:59"})
        self.assertEqual(event["event_time"], utc(2026, 10, 11, 23, 59, 59))

    def test_logged_out_user_is_none(self):
        without_member = {k: v for k, v in PAY_OK_RAW.items() if k != "mem_no"}
        self.assertIsNone(channel_c.to_event({**PAY_OK_RAW, "mem_no": ""})["user_id"])
        self.assertIsNone(channel_c.to_event(without_member)["user_id"])

    def test_unmapped_codes_become_other(self):
        self.assertEqual(channel_c.to_event({**PAY_OK_RAW, "pay_type": "99"})["payment_method"], "OTHER")
        self.assertEqual(channel_c.to_event({**CANCEL_RAW, "rsn_cd": "99"})["cancel_reason_code"], "OTHER")
        self.assertEqual(channel_c.to_event({**PAY_FAIL_RAW, "fail_cd": "F9"})["fail_reason_code"], "OTHER")
        self.assertEqual(channel_c.to_event({**REFUND_RAW, "rsn_cd": "99"})["refund_reason_code"], "OTHER")

    def test_input_is_not_modified(self):
        before = copy.deepcopy(ORDER_RAW)
        channel_c.to_event(ORDER_RAW)
        self.assertEqual(ORDER_RAW, before)


class TestErrors(unittest.TestCase):
    def reason(self, raw):
        with self.assertRaises(ConversionError) as ctx:
            channel_c.to_event(raw)
        return ctx.exception.reason_code

    def test_missing_required(self):
        raw = {k: v for k, v in PAY_OK_RAW.items() if k != "pay_amt"}
        self.assertEqual(self.reason(raw), "MISSING_REQUIRED")

    def test_missing_refund_seq(self):
        raw = {k: v for k, v in REFUND_RAW.items() if k != "seq"}
        self.assertEqual(self.reason(raw), "MISSING_REQUIRED")

    def test_invalid_type_item_quantity(self):
        raw = copy.deepcopy(ORDER_RAW)
        raw["lines"][0]["qty"] = "N/A"
        self.assertEqual(self.reason(raw), "INVALID_TYPE")

    def test_invalid_type_seq(self):
        raw = copy.deepcopy(ORDER_RAW)
        raw["lines"][0]["seq"] = "1"
        self.assertEqual(self.reason(raw), "INVALID_TYPE")

    def test_invalid_type_time(self):
        for bad in ("yesterday", "2026-10-12T10:00:00", "2026-10-12 10:00"):
            self.assertEqual(self.reason({**PAY_OK_RAW, "evt_dt": bad}), "INVALID_TYPE")

    def test_unmapped_refund_type_is_invalid_value(self):
        self.assertEqual(self.reason({**REFUND_RAW, "clm_tp": "9"}), "INVALID_VALUE")

    def test_unknown_event_type(self):
        self.assertEqual(self.reason({**PAY_OK_RAW, "evt_cd": "D01"}), "UNKNOWN_EVENT_TYPE")


class TestFixtures(unittest.TestCase):
    """고정 입력 C 24건 전체를 변환한다."""

    @classmethod
    def setUpClass(cls):
        lines = FIXTURE.read_text(encoding="utf-8").splitlines()
        cls.payloads = [json.loads(json.loads(line)["payload"]) for line in lines if line.strip()]
        cls.events, cls.errors = [], []
        for payload in cls.payloads:
            try:
                cls.events.append(channel_c.to_event(payload))
            except ConversionError as exc:
                cls.errors.append(exc.reason_code)

    def count(self, event_type):
        ids = {e["event_id"] for e in self.events if e["event_type"] == event_type}
        return len(ids)

    def test_duplicate_is_same_event(self):
        """S19 중복 두 줄은 변환 결과가 같다. 제거는 공통 처리의 몫이다."""
        dup = [e for e in self.events if e["event_id"] == "C-S19-1-E2"]
        self.assertEqual(len(dup), 2)
        self.assertEqual(dup[0], dup[1])

    def test_kst_midnight_payment_time(self):
        """S23: 결제 시각은 UTC 2026-10-11T15:30:00Z (KST 10-12 00:30)."""
        event = next(e for e in self.events if e["event_id"] == "C-S23-1-E2")
        self.assertEqual(event["event_time"], utc(2026, 10, 11, 15, 30, 0))


if __name__ == "__main__":
    unittest.main()
