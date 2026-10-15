"""silver_row_to_event.py 테스트. 고정 입력 72줄(A·B·C 각 24줄) 전체를 변환한다."""
import json
import sys
import unittest
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

from conversion_error import ConversionError
from silver_row_to_event import to_common_event

FIXTURES = ROOT / "01_contracts" / "data_fixtures"


def bronze_lines(channel):
    """Function이 만든 봉투 한 줄 = {"channel", "payload"} (Bronze 행과 같은 두 값)."""
    text = (FIXTURES / channel / "events.jsonl").read_text(encoding="utf-8")
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def reason_of(channel, payload):
    """변환 오류의 reason_code를 돌려준다. 오류가 없으면 None."""
    try:
        to_common_event(channel, payload)
    except ConversionError as exc:
        return exc.reason_code
    return None


class TestBadInput(unittest.TestCase):
    def test_unknown_channel(self):
        for channel in ("X", "a", "", None):
            self.assertEqual(reason_of(channel, "{}"), "UNKNOWN_CHANNEL")

    def test_unknown_channel_wins_over_bad_payload(self):
        self.assertEqual(reason_of("X", "not json"), "UNKNOWN_CHANNEL")

    def test_malformed_json(self):
        for payload in ("not json", "", "{\"a\":", None):
            self.assertEqual(reason_of("A", payload), "MALFORMED_JSON")

    def test_json_but_not_object(self):
        for payload in ("[]", "123", "\"text\"", "null"):
            self.assertEqual(reason_of("A", payload), "MALFORMED_JSON")

    def test_channel_conversion_error_passes_through(self):
        self.assertEqual(reason_of("A", "{}"), "MISSING_REQUIRED")

    def test_invalid_value_from_validation(self):
        line = bronze_lines("A")[1]            # A-S01-1-E2 결제 성공
        payload = json.loads(line["payload"])
        payload["amount"] = -1
        self.assertEqual(reason_of("A", json.dumps(payload)), "INVALID_VALUE")

    def test_invalid_value_zero_quantity(self):
        line = bronze_lines("B")[0]            # B-S01-1-E1 주문 생성
        payload = json.loads(line["payload"])
        payload["orderItems"][0]["qty"] = 0
        self.assertEqual(reason_of("B", json.dumps(payload)), "INVALID_VALUE")


class TestFixtures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.events, cls.reasons = {}, {}
        for channel in ("A", "B", "C"):
            events, reasons = [], []
            for line in bronze_lines(channel):
                try:
                    events.append(to_common_event(line["channel"], line["payload"]))
                except ConversionError as exc:
                    reasons.append(exc.reason_code)
            cls.events[channel], cls.reasons[channel] = events, reasons

    def test_payment_amount_equals_items_plus_shipping(self):
        """성공 결제금액 = 그 주문의 상품 결제금액 합 + 배송비 (금액 정합)."""
        for channel in ("A", "B", "C"):
            orders = {e["order_id"]: e for e in self.events[channel] if e["event_type"] == "ORDER_CREATED"}
            for event in self.events[channel]:
                if event["event_type"] != "PAYMENT_SUCCEEDED" or event["order_id"] not in orders:
                    continue
                order = orders[event["order_id"]]
                items_paid = sum(i["unit_price"] * i["quantity"] - i["item_discount"] for i in order["items"])
                self.assertEqual(event["payment_amount"], items_paid + order["shipping_fee"], event["payment_id"])

    def test_channel_column_is_set(self):
        for channel in ("A", "B", "C"):
            self.assertEqual({e["channel"] for e in self.events[channel]}, {channel})


if __name__ == "__main__":
    unittest.main()
