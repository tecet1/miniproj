"""validate_values.py 테스트. 기대값은 이벤트 명세 4절의 INVALID_VALUE 조건을 이 파일에 직접 적은 것이다."""
import copy
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

from conversion_error import ConversionError
from validate_values import validate_event_values

ORDER = {
    "event_type": "ORDER_CREATED", "shipping_fee": 4000,
    "items": [{"order_item_id": "X-1", "quantity": 2, "unit_price": 29000, "item_discount": 5000}],
}
PAYMENT = {"event_type": "PAYMENT_SUCCEEDED", "payment_amount": 12000}
REFUND = {"event_type": "REFUND_SUCCEEDED", "refund_quantity": 1, "refund_amount": 35000}
CANCEL = {"event_type": "ORDER_CANCELLED", "cancel_reason_code": "OTHER"}


def order_with(**item_changes):
    event = copy.deepcopy(ORDER)
    event["items"][0].update(item_changes)
    return event


class TestValid(unittest.TestCase):
    def test_normal_events_pass(self):
        for event in (ORDER, PAYMENT, {**PAYMENT, "event_type": "PAYMENT_FAILED"}, REFUND, CANCEL):
            self.assertIsNone(validate_event_values(event))

    def test_zero_amounts_are_allowed(self):
        self.assertIsNone(validate_event_values({**ORDER, "shipping_fee": 0}))
        self.assertIsNone(validate_event_values(order_with(item_discount=0)))
        self.assertIsNone(validate_event_values({**PAYMENT, "payment_amount": 0}))
        self.assertIsNone(validate_event_values({**REFUND, "refund_amount": 0}))

    def test_discount_equal_to_gross_is_allowed(self):
        self.assertIsNone(validate_event_values(order_with(item_discount=58000)))   # 29000 x 2


class TestInvalid(unittest.TestCase):
    def assert_invalid(self, event):
        with self.assertRaises(ConversionError) as ctx:
            validate_event_values(event)
        self.assertEqual(ctx.exception.reason_code, "INVALID_VALUE")

    def test_negative_shipping_fee(self):
        self.assert_invalid({**ORDER, "shipping_fee": -1})

    def test_item_quantity_zero_or_negative(self):
        self.assert_invalid(order_with(quantity=0))
        self.assert_invalid(order_with(quantity=-1))

    def test_negative_unit_price(self):
        self.assert_invalid(order_with(unit_price=-1))

    def test_negative_item_discount(self):
        self.assert_invalid(order_with(item_discount=-1))

    def test_discount_exceeds_gross(self):
        self.assert_invalid(order_with(item_discount=58001))

    def test_second_item_is_checked_too(self):
        event = copy.deepcopy(ORDER)
        event["items"].append({"order_item_id": "X-2", "quantity": 0, "unit_price": 1000, "item_discount": 0})
        self.assert_invalid(event)

    def test_negative_payment_amount(self):
        self.assert_invalid({**PAYMENT, "payment_amount": -1})
        self.assert_invalid({"event_type": "PAYMENT_FAILED", "payment_amount": -1})

    def test_refund_quantity_zero_or_negative(self):
        self.assert_invalid({**REFUND, "refund_quantity": 0})
        self.assert_invalid({**REFUND, "refund_quantity": -1})

    def test_negative_refund_amount(self):
        self.assert_invalid({**REFUND, "refund_amount": -1})


if __name__ == "__main__":
    unittest.main()
