import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "01_contracts" / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import _events as ev
import channel_a


class TestChannelA(unittest.TestCase):
    def test_order_created(self):
        self.assertEqual(channel_a.to_raw(ev.created("A")), {
            "event_id": "A-S01-1-E1", "event_type": "order.created",
            "occurred_at": "2026-10-12T01:00:00.000Z", "order_id": "A-S01-1", "user_id": "U01",
            "shipping_fee": 0,
            "items": [{"order_item_id": "A-S01-1-1", "sku": "SKU-004", "product_name": "러닝화",
                       "category": "신발", "quantity": 1, "unit_price": 89000,
                       "discount_amount": 0}]})

    def test_logged_out_user_is_null(self):
        self.assertIsNone(channel_a.to_raw(ev.created("A", user_id=None))["user_id"])

    def test_payment_succeeded_has_no_failure_field(self):
        self.assertEqual(channel_a.to_raw(ev.payment_ok("A")), {
            "event_id": "A-S01-1-E2", "event_type": "payment.succeeded",
            "occurred_at": "2026-10-12T01:00:30.000Z", "order_id": "A-S01-1", "user_id": "U01",
            "payment_id": "A-S01-1-P1", "method": "card", "amount": 89000})

    def test_payment_failed(self):
        raw = channel_a.to_raw(ev.payment_failed("A"))
        self.assertEqual(raw["event_type"], "payment.failed")
        self.assertEqual(raw["failure_reason"], "limit_exceeded")

    def test_order_cancelled(self):
        raw = channel_a.to_raw(ev.cancelled("A"))
        self.assertEqual(raw["event_type"], "order.cancelled")
        self.assertEqual(raw["reason"], "change_of_mind")

    def test_refund_succeeded(self):
        raw = channel_a.to_raw(ev.refund("A"))
        self.assertEqual(raw["event_type"], "refund.succeeded")
        self.assertEqual(raw["refund_id"], "A-S09-1-R1")
        self.assertEqual(raw["payment_id"], "A-S09-1-P1")
        self.assertEqual(raw["order_item_id"], "A-S09-1-1")
        self.assertEqual((raw["amount"], raw["type"], raw["reason"]), (35000, "return", "defect"))

    def test_refund_quantity(self):
        self.assertEqual(channel_a.to_raw(ev.refund("A"))["refund_quantity"], 1)


if __name__ == "__main__":
    unittest.main()
