import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "01_contracts" / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import _events as ev
import channel_b


class TestChannelB(unittest.TestCase):
    def test_order_created(self):
        self.assertEqual(channel_b.to_raw(ev.created("B")), {
            "eventId": "B-S01-1-E1", "eventName": "ORDER_PLACED", "occurredAtMs": 1791766800000,
            "orderNo": "B-S01-1", "buyer": {"memberId": "U01"}, "delivery": {"fee": 0},
            "orderItems": [{"lineId": "B-S01-1-1", "sellerSku": "SKU-004", "itemName": "러닝화",
                            "categoryName": "신발", "qty": 1, "unitPrice": 89000,
                            "discount": {"total": 0}}]})

    def test_logged_out_user_has_no_member_id(self):
        raw = channel_b.to_raw(ev.created("B", user_id=None))
        self.assertEqual(raw["buyer"], {})

    def test_payment_succeeded(self):
        self.assertEqual(channel_b.to_raw(ev.payment_ok("B")), {
            "eventId": "B-S01-1-E2", "eventName": "PAY_APPROVED", "occurredAtMs": 1791766830000,
            "orderNo": "B-S01-1", "buyer": {"memberId": "U01"},
            "paymentId": "B-S01-1-P1", "payMethod": "CARD", "paidAmount": 89000})

    def test_payment_failed(self):
        raw = channel_b.to_raw(ev.payment_failed("B"))
        self.assertEqual(raw["eventName"], "PAY_DECLINED")
        self.assertEqual(raw["declineCode"], "LIMIT")

    def test_order_cancelled(self):
        raw = channel_b.to_raw(ev.cancelled("B"))
        self.assertEqual(raw["eventName"], "ORDER_CANCELED")
        self.assertEqual(raw["cancelReason"], "BUYER_REMORSE")

    def test_refund_succeeded(self):
        raw = channel_b.to_raw(ev.refund("B"))
        self.assertEqual(raw["eventName"], "REFUND_DONE")
        self.assertEqual(raw["refundId"], "B-S09-1-R1")
        self.assertEqual(raw["paymentId"], "B-S09-1-P1")
        self.assertEqual(raw["lineId"], "B-S09-1-1")
        self.assertEqual((raw["refundAmount"], raw["claimType"], raw["claimReason"]),
                         (35000, "RETURN", "DEFECTIVE"))

    @unittest.skip("명세 승인 대기: 환불 수량 필드명 (제안: refundQty)")
    def test_refund_quantity(self):
        self.assertEqual(channel_b.to_raw(ev.refund("B"))["refundQty"], 1)


if __name__ == "__main__":
    unittest.main()
