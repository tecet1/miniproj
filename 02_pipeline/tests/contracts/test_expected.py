import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "01_contracts" / "src"))
CONTRACTS = ROOT / "01_contracts" / "contracts"

import json

import expected

KST = "+09:00"


def load(name):
    return json.loads((CONTRACTS / name).read_text(encoding="utf-8"))


class TestExpectedRules(unittest.TestCase):
    """합성 시나리오로 expected.py의 규칙(이상 주문 제외, KST 일자)을 확인한다."""

    SKUS = [{"product_id": "SKU-001", "unit_price": 29000}]

    def compute(self, start_kst, orders):
        scenario = {"id": "X", "start_kst": start_kst, "orders": orders}
        return expected.compute_expected([scenario], self.SKUS)["per_channel"]["A"]

    @staticmethod
    def order(events, user="U1"):
        return {"order_no": 1, "user_id": user, "items": [{"sku": "SKU-001", "quantity": 2, "discount": 0}],
                "events": events}

    def test_cancelled_and_paid_order_is_excluded_from_gold(self):
        events = [{"type": "ORDER_CREATED", "offset_sec": 0},
                  {"type": "ORDER_CANCELLED", "offset_sec": 10, "reason": "OTHER"},
                  {"type": "PAYMENT_SUCCEEDED", "offset_sec": 20, "method": "CARD"},
                  {"type": "REFUND_SUCCEEDED", "offset_sec": 30, "item": 1, "quantity": 1,
                   "refund_type": "CANCEL", "reason": "OTHER"}]
        paid = self.order([{"type": "ORDER_CREATED", "offset_sec": 0},
                           {"type": "PAYMENT_SUCCEEDED", "offset_sec": 20, "method": "CARD"}], user="U2")
        m = self.compute("2026-10-12T10:00:00+09:00", [self.order(events), paid])
        self.assertEqual((m["fact_sales_item"], m["fact_refund"], m["paying_customers"]), (1, 0, 1))
        self.assertEqual((m["silver_refunds"], m["silver_order_cancellations"]), (1, 1))  # Silver에는 남는다

    def test_payment_date_uses_kst_not_utc(self):
        """KST 00:30은 UTC 전날 15:30이다. 일자는 KST로 센다."""
        paid = self.order([{"type": "ORDER_CREATED", "offset_sec": 0},
                           {"type": "PAYMENT_SUCCEEDED", "offset_sec": 30, "method": "CARD"}])
        m = self.compute("2026-10-12T00:29:30" + KST, [paid])
        self.assertEqual(m["sales_item_by_payment_date"], {"2026-10-12": 1})

    def test_refund_date_can_differ_from_payment_date(self):
        events = [{"type": "ORDER_CREATED", "offset_sec": 0},
                  {"type": "PAYMENT_SUCCEEDED", "offset_sec": 30, "method": "CARD"},
                  {"type": "REFUND_SUCCEEDED", "offset_sec": 86400, "item": 1, "quantity": 1,
                   "refund_type": "CANCEL", "reason": "DEFECT"}]
        m = self.compute("2026-10-12T23:00:00" + KST, [self.order(events)])
        self.assertEqual(m["refund_by_payment_date"], {"2026-10-12": 1})
        self.assertEqual(m["refund_by_refund_date"], {"2026-10-13": 1})


if __name__ == "__main__":
    unittest.main()
