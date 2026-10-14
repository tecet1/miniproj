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


class TestExpected(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        result = expected.compute_expected(load("scenarios.json")["scenarios"], load("skus.json")["skus"])
        cls.per_channel = result["per_channel"]
        cls.total = result["total"]
        cls.dimensions = result["dimensions"]

    def test_counts_per_channel(self):
        want = {"bronze_rows": 24, "silver_orders": 9, "silver_order_items": 11,
                "silver_payments": 9, "silver_order_cancellations": 1, "silver_refunds": 1,
                "quarantine": 3, "fact_sales_item": 10, "fact_refund": 1}
        for channel in "ABC":
            for key, value in want.items():
                with self.subTest(channel=channel, key=key):
                    self.assertEqual(self.per_channel[channel][key], value)

    def test_counts_total(self):
        want = {"bronze_rows": 72, "quarantine": 9, "fact_sales_item": 30, "fact_refund": 3}
        for key, value in want.items():
            with self.subTest(key=key):
                self.assertEqual(self.total[key], value)

    def test_amounts_per_channel(self):
        for channel in "ABC":
            m = self.per_channel[channel]
            self.assertEqual((m["total_sales"], m["refund_amount"], m["net_sales"]),
                             (422000, 35000, 387000))
            self.assertEqual((m["paid_item_count"], m["paying_customers"]), (10, 6))

    def test_amounts_total(self):
        m = self.total
        self.assertEqual((m["total_sales"], m["refund_amount"], m["net_sales"]),
                         (1266000, 105000, 1161000))
        self.assertEqual((m["paid_item_count"], m["paying_customers"]), (30, 18))

    def test_ratios(self):
        for m in (self.per_channel["A"], self.total):
            self.assertAlmostEqual(m["refund_ratio"], 0.0829, places=4)      # 35,000 / 422,000
            self.assertAlmostEqual(m["unit_price_per_item"], 42200.0, places=2)
            self.assertAlmostEqual(m["average_order_value"], 70333.33, places=2)

    def test_gold_distributions_per_channel(self):
        """Gold 대조용 분포. 모든 이벤트가 KST 2026-10-12이고 S23(UTC 10-11 15:30)이 10-12에 속한다."""
        for channel in "ABC":
            m = self.per_channel[channel]
            with self.subTest(channel=channel):
                self.assertEqual(m["sales_item_by_payment_date"], {"2026-10-12": 10})
                self.assertEqual(m["sales_item_by_payment_method"],
                                 {"BANK_TRANSFER": 1, "CARD": 6, "EASY_PAY": 2, "VIRTUAL_ACCOUNT": 1})
                self.assertEqual(m["refund_by_payment_date"], {"2026-10-12": 1})
                self.assertEqual(m["refund_by_refund_date"], {"2026-10-12": 1})
                self.assertEqual(m["refund_by_profile"], {"RETURN-DEFECT": 1})
                self.assertEqual(m["refund_quantity"], 1)

    def test_gold_distributions_total_and_dimensions(self):
        self.assertEqual(self.total["sales_item_by_payment_date"], {"2026-10-12": 30})
        self.assertEqual(self.total["sales_item_by_payment_method"],
                         {"BANK_TRANSFER": 3, "CARD": 18, "EASY_PAY": 6, "VIRTUAL_ACCOUNT": 3})
        self.assertEqual(self.total["refund_by_profile"], {"RETURN-DEFECT": 3})
        self.assertEqual(self.total["refund_quantity"], 3)
        self.assertEqual(self.dimensions, {"dim_channel": 3, "dim_payment_method": 5, "dim_refund_profile": 10,
                                           "dim_product": 15})


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
