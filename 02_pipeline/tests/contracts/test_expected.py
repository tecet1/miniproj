import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "01_contracts" / "src"))
CONTRACTS = ROOT / "01_contracts" / "contracts"

import json

import expected


def load(name):
    return json.loads((CONTRACTS / name).read_text(encoding="utf-8"))


class TestExpected(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        result = expected.compute_expected(load("scenarios.json")["scenarios"], load("skus.json")["skus"])
        cls.per_channel = result["per_channel"]
        cls.total = result["total"]

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


if __name__ == "__main__":
    unittest.main()
