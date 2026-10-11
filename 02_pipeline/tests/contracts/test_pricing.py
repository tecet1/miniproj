import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "01_contracts" / "src"))
CONTRACTS = ROOT / "01_contracts" / "contracts"

import pricing


class TestPricing(unittest.TestCase):
    def test_item_amounts_without_discount(self):
        self.assertEqual(pricing.item_amounts(89000, 1, 0), (89000, 89000))

    def test_item_amounts_with_discount(self):
        # SKU-001 x2, 할인 5,000 -> gross 58,000 / paid 53,000
        self.assertEqual(pricing.item_amounts(29000, 2, 5000), (58000, 53000))

    def test_shipping_fee_below_threshold(self):
        for paid_total in (8000, 12000, 19999):
            self.assertEqual(pricing.shipping_fee(paid_total), 4000, paid_total)

    def test_shipping_fee_at_or_above_threshold(self):
        for paid_total in (20000, 88000):
            self.assertEqual(pricing.shipping_fee(paid_total), 0, paid_total)

    def test_refund_amount_without_discount(self):
        self.assertEqual(pricing.refund_amount(70000, 2, 1), 35000)

    def test_refund_amount_is_proportional(self):
        self.assertEqual(pricing.refund_amount(53000, 2, 1), 26500)


if __name__ == "__main__":
    unittest.main()
