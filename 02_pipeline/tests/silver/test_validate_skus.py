"""참조 데이터(skus.json)에 없는 product_id 검증 테스트. 기준: sku_spec.md '참조 데이터에 없는 SKU', event_spec.md 4절 INVALID_VALUE."""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

from conversion_error import ConversionError  # noqa: E402
from validate_skus import load_known_skus, validate_known_skus  # noqa: E402

SKUS_JSON = ROOT / "01_contracts" / "contracts" / "skus.json"
KNOWN = {"SKU-001", "SKU-004"}


def order(*product_ids) -> dict:
    items = [{"order_item_id": f"O-{i}", "product_id": pid, "quantity": 1, "unit_price": 1000, "item_discount": 0}
             for i, pid in enumerate(product_ids, start=1)]
    return {"event_type": "ORDER_CREATED", "items": items}


class TestLoadKnownSkus(unittest.TestCase):
    def test_reads_product_ids_from_skus_json(self):
        known = load_known_skus(SKUS_JSON)
        self.assertEqual(15, len(known))
        self.assertIn("SKU-001", known)
        self.assertIn("SKU-015", known)


class TestValidateKnownSkus(unittest.TestCase):
    def test_all_known_passes(self):
        validate_known_skus(order("SKU-001", "SKU-004"), KNOWN)

    def test_unknown_product_id_is_invalid_value(self):
        with self.assertRaises(ConversionError) as ctx:
            validate_known_skus(order("SKU-001", "SKU-999"), KNOWN)
        self.assertEqual("INVALID_VALUE", ctx.exception.reason_code)

    def test_error_detail_names_the_unknown_product(self):
        with self.assertRaises(ConversionError) as ctx:
            validate_known_skus(order("SKU-999"), KNOWN)
        self.assertIn("SKU-999", ctx.exception.detail)

    def test_non_order_events_are_not_checked(self):
        for event_type in ("ORDER_CANCELLED", "PAYMENT_SUCCEEDED", "PAYMENT_FAILED", "REFUND_SUCCEEDED"):
            validate_known_skus({"event_type": event_type}, KNOWN)   # items가 없어도 통과한다


if __name__ == "__main__":
    unittest.main()
