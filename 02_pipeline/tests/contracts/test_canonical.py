import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "01_contracts" / "src"))
CONTRACTS = ROOT / "01_contracts" / "contracts"

from datetime import datetime, timezone

import canonical
import scenario_loader

UTC = timezone.utc


def utc(*args):
    return datetime(*args, tzinfo=UTC)


class TestCanonical(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.skus = scenario_loader.load_skus(CONTRACTS / "skus.json")
        scenarios = scenario_loader.load_scenarios(CONTRACTS / "scenarios.json")
        cls.by_id = {s["id"]: s for s in scenarios}

    def events(self, scenario_id, channel="A"):
        return canonical.build_events(self.by_id[scenario_id], self.skus, channel)

    def test_s01_order_created(self):
        created, payment = self.events("S01")
        self.assertEqual(created["event_id"], "A-S01-1-E1")
        self.assertEqual(created["event_type"], "ORDER_CREATED")
        self.assertEqual(created["order_id"], "A-S01-1")
        self.assertEqual(created["user_id"], "U01")
        self.assertEqual(created["event_time"], utc(2026, 10, 12, 1, 0, 0))
        self.assertEqual(created["shipping_fee"], 0)
        self.assertEqual(created["items"], [{
            "order_item_id": "A-S01-1-1", "product_id": "SKU-004", "product_name": "러닝화",
            "category": "신발", "quantity": 1, "unit_price": 89000, "item_discount": 0}])
        self.assertIsNone(created["fault"])
        self.assertEqual(created["send_count"], 1)

    def test_s01_payment(self):
        _, payment = self.events("S01")
        self.assertEqual(payment["event_id"], "A-S01-1-E2")
        self.assertEqual(payment["event_type"], "PAYMENT_SUCCEEDED")
        self.assertEqual(payment["payment_id"], "A-S01-1-P1")
        self.assertEqual(payment["payment_method"], "CARD")
        self.assertEqual(payment["payment_amount"], 89000)
        self.assertIsNone(payment["fail_reason_code"])
        self.assertEqual(payment["event_time"], utc(2026, 10, 12, 1, 0, 30))

    def test_channel_prefix_in_ids(self):
        created, _ = self.events("S01", channel="B")
        self.assertEqual(created["order_id"], "B-S01-1")
        self.assertEqual(created["items"][0]["order_item_id"], "B-S01-1-1")

    def test_s02_shipping_and_boundary(self):
        events = self.events("S02")
        self.assertEqual(len(events), 4)
        o1_created, o1_pay, o2_created, o2_pay = events
        self.assertEqual((o1_created["shipping_fee"], o1_pay["payment_amount"]), (4000, 12000))
        self.assertEqual(o1_pay["payment_method"], "EASY_PAY")
        self.assertEqual((o2_created["shipping_fee"], o2_pay["payment_amount"]), (0, 20000))
        self.assertEqual([i["order_item_id"] for i in o2_created["items"]],
                         ["A-S02-2-1", "A-S02-2-2"])
        # 주문 ②는 start_offset_sec=300 이므로 KST 10:15:00 = UTC 01:15:00
        self.assertEqual(o2_created["event_time"], utc(2026, 10, 12, 1, 15, 0))

    def test_s04_multi_items_and_discount(self):
        created, payment = self.events("S04")
        self.assertEqual([i["item_discount"] for i in created["items"]], [5000, 0])
        self.assertEqual([i["quantity"] for i in created["items"]], [2, 1])
        self.assertEqual(payment["payment_amount"], 88000)

    def test_s06_payment_attempts(self):
        created, failed, success = self.events("S06")
        self.assertEqual([e["event_id"] for e in (created, failed, success)],
                         ["A-S06-1-E1", "A-S06-1-E2", "A-S06-1-E3"])
        self.assertEqual(failed["event_type"], "PAYMENT_FAILED")
        self.assertEqual(failed["payment_id"], "A-S06-1-P1")
        self.assertEqual(failed["fail_reason_code"], "LIMIT_EXCEEDED")
        self.assertEqual(failed["payment_method"], "CARD")
        self.assertEqual(failed["event_time"], utc(2026, 10, 12, 1, 30, 20))
        self.assertEqual(success["payment_id"], "A-S06-1-P2")
        self.assertEqual(success["payment_method"], "EASY_PAY")
        self.assertEqual(success["payment_amount"], 59000)
        self.assertEqual(success["event_time"], utc(2026, 10, 12, 1, 30, 50))

    def test_s08_cancel(self):
        created, cancelled = self.events("S08")
        self.assertEqual(created["shipping_fee"], 4000)
        self.assertEqual(cancelled["event_type"], "ORDER_CANCELLED")
        self.assertEqual(cancelled["cancel_reason_code"], "CHANGE_OF_MIND")

    def test_s09_partial_refund(self):
        refund = self.events("S09")[-1]
        self.assertEqual(refund["event_id"], "A-S09-1-E3")
        self.assertEqual(refund["event_type"], "REFUND_SUCCEEDED")
        self.assertEqual(refund["refund_id"], "A-S09-1-R1")
        self.assertEqual(refund["payment_id"], "A-S09-1-P1")
        self.assertEqual(refund["order_item_id"], "A-S09-1-1")
        self.assertEqual(refund["refund_quantity"], 1)
        self.assertEqual(refund["refund_amount"], 35000)
        self.assertEqual(refund["refund_type"], "RETURN")
        self.assertEqual(refund["refund_reason_code"], "DEFECT")
        # 주문 10:50:00 + 630초 = KST 11:00:30 = UTC 02:00:30
        self.assertEqual(refund["event_time"], utc(2026, 10, 12, 2, 0, 30))

    def test_s19_duplicate_is_marked(self):
        counts = [e["send_count"] for e in self.events("S19")]
        self.assertEqual(counts, [1, 2])

    def test_s21_faults_are_carried(self):
        events = self.events("S21")
        self.assertEqual([e["fault"] for e in events],
                         ["MISSING_REQUIRED", "INVALID_TYPE", "UNKNOWN_EVENT_TYPE"])
        self.assertEqual([e["order_id"] for e in events], ["A-S21-1", "A-S21-2", "A-S21-3"])
        self.assertEqual([e["event_id"] for e in events],
                         ["A-S21-1-E1", "A-S21-2-E1", "A-S21-3-E1"])

    def test_s23_midnight_boundary(self):
        created, payment = self.events("S23")
        self.assertEqual(created["event_time"], utc(2026, 10, 11, 15, 29, 30))
        self.assertEqual(payment["event_time"], utc(2026, 10, 11, 15, 30, 0))
        self.assertEqual(payment["payment_method"], "VIRTUAL_ACCOUNT")


if __name__ == "__main__":
    unittest.main()
