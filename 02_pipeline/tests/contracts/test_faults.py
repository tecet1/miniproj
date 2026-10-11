import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "01_contracts" / "src"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import copy

import channel_a
import channel_b
import channel_c
import faults

# 채널별 최소 원본 (필요한 필드만)
RAW = {
    "A": {"payment": {"event_type": "payment.succeeded", "amount": 59000},
          "created": {"event_type": "order.created", "items": [{"quantity": 1}]}},
    "B": {"payment": {"eventName": "PAY_APPROVED", "paidAmount": 59000},
          "created": {"eventName": "ORDER_PLACED", "orderItems": [{"qty": 1}]}},
    "C": {"payment": {"evt_cd": "P01", "pay_amt": 59000},
          "created": {"evt_cd": "O01", "lines": [{"qty": 1}]}},
}
MODULES = {"A": channel_a, "B": channel_b, "C": channel_c}


class TestFaults(unittest.TestCase):
    def apply(self, channel, kind, fault):
        module = MODULES[channel]
        raw = copy.deepcopy(RAW[channel][kind])
        return raw, faults.apply_fault(raw, fault, module.FAULT_PATHS, module.UNKNOWN_EVENT_CODE)

    def test_missing_required_removes_payment_amount(self):
        expected_removed = {"A": "amount", "B": "paidAmount", "C": "pay_amt"}
        for channel in "ABC":
            with self.subTest(channel=channel):
                _, result = self.apply(channel, "payment", "MISSING_REQUIRED")
                self.assertNotIn(expected_removed[channel], result)

    def test_invalid_type_makes_quantity_a_string(self):
        for channel, path in {"A": ("items", 0, "quantity"), "B": ("orderItems", 0, "qty"),
                              "C": ("lines", 0, "qty")}.items():
            with self.subTest(channel=channel):
                _, result = self.apply(channel, "created", "INVALID_TYPE")
                value = result[path[0]][path[1]][path[2]]
                self.assertIsInstance(value, str)

    def test_unknown_event_type_uses_unmapped_code(self):
        for channel, field in {"A": "event_type", "B": "eventName", "C": "evt_cd"}.items():
            with self.subTest(channel=channel):
                module = MODULES[channel]
                _, result = self.apply(channel, "created", "UNKNOWN_EVENT_TYPE")
                self.assertEqual(result[field], module.UNKNOWN_EVENT_CODE)
                self.assertNotIn(module.UNKNOWN_EVENT_CODE, module.EVENT_CODES.values())

    def test_input_is_not_mutated(self):
        raw, _ = self.apply("A", "payment", "MISSING_REQUIRED")
        self.assertEqual(raw, RAW["A"]["payment"])

    def test_unknown_fault_name_is_rejected(self):
        with self.assertRaises(ValueError):
            faults.apply_fault({}, "NOT_A_FAULT", channel_a.FAULT_PATHS, "x")


if __name__ == "__main__":
    unittest.main()
