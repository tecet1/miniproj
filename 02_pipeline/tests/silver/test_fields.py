import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

from conversion_error import ConversionError
from fields import get_required, get_required_int


class TestGetRequired(unittest.TestCase):
    def test_returns_value(self):
        self.assertEqual(get_required({"a": 1}, "a"), 1)

    def test_missing_key_is_missing_required(self):
        with self.assertRaises(ConversionError) as ctx:
            get_required({}, "a")
        self.assertEqual(ctx.exception.reason_code, "MISSING_REQUIRED")

    def test_none_value_is_missing_required(self):
        with self.assertRaises(ConversionError) as ctx:
            get_required({"a": None}, "a")
        self.assertEqual(ctx.exception.reason_code, "MISSING_REQUIRED")

    def test_zero_and_empty_string_are_present(self):
        self.assertEqual(get_required({"a": 0}, "a"), 0)
        self.assertEqual(get_required({"a": ""}, "a"), "")


class TestGetRequiredInt(unittest.TestCase):
    def test_accepts_int(self):
        self.assertEqual(get_required_int({"amount": 50000}, "amount"), 50000)

    def test_rejects_string_float_and_bool(self):
        for bad in ("N/A", "100", 1.5, True):
            with self.assertRaises(ConversionError) as ctx:
                get_required_int({"amount": bad}, "amount")
            self.assertEqual(ctx.exception.reason_code, "INVALID_TYPE")

    def test_missing_or_none_is_missing_required(self):
        for data in ({}, {"amount": None}):
            with self.assertRaises(ConversionError) as ctx:
                get_required_int(data, "amount")
            self.assertEqual(ctx.exception.reason_code, "MISSING_REQUIRED")


if __name__ == "__main__":
    unittest.main()
