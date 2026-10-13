"""Dimension 행 테스트 (순수 Python). 기준은 event_spec.md 6.2절과 skus.json이다."""
import json
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "gold"))

import dimensions  # noqa: E402
import gold_tables  # noqa: E402

SKUS = json.loads((ROOT / "01_contracts" / "contracts" / "skus.json").read_text(encoding="utf-8"))["skus"]
EXPECTED = json.loads((ROOT / "01_contracts" / "data_fixtures" / "expected" / "expected.json").read_text(encoding="utf-8"))


class TestConstantDimensions(unittest.TestCase):
    def test_row_counts_match_expected_dimensions(self):
        want = EXPECTED["dimensions"]
        self.assertEqual(want["dim_channel"], len(dimensions.channel_rows()))
        self.assertEqual(want["dim_payment_method"], len(dimensions.payment_method_rows()))
        self.assertEqual(want["dim_refund_profile"], len(dimensions.refund_profile_rows()))
        self.assertEqual(want["dim_product"], len(dimensions.product_rows()))

    def test_channels(self):
        self.assertEqual([("A", "자사몰"), ("B", "종합 오픈마켓"), ("C", "스포츠 전문몰")], dimensions.channel_rows())

    def test_payment_methods_are_the_five_common_codes(self):
        self.assertEqual({"CARD", "EASY_PAY", "BANK_TRANSFER", "VIRTUAL_ACCOUNT", "OTHER"},
                         {key for key, _ in dimensions.payment_method_rows()})

    def test_refund_profile_is_type_times_reason_with_unique_keys(self):
        rows = dimensions.refund_profile_rows()
        self.assertEqual(10, len({key for key, _, _ in rows}))
        self.assertEqual({"CANCEL", "RETURN"}, {kind for _, kind, _ in rows})
        self.assertEqual({"CHANGE_OF_MIND", "DEFECT", "WRONG_ITEM", "DELAY", "OTHER"}, {reason for _, _, reason in rows})
        self.assertIn(("RETURN-DEFECT", "RETURN", "DEFECT"), rows)
        for key, kind, reason in rows:
            self.assertEqual(f"{kind}-{reason}", key)

    def test_products_come_from_skus_json(self):
        rows = dimensions.product_rows()
        self.assertEqual([(s["product_id"], s["product_name"], s["category"]) for s in SKUS], rows)

    def test_rows_fit_the_table_definitions(self):
        sample = {"dim_channel": dimensions.channel_rows(), "dim_payment_method": dimensions.payment_method_rows(),
                  "dim_refund_profile": dimensions.refund_profile_rows(), "dim_product": dimensions.product_rows(),
                  "dim_date": dimensions.date_rows(date(2026, 10, 12), date(2026, 10, 12))}
        for table, rows in sample.items():
            width = len(gold_tables.column_names(table))
            for row in rows:
                self.assertEqual(width, len(row), table)


class TestDateRows(unittest.TestCase):
    def test_fixture_day(self):
        self.assertEqual([(date(2026, 10, 12), "월", date(2026, 10, 12), "2026-10", False)],
                         dimensions.date_rows(date(2026, 10, 12), date(2026, 10, 12)))

    def test_range_is_inclusive_and_ordered(self):
        rows = dimensions.date_rows(date(2026, 10, 9), date(2026, 10, 13))
        self.assertEqual([date(2026, 10, d) for d in range(9, 14)], [r[0] for r in rows])
        self.assertEqual(["금", "토", "일", "월", "화"], [r[1] for r in rows])

    def test_week_starts_on_monday(self):
        rows = dimensions.date_rows(date(2026, 10, 9), date(2026, 10, 13))
        self.assertEqual([date(2026, 10, 5)] * 3 + [date(2026, 10, 12)] * 2, [r[2] for r in rows])

    def test_month_label_crosses_year_end(self):
        rows = dimensions.date_rows(date(2026, 12, 31), date(2027, 1, 1))
        self.assertEqual(["2026-12", "2027-01"], [r[3] for r in rows])

    def test_holiday_flag(self):
        flags = {r[0]: r[4] for r in dimensions.date_rows(date(2026, 10, 8), date(2026, 10, 10))}
        self.assertEqual({date(2026, 10, 8): False, date(2026, 10, 9): True, date(2026, 10, 10): False}, flags)

    def test_empty_when_end_is_before_start(self):
        self.assertEqual([], dimensions.date_rows(date(2026, 10, 13), date(2026, 10, 12)))


if __name__ == "__main__":
    unittest.main()
