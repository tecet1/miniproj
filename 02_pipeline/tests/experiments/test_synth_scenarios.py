"""synth_scenarios 테스트 (pyspark 없이). 같은 시드의 재현성, 형식 유효성, 업무 규칙, 기존 생성기와의 연결을 확인한다.

기대값과의 독립성은 expected.py가 시나리오만 읽는 구조로 지킨다. 여기서는 expected를 시나리오에서 한 번 더 세어 대조한다.
"""
import json
import sys
import unittest
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "04_experiments" / "src"), str(ROOT / "01_contracts" / "src")]

import canonical  # noqa: E402
import channel_a  # noqa: E402
import channel_b  # noqa: E402
import channel_c  # noqa: E402
import expected  # noqa: E402
import scenario_loader  # noqa: E402
import scenario_validator  # noqa: E402
import synth_scenarios as synth  # noqa: E402
from ids import NamedIds  # noqa: E402

CONTRACTS = ROOT / "01_contracts" / "contracts"
SKUS = scenario_loader.load_skus(CONTRACTS / "skus.json")
SEED, START, END, BASE = 7, date(2025, 10, 15), date(2026, 10, 14), 12
YEAR = synth.build_scenarios(SKUS, SEED, START, END, BASE)
ORDERS = [(s, o) for s in YEAR for o in s["orders"]]
# 채널 변환기(생성기 쪽)가 만들 수 있는 코드. 세 채널의 교집합이다. synth_scenarios의 상수와 별개로 읽는다.
CONVERTIBLE_METHODS = set(channel_a.METHODS) & set(channel_b.METHODS) & set(channel_c.METHODS)
CONVERTIBLE_REASONS = set(channel_a.REASONS) & set(channel_b.REASONS) & set(channel_c.REASONS)
CONVERTIBLE_FAILS = set(channel_a.FAIL_REASONS) & set(channel_b.FAIL_REASONS) & set(channel_c.FAIL_REASONS)


def kind_of(order):
    """주문 이벤트 구성으로 유형을 거꾸로 알아낸다 (생성 코드의 분기와 별개로 센다)."""
    types = [e["type"] for e in order["events"]]
    if "ORDER_CANCELLED" in types:
        return "CANCELLED"
    if "REFUND_SUCCEEDED" in types:
        return "REFUNDED"
    if "PAYMENT_SUCCEEDED" not in types:
        return "FAILED_ONLY"
    return "RETRY_PAID" if "PAYMENT_FAILED" in types else "PAID"


class TestReproducibility(unittest.TestCase):
    def test_same_seed_gives_same_scenarios(self):
        self.assertEqual(YEAR, synth.build_scenarios(SKUS, SEED, START, END, BASE))

    def test_other_seed_gives_other_scenarios(self):
        self.assertNotEqual(YEAR, synth.build_scenarios(SKUS, SEED + 1, START, END, BASE))

    def test_days_differ_from_each_other(self):
        first_orders = {json.dumps(s["orders"][0], sort_keys=True) for s in YEAR}
        self.assertGreater(len(first_orders), 300)

    def test_a_day_does_not_depend_on_the_range(self):
        short = synth.build_scenarios(SKUS, SEED, date(2026, 3, 1), date(2026, 10, 14), BASE)
        self.assertEqual(next(s for s in YEAR if s["id"] == "L20260301"), short[0])


class TestShape(unittest.TestCase):
    def test_one_scenario_per_day_in_order(self):
        self.assertEqual(365, len(YEAR))
        days = [datetime.fromisoformat(s["start_kst"]).date() for s in YEAR]
        self.assertEqual([START + timedelta(days=n) for n in range(365)], days)
        self.assertEqual(len(YEAR), len({s["id"] for s in YEAR}))

    def test_ids_do_not_collide_with_fixtures(self):
        fixture_ids = {s["id"] for s in scenario_loader.load_scenarios(CONTRACTS / "scenarios.json")}
        self.assertFalse(fixture_ids & {s["id"] for s in YEAR})

    def test_passes_the_scenario_validator(self):
        scenario_validator.validate_scenarios(YEAR, SKUS)

    def test_order_numbers_are_unique_per_scenario(self):
        for scenario in YEAR:
            numbers = [o["order_no"] for o in scenario["orders"]]
            self.assertEqual(list(range(1, len(numbers) + 1)), numbers, scenario["id"])

    def test_every_order_kind_appears(self):
        self.assertEqual({"PAID", "RETRY_PAID", "FAILED_ONLY", "CANCELLED", "REFUNDED"},
                         {kind_of(o) for _, o in ORDERS})

    def test_weekend_is_busier_than_midweek(self):
        by_weekday = Counter()
        for scenario in YEAR:
            by_weekday[datetime.fromisoformat(scenario["start_kst"]).weekday()] += len(scenario["orders"])
        self.assertGreater(by_weekday[5], by_weekday[1])

    def test_no_fault_is_injected(self):
        self.assertFalse([e for _, o in ORDERS for e in o["events"] if "fault" in e])

    def test_some_events_are_sent_twice(self):
        self.assertTrue([e for _, o in ORDERS for e in o["events"] if e.get("send_count") == 2])


class TestBusinessRules(unittest.TestCase):
    def test_discount_is_within_a_fifth_of_gross_and_in_500_units(self):
        discounts = []
        for _, order in ORDERS:
            for line in order["items"]:
                gross = SKUS[line["sku"]]["unit_price"] * line["quantity"]
                self.assertLessEqual(line["discount"] * 5, gross)
                self.assertEqual(0, line["discount"] % 500)
                discounts.append(line["discount"])
        self.assertTrue([d for d in discounts if d > 0])

    def test_codes_are_the_ones_channel_converters_map_and_all_of_them_appear(self):
        events = [e for _, o in ORDERS for e in o["events"]]
        self.assertEqual(CONVERTIBLE_METHODS, {e["method"] for e in events if "method" in e})
        self.assertEqual(CONVERTIBLE_REASONS, {e["reason"] for e in events if "reason" in e})
        self.assertEqual(CONVERTIBLE_FAILS, {e["fail_reason"] for e in events if "fail_reason" in e})

    def test_cancelled_orders_are_never_paid(self):
        for _, order in ORDERS:
            if kind_of(order) == "CANCELLED":
                self.assertNotIn("PAYMENT_SUCCEEDED", [e["type"] for e in order["events"]])

    def test_refund_follows_the_payment_and_fits_the_line(self):
        for _, order in ORDERS:
            paid_at = next((e["offset_sec"] for e in order["events"] if e["type"] == "PAYMENT_SUCCEEDED"), None)
            seen = set()
            for event in order["events"]:
                if event["type"] != "REFUND_SUCCEEDED":
                    continue
                self.assertIsNotNone(paid_at)
                self.assertGreaterEqual(event["offset_sec"] - paid_at, 3600)   # 결제 1시간 이후 (계획 06)
                self.assertLessEqual(event["quantity"], order["items"][event["item"] - 1]["quantity"])
                self.assertNotIn(event["item"], seen)   # 상품 행마다 환불은 최대 1건
                seen.add(event["item"])

    def test_refunds_split_between_cancel_and_return_and_cross_days(self):
        refunds = [(s, o, e) for s, o in ORDERS for e in o["events"] if e["type"] == "REFUND_SUCCEEDED"]
        self.assertEqual({"CANCEL", "RETURN"}, {e["refund_type"] for _, _, e in refunds})
        self.assertTrue([1 for _, o, e in refunds if e["offset_sec"] >= 86400 * 2])   # 결제일 != 환불일

    def test_nothing_happens_after_the_end_of_the_range(self):
        limit = datetime(END.year, END.month, END.day, tzinfo=synth.KST) + timedelta(days=1)
        for scenario in YEAR:
            start = datetime.fromisoformat(scenario["start_kst"])
            for order in scenario["orders"]:
                for event in order["events"]:
                    when = start + timedelta(seconds=order["start_offset_sec"] + event["offset_sec"])
                    self.assertLess(when, limit, scenario["id"])


class TestWithExistingGenerators(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.month = synth.build_scenarios(SKUS, SEED, date(2026, 9, 1), date(2026, 9, 30), BASE)
        cls.ids = NamedIds(str(SEED))

    def test_canonical_builds_events_with_unique_uuid_ids(self):
        events = [e for s in self.month for e in canonical.build_events(s, SKUS, "A", self.ids)]
        self.assertEqual(len(events), len({e["event_id"] for e in events}))
        self.assertTrue(all(len(e["event_id"]) == 36 for e in events))

    def test_event_count_matches_scenarios(self):
        built = sum(len(canonical.build_events(s, SKUS, "A", self.ids)) for s in self.month)
        self.assertEqual(sum(len(o["events"]) for s in self.month for o in s["orders"]), built)

    def test_expected_counts_agree_with_a_second_count(self):
        result = expected.compute_expected(self.month, list(SKUS.values()))["per_channel"]["A"]
        orders = [o for s in self.month for o in s["orders"]]
        kinds = Counter(kind_of(o) for o in orders)
        self.assertEqual(len(orders), result["silver_orders"])
        self.assertEqual(sum(len(o["items"]) for o in orders), result["silver_order_items"])
        self.assertEqual(kinds["CANCELLED"], result["silver_order_cancellations"])
        self.assertEqual(sum(len(o["events"]) + sum(1 for e in o["events"] if e.get("send_count") == 2)
                             for o in orders), result["bronze_rows"])
        paying = kinds["PAID"] + kinds["RETRY_PAID"] + kinds["REFUNDED"]
        self.assertEqual(sum(len(o["items"]) for o in orders if kind_of(o) in ("PAID", "RETRY_PAID", "REFUNDED")),
                         result["fact_sales_item"])
        self.assertGreater(paying, 0)
        self.assertEqual(0, result["quarantine"])


if __name__ == "__main__":
    unittest.main()
