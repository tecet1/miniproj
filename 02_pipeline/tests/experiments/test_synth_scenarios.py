"""synth_scenarios 테스트 (pyspark 없이). 모형 파라미터의 실현, 재현성, 형식 유효성, 업무 규칙, 기존 생성기와의 연결을 확인한다.

모형(월 수준 경로, 음이항 일 주문 수, 요일·판촉 계수)은 04_experiments/docs/기업_프로필과_성장_모형.md가 근거다.
기대값과의 독립성은 expected.py가 시나리오만 읽는 구조로 지킨다. 여기서는 expected를 시나리오에서 한 번 더 세어 대조한다.
"""
import json
import math
import random
import statistics
import sys
import unittest
from collections import Counter
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "04_experiments" / "src"), str(ROOT / "01_contracts" / "src")]

sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

import canonical  # noqa: E402
import generate  # noqa: E402
import channel_a  # noqa: E402
import channel_b  # noqa: E402
import channel_c  # noqa: E402
import expected  # noqa: E402
import scenario_loader  # noqa: E402
import scenario_validator  # noqa: E402
import synth_scenarios as synth  # noqa: E402
from ids import NamedIds  # noqa: E402
from convert_row import convert_row  # noqa: E402
from validate_skus import load_known_skus  # noqa: E402

CONTRACTS = ROOT / "01_contracts" / "contracts"
SKUS = scenario_loader.load_skus(CONTRACTS / "skus.json")
DUMP_CONFIG = json.loads((ROOT / "04_experiments" / "configs" / "dump.json").read_text(encoding="utf-8"))
CONFIG = dict(DUMP_CONFIG, error_rates={})      # 모형 테스트는 오류 주문이 없는 데이터로 본다 (오류 주입은 TestErrorInjection이 따로 본다)
YEAR = synth.build_scenarios(SKUS, CONFIG)
ORDERS = [(s, o) for s in YEAR for o in s["orders"]]
START, END = date.fromisoformat(CONFIG["start"]), date.fromisoformat(CONFIG["end"])
FLAT = dict(CONFIG, growth_annual=0.0, sigma_monthly=0.0, month_factor={}, promo={"days_per_month": [0, 0], "multiplier": [2.0, 3.0]})
# 보고서(기업_프로필과_성장_모형.md 2절)의 채널당 연간 주문 수: 시드 2,000개 평균 약 83,300, 5~95% 약 76,700~90,400
REPORT_ANNUAL_ORDERS = 83300
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


def sample_moments(mean, dispersion, n, seed=1):
    rng = random.Random(seed)
    values = [synth.draw_count(rng, mean, dispersion) for _ in range(n)]
    return statistics.mean(values), statistics.pvariance(values)


class TestGrowthModel(unittest.TestCase):
    def test_month_levels_start_at_one(self):
        self.assertEqual(1.0, synth.month_levels(CONFIG, 12)[0])

    def test_monthly_log_growth_has_the_configured_mean_and_sigma(self):
        steps = []
        for seed in range(400):
            levels = synth.month_levels(dict(CONFIG, seed=seed), 12)
            steps += [math.log(levels[i] / levels[i - 1]) for i in range(1, 13)]
        mu = math.log(1 + CONFIG["growth_annual"]) / 12
        self.assertAlmostEqual(mu, statistics.mean(steps), delta=0.0008)   # 표준오차 약 0.0004
        self.assertAlmostEqual(CONFIG["sigma_monthly"], statistics.pstdev(steps), delta=0.002)

    def test_level_path_does_not_depend_on_the_end_month(self):
        self.assertEqual(synth.month_levels(CONFIG, 12)[:7], synth.month_levels(CONFIG, 6))

    def test_without_growth_and_noise_the_level_is_flat(self):
        self.assertEqual([1.0] * 13, synth.month_levels(FLAT, 12))

    def test_month_index_counts_from_the_anchor_month(self):
        self.assertEqual([0, 1, 3, 12], [synth.month_index(date(y, m, 1), CONFIG)
                                         for y, m in ((2025, 10), (2025, 11), (2026, 1), (2026, 10))])


class TestDailyMeans(unittest.TestCase):
    def test_flat_config_gives_the_base_times_a_weekday_factor_averaging_one(self):
        means = synth.daily_mean_orders(FLAT)
        base = CONFIG["base_orders_per_day"]
        week = CONFIG["weekday_factor"]
        for day, mean in means:
            self.assertAlmostEqual(base * week[day.weekday()] / (sum(week) / 7), mean, places=9, msg=str(day))
        self.assertAlmostEqual(1.35 / 1.0, dict((d.weekday(), m) for d, m in means)[5] / dict((d.weekday(), m) for d, m in means)[1])

    def test_promo_days_per_month_are_one_or_two_with_the_configured_multiplier(self):
        for month in range(1, 13):
            promos = synth.promo_days(CONFIG, 2026, month)
            self.assertIn(len(promos), (1, 2))
            self.assertTrue(all(1 <= day <= synth.days_in_month(2026, month) for day in promos))
            self.assertTrue(all(2.0 <= m <= 3.0 for m in promos.values()))
        self.assertEqual(synth.promo_days(CONFIG, 2026, 5), synth.promo_days(CONFIG, 2026, 5))

    def test_promo_days_raise_those_days_but_not_the_monthly_volume(self):
        flat_promo = dict(FLAT, start="2026-09-01", end="2026-09-30", promo=CONFIG["promo"])
        totals = []
        for seed in range(150):
            means = synth.daily_mean_orders(dict(flat_promo, seed=seed))
            totals.append(sum(m for _, m in means))
        self.assertAlmostEqual(CONFIG["base_orders_per_day"] * 30, statistics.mean(totals), delta=CONFIG["base_orders_per_day"] * 30 * 0.02)
        peak = max(m for _, m in synth.daily_mean_orders(dict(flat_promo, seed=3)))
        week = CONFIG["weekday_factor"]
        self.assertGreater(peak, CONFIG["base_orders_per_day"] * max(week) / (sum(week) / 7) * 1.4)

    def test_expected_annual_volume_matches_the_report(self):
        totals = [sum(m for _, m in synth.daily_mean_orders(dict(CONFIG, seed=seed))) for seed in range(100)]
        self.assertAlmostEqual(REPORT_ANNUAL_ORDERS, statistics.mean(totals), delta=REPORT_ANNUAL_ORDERS * 0.02)


class TestCountDistribution(unittest.TestCase):
    def test_negative_binomial_moments_for_a_large_mean(self):
        mean, variance = sample_moments(200, 50, 20000)
        self.assertAlmostEqual(200, mean, delta=1.5)
        self.assertAlmostEqual(200 + 200 ** 2 / 50, variance, delta=100)   # 1000 +-10%

    def test_negative_binomial_moments_for_a_small_mean(self):
        mean, variance = sample_moments(5, 50, 40000)
        self.assertAlmostEqual(5, mean, delta=0.1)
        self.assertAlmostEqual(5 + 5 ** 2 / 50, variance, delta=0.4)

    def test_counts_are_more_dispersed_than_poisson(self):
        mean, variance = sample_moments(200, 50, 20000)
        self.assertGreater(variance, 3 * mean)

    def test_counts_are_never_negative(self):
        rng = random.Random(0)
        self.assertGreaterEqual(min(synth.draw_count(rng, 3, 50) for _ in range(2000)), 0)


class TestReproducibility(unittest.TestCase):
    def test_same_config_gives_same_scenarios(self):
        self.assertEqual(YEAR, synth.build_scenarios(SKUS, CONFIG))

    def test_other_seed_gives_other_scenarios(self):
        month = dict(CONFIG, start="2026-03-01", end="2026-03-31")
        self.assertNotEqual(synth.build_scenarios(SKUS, month), synth.build_scenarios(SKUS, dict(month, seed=1)))

    def test_days_differ_from_each_other(self):
        first_orders = {json.dumps(s["orders"][0], sort_keys=True) for s in YEAR}
        self.assertGreater(len(first_orders), 300)

    def test_days_already_made_do_not_change_when_the_end_is_moved(self):
        short = synth.build_scenarios(SKUS, dict(CONFIG, end="2026-06-30"))
        stable = (date(2026, 6, 19) - START).days + 1   # 종료일 11일 전까지. 그 뒤 날짜는 환불이 종료일에 잘려 달라진다
        self.assertEqual(len(YEAR[:stable]), len(short[:stable]))
        self.assertTrue(all(a == b for a, b in zip(YEAR[:stable], short[:stable])))

    def test_days_near_the_end_lose_refunds_that_would_fall_after_it(self):
        short = synth.build_scenarios(SKUS, dict(CONFIG, end="2026-06-30"))
        refunds = lambda scenarios: sum(1 for s in scenarios for o in s["orders"] for e in o["events"]
                                         if e["type"] == "REFUND_SUCCEEDED")
        self.assertLess(refunds(short[-10:]), refunds(YEAR[len(short) - 10:len(short)]))

    def test_a_day_does_not_depend_on_the_start_date(self):
        short = synth.build_scenarios(SKUS, dict(CONFIG, start="2026-03-01"))
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

    def test_orders_are_numbered_in_time_order(self):
        for scenario in YEAR:
            self.assertEqual(list(range(1, len(scenario["orders"]) + 1)), [o["order_no"] for o in scenario["orders"]])
            starts = [o["start_offset_sec"] for o in scenario["orders"]]
            self.assertEqual(sorted(starts), starts, scenario["id"])

    def test_every_order_kind_appears(self):
        self.assertEqual({"PAID", "RETRY_PAID", "FAILED_ONLY", "CANCELLED", "REFUNDED"},
                         {kind_of(o) for _, o in ORDERS})

    def test_weekend_is_busier_than_midweek_in_the_realized_data(self):
        by_weekday = Counter()
        for scenario in YEAR:
            by_weekday[datetime.fromisoformat(scenario["start_kst"]).weekday()] += len(scenario["orders"])
        self.assertGreater(by_weekday[5], by_weekday[1])

    def test_realized_year_is_inside_the_model_band(self):
        self.assertTrue(76000 <= len(ORDERS) <= 91000, len(ORDERS))   # 보고서 5~95% 구간 근처

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

    def test_average_paid_order_amount_is_about_80000(self):
        amounts = []
        for _, order in ORDERS:
            if kind_of(order) in ("PAID", "RETRY_PAID", "REFUNDED"):
                paid = sum(SKUS[i["sku"]]["unit_price"] * i["quantity"] - i["discount"] for i in order["items"])
                amounts.append(paid + (0 if paid >= 20000 else 4000))
        self.assertTrue(72000 <= statistics.mean(amounts) <= 84000, statistics.mean(amounts))

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


class TestCustomersHoursAndRefunds(unittest.TestCase):
    """고객 수 증가, 시간대 분포, 월별 환불율 변동 (2026-10-14 승인 방향)."""

    @staticmethod
    def month_of(scenario):
        return scenario["start_kst"][:7]

    def test_customer_pool_grows_with_the_monthly_level(self):
        pools = synth.customer_pools(CONFIG)
        self.assertEqual(CONFIG["customers_start"], pools[0])
        self.assertGreater(pools[-1], pools[0] * 1.05)

    def test_customer_ids_stay_inside_the_pool_of_the_day(self):
        pools = {d.isoformat(): n for (d, _), n in zip(synth.daily_mean_orders(CONFIG), synth.customer_pools(CONFIG))}
        for scenario, order in ORDERS[::50]:
            self.assertLessEqual(int(order["user_id"][1:]), pools[scenario["start_kst"][:10]])

    def test_customer_base_widens_over_the_year(self):
        """고객 번호는 가입 순서다. 달의 상위 5% 번호(= 그 달 고객 풀의 크기를 보여 준다)가 해를 따라 커진다."""
        by_month = {}
        for scenario, order in ORDERS:
            by_month.setdefault(self.month_of(scenario), []).append(int(order["user_id"][1:]))
        p95 = {m: sorted(ids)[int(len(ids) * 0.95)] for m, ids in by_month.items()}
        self.assertGreater(p95["2026-09"], p95["2025-11"] * 1.05)

    def test_customers_are_realistic_in_number_and_buy_repeatedly(self):
        users = Counter(order["user_id"] for _, order in ORDERS)
        orders_per_customer = len(ORDERS) / len(users)
        self.assertTrue(1.5 < orders_per_customer < 6, orders_per_customer)
        self.assertGreater(len(users), 5000)

    def test_order_start_hours_rise_from_9_peak_at_13_fall_to_5(self):
        hours = Counter(int(o["start_offset_sec"] // 3600) for _, o in ORDERS)
        counts = [hours[h] for h in range(24)]
        self.assertEqual(13, counts.index(max(counts)))
        self.assertEqual(5, counts.index(min(counts)))
        self.assertTrue(all(counts[h] < counts[h + 1] for h in range(8, 13)), counts)      # 9시부터 오른다
        self.assertTrue(all(counts[h] > counts[h + 1] for h in range(13, 23)), counts)      # 13시 뒤로 계속 준다
        self.assertTrue(all(counts[h] > counts[h + 1] for h in range(21, 23)))             # 21시 이후는 더 가파르다
        self.assertGreater(counts[13] / counts[21], 1.5)
        self.assertLess((counts[13] - counts[21]) / 8, (counts[21] - counts[5]) / 8 * 2)   # 13~21시가 더 완만하다

    def test_refund_factor_is_fixed_per_month_and_differs_between_months(self):
        factors = {m: synth.refund_factor(CONFIG, date(2026, m, 1)) for m in range(1, 13)}
        self.assertEqual(factors[3], synth.refund_factor(CONFIG, date(2026, 3, 20)))
        self.assertGreater(len({round(f, 6) for f in factors.values()}), 10)

    def test_monthly_refund_share_varies_and_follows_the_factor(self):
        share, factor = {}, {}
        for scenario in YEAR:
            month = self.month_of(scenario)
            orders = scenario["orders"]
            refunded = sum(1 for o in orders if kind_of(o) == "REFUNDED")
            total, count = share.get(month, (0, 0))
            share[month] = (total + refunded, count + len(orders))
            factor[month] = synth.refund_factor(CONFIG, date.fromisoformat(scenario["start_kst"][:10]))
        rates = {m: r / n for m, (r, n) in share.items() if m not in ("2025-10", "2026-10")}   # 첫·끝 달은 일부만
        self.assertGreater(max(rates.values()) / min(rates.values()), 1.2)
        months = sorted(rates)
        correlation = statistics.correlation([rates[m] for m in months], [factor[m] for m in months])
        self.assertGreater(correlation, 0.8)


class TestChannelStreams(unittest.TestCase):
    """채널별 규모·성장 (2026-10-14 승인: A 20%/+30%, B 50%/+15%, C 30%/+40%)."""

    def test_channel_config_scales_orders_and_customers_by_share(self):
        n = len(CONFIG["channels"])
        for channel, spec in CONFIG["channels"].items():
            stream = synth.channel_config(CONFIG, channel)
            self.assertAlmostEqual(CONFIG["base_orders_per_day"] * n * spec["share"], stream["base_orders_per_day"])
            self.assertEqual(round(CONFIG["customers_start"] * n * spec["share"]), stream["customers_start"])
            self.assertEqual(spec["growth_annual"], stream["growth_annual"])

    def test_shares_sum_to_one(self):
        self.assertAlmostEqual(1.0, sum(c["share"] for c in CONFIG["channels"].values()))

    def test_total_start_volume_is_unchanged(self):
        total = sum(synth.channel_config(CONFIG, c)["base_orders_per_day"] for c in CONFIG["channels"])
        self.assertAlmostEqual(CONFIG["base_orders_per_day"] * len(CONFIG["channels"]), total)

    def test_channels_use_independent_month_paths_and_their_own_growth(self):
        paths = {c: synth.month_levels(synth.channel_config(CONFIG, c), 12) for c in CONFIG["channels"]}
        self.assertNotEqual(paths["A"], paths["B"])
        self.assertGreater(paths["C"][12], paths["B"][12])     # +40% vs +15%

    def test_realized_channel_growth_follows_the_configured_rate(self):
        for channel, spec in CONFIG["channels"].items():
            steps = []
            for seed in range(300):
                stream = {k: v for k, v in synth.channel_config(CONFIG, channel).items() if k != "shared_seed"}
                levels = synth.month_levels(dict(stream, seed=seed), 12)
                steps += [math.log(levels[i] / levels[i - 1]) for i in range(1, 13)]
            self.assertAlmostEqual(math.log(1 + spec["growth_annual"]) / 12, statistics.mean(steps), delta=0.001, msg=channel)

    def test_channel_month_paths_move_together_but_not_identically(self):
        paths = {c: synth.month_levels(synth.channel_config(CONFIG, c), 12) for c in CONFIG["channels"]}
        steps = {c: [math.log(p[i] / p[i - 1]) for i in range(1, 13)] for c, p in paths.items()}
        self.assertGreater(statistics.correlation(steps["A"], steps["B"]), 0.4)
        self.assertNotEqual(steps["A"], steps["B"])
        flat = dict(CONFIG, shock_correlation=0.0)
        independent = {c: synth.month_levels(synth.channel_config(flat, c), 12) for c in CONFIG["channels"]}
        self.assertNotEqual(paths["A"], independent["A"])

    def test_total_shock_variance_does_not_depend_on_the_correlation(self):
        for rho in (0.0, 0.8):
            steps = []
            for seed in range(300):
                levels = synth.month_levels(dict(CONFIG, seed=seed, shock_correlation=rho), 12)
                steps += [math.log(levels[i] / levels[i - 1]) for i in range(1, 13)]
            self.assertAlmostEqual(CONFIG["sigma_monthly"], statistics.pstdev(steps), delta=0.002, msg=str(rho))

    def test_same_config_gives_same_channel_scenarios_and_channels_differ(self):
        first = synth.build_channel_scenarios(SKUS, CONFIG)
        self.assertEqual(first, synth.build_channel_scenarios(SKUS, CONFIG))
        self.assertNotEqual(first["A"][10]["orders"], first["C"][10]["orders"])


class TestWithExistingGenerators(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.month = synth.build_scenarios(SKUS, dict(CONFIG, start="2026-09-01", end="2026-09-30"))
        cls.ids = NamedIds(str(CONFIG["seed"]))

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
        self.assertEqual(sum(len(o["items"]) for o in orders if kind_of(o) in ("PAID", "RETRY_PAID", "REFUNDED")),
                         result["fact_sales_item"])
        self.assertEqual(0, result["quarantine"])


# ── 비율 기반 오류 주입 (2026-10-15) ─────────────────────────────────────────────────────
SHORT = dict(CONFIG, start="2025-11-03", end="2025-11-16")      # 14일(주문 약 3천 건)로 줄여 빠르게 돈다
ALL_RATES = {"fault": 0.04, "double_payment": 0.04, "duplicate_order_created": 0.04, "refund_to_failed_payment": 0.15,
             "refund_order_mismatch": 0.15, "duplicate_item_id": 0.08}
KNOWN_SKUS = load_known_skus(CONTRACTS / "skus.json")


def by_key(scenarios):
    return {(s["id"], o["order_no"]): o for s in scenarios for o in s["orders"]}


class TestErrorInjection(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.base = synth.build_scenarios(SKUS, SHORT)
        cls.injected = synth.build_scenarios(SKUS, dict(SHORT, error_rates=ALL_RATES))
        cls.base_orders, cls.orders = by_key(cls.base), by_key(cls.injected)

    def of_kind(self, kind):
        return [o for o in self.orders.values() if o.get("error") == kind]

    def test_without_rates_nothing_is_injected_and_zero_rates_change_nothing(self):
        self.assertEqual({}, synth.error_summary(self.base))
        zero = synth.build_scenarios(SKUS, dict(SHORT, error_rates=dict.fromkeys(synth.ERROR_KINDS, 0)))
        self.assertEqual(self.base, zero)

    def test_same_config_gives_the_same_errors_and_another_seed_gives_others(self):
        again = synth.build_scenarios(SKUS, dict(SHORT, error_rates=ALL_RATES))
        self.assertEqual(self.injected, again)
        other = synth.build_scenarios(SKUS, dict(SHORT, seed=SHORT["seed"] + 1, error_rates=ALL_RATES))
        self.assertNotEqual(synth.error_summary(self.injected), synth.error_summary(other))

    def test_orders_without_an_error_are_exactly_the_ones_without_injection(self):
        self.assertEqual(set(self.base_orders), set(self.orders))
        for key, order in self.orders.items():
            if "error" not in order:
                self.assertEqual(self.base_orders[key], order, key)

    def test_every_kind_appears_and_each_order_has_at_most_one_error(self):
        summary = synth.error_summary(self.injected)
        self.assertEqual(set(synth.ERROR_KINDS), set(summary), summary)
        self.assertEqual(sum(summary.values()), len([o for o in self.orders.values() if "error" in o]))

    def test_injected_scenarios_still_pass_the_validator(self):
        scenario_validator.validate_scenarios(self.injected, SKUS)

    def test_fault_goes_on_one_event_that_the_fault_can_be_applied_to(self):
        for order in self.of_kind("fault"):
            marked = [e for e in order["events"] if "fault" in e]
            self.assertEqual(1, len(marked))
            fault, event = marked[0]["fault"], marked[0]
            self.assertIn(fault, synth.FAULTS)
            if fault == "MISSING_REQUIRED":
                self.assertTrue(event["type"].startswith("PAYMENT_"))
            if fault == "INVALID_TYPE":
                self.assertEqual("ORDER_CREATED", event["type"])

    def test_double_payment_has_two_successful_payments_in_time_order(self):
        for order in self.of_kind("double_payment"):
            paid = [e for e in order["events"] if e["type"] == "PAYMENT_SUCCEEDED"]
            self.assertEqual(2, len(paid))
            self.assertLess(paid[0]["offset_sec"], paid[1]["offset_sec"])
            self.assertNotIn("send_count", paid[1])

    def test_duplicate_order_created_has_two_order_created_events(self):
        for order in self.of_kind("duplicate_order_created"):
            self.assertEqual(2, [e["type"] for e in order["events"]].count("ORDER_CREATED"))
            self.assertEqual("ORDER_CREATED", order["events"][0]["type"])

    def test_refund_to_failed_payment_has_a_failed_payment_before_the_refund(self):
        for order in self.of_kind("refund_to_failed_payment"):
            types = [e["type"] for e in order["events"]]
            refund = next(e for e in order["events"] if e["type"] == "REFUND_SUCCEEDED")
            self.assertEqual("failed", refund["payment"])
            self.assertLess(types.index("PAYMENT_FAILED"), types.index("REFUND_SUCCEEDED"))
            self.assertEqual(1, types.count("PAYMENT_SUCCEEDED"))

    def test_refund_order_mismatch_points_at_another_order_of_the_same_day(self):
        for key, order in ((k, o) for k, o in self.orders.items() if o.get("error") == "refund_order_mismatch"):
            refund = next(e for e in order["events"] if e["type"] == "REFUND_SUCCEEDED")
            day_orders = {no for (day, no) in self.orders if day == key[0]}
            self.assertIn(refund["order_no"], day_orders)
            self.assertNotEqual(order["order_no"], refund["order_no"])

    def test_duplicate_item_id_reuses_an_earlier_row_of_a_multi_item_order(self):
        for order in self.of_kind("duplicate_item_id"):
            reused = [(i, item["reuse_item_id_of"]) for i, item in enumerate(order["items"], 1) if "reuse_item_id_of" in item]
            self.assertEqual(1, len(reused))
            self.assertLess(reused[0][1], reused[0][0])

    def test_realized_share_follows_the_rate_for_a_kind_that_applies_to_every_order(self):
        scenarios = synth.build_scenarios(SKUS, dict(SHORT, error_rates={"fault": 0.1}))
        orders = [o for s in scenarios for o in s["orders"]]
        share = sum(1 for o in orders if o.get("error") == "fault") / len(orders)
        self.assertTrue(0.08 <= share <= 0.12, share)

    def test_kinds_that_do_not_fit_an_order_are_skipped_so_the_share_is_lower(self):
        scenarios = synth.build_scenarios(SKUS, dict(SHORT, error_rates={"refund_to_failed_payment": 0.5}))
        orders = [o for s in scenarios for o in s["orders"]]
        injected = [o for o in orders if o.get("error")]
        self.assertTrue(injected)
        self.assertLess(len(injected) / len(orders), 0.5 * 0.3)       # 환불 주문은 일부(약 14%)뿐이다
        self.assertTrue(all(any(e["type"] == "REFUND_SUCCEEDED" for e in o["events"]) for o in injected))

    def test_the_shipped_dump_config_has_valid_rates_for_every_kind(self):
        rates = synth.error_rates(DUMP_CONFIG)
        self.assertEqual(set(synth.ERROR_KINDS), set(rates))
        self.assertLessEqual(sum(rates.values()), 0.1)      # 오류가 데이터의 일부(10% 이하)다

    def test_bad_rates_are_refused(self):
        for bad in ({"nonsense": 0.1}, {"fault": -0.1}, {"fault": 1.5}, {"fault": 0.6, "double_payment": 0.6}, {"fault": "x"}):
            with self.assertRaises(ValueError, msg=bad):
                synth.error_rates(dict(SHORT, error_rates=bad))

    def test_the_generator_and_silver_conversion_treat_each_kind_as_designed(self):
        """채널 A 원본으로 만들어 Silver 행 변환에 넣는다: 격리 대상 오류와 items ID 중복만 격리되고 나머지 오류 주문은 통과한다."""
        lines = list(dict.fromkeys(generate.build_lines("A", self.injected, SKUS, NamedIds(7))))
        reasons = Counter()
        for line in lines:
            result = convert_row("A", json.loads(line)["payload"], KNOWN_SKUS)
            if result["reason_code"]:
                reasons[result["reason_code"]] += 1
        summary = synth.error_summary(self.injected)
        faults = sum(1 for o in self.orders.values() for e in o["events"] if "fault" in e)
        self.assertEqual(faults, sum(reasons[r] for r in synth.FAULTS))
        self.assertEqual(summary["duplicate_item_id"], reasons["INVALID_VALUE"])
        self.assertEqual(faults + summary["duplicate_item_id"], sum(reasons.values()))


if __name__ == "__main__":
    unittest.main()
