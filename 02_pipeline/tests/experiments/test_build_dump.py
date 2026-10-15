"""build_dump 테스트 (pyspark 없이). 덤프 줄 수와 합친 기대값의 일치, 재현성, ID 유일성, 기대값의 출처를 확인한다."""
import json
import sys
import tempfile
import unittest
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "04_experiments" / "src"), str(ROOT / "01_contracts" / "src")]

import build_dump  # noqa: E402
import expected  # noqa: E402
import scenario_loader  # noqa: E402
import synth_scenarios as synth  # noqa: E402

CONTRACTS = ROOT / "01_contracts" / "contracts"
FIXTURES = ROOT / "01_contracts" / "data_fixtures"
SKUS = scenario_loader.load_skus(CONTRACTS / "skus.json")
FIXTURE_SCENARIOS = scenario_loader.load_scenarios(CONTRACTS / "scenarios.json")
CONFIG = dict(json.loads(build_dump.DEFAULT_CONFIG.read_text(encoding="utf-8")), error_rates={})   # 오류 주입은 test_synth_scenarios가 본다
CHANNELS = ("A", "B", "C")


def read_lines(directory, channel):
    return (Path(directory) / channel / "events.jsonl").read_text(encoding="utf-8").splitlines()


_SHARED = {}


def shared_dump():
    """1년치 생성이 느려서 모듈 안에서 한 번만 만든다."""
    if not _SHARED:
        out = tempfile.TemporaryDirectory()
        combined = build_dump.write_dump(out.name, CONFIG, SKUS, FIXTURE_SCENARIOS)
        synthetic, _ = build_dump.build_dump(CONFIG, SKUS, FIXTURE_SCENARIOS)
        _SHARED.update(tmp=out, out=Path(out.name), combined=combined, synthetic=synthetic,
                       dump_only=expected.compute_expected([], list(SKUS.values()), channel_scenarios=synthetic))
    return _SHARED


def tearDownModule():
    if _SHARED:
        _SHARED["tmp"].cleanup()


class _Dump(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        for key, value in shared_dump().items():
            if key != "tmp":
                setattr(cls, key, value)


class TestLineCounts(_Dump):
    def test_dump_lines_match_expected_bronze_rows_of_the_synthetic_scenarios(self):
        for channel in CHANNELS:
            self.assertEqual(self.dump_only["per_channel"][channel]["bronze_rows"],
                             len(read_lines(self.out / "dump", channel)), channel)

    def test_dump_plus_fixture_lines_match_combined_bronze_rows(self):
        for channel in CHANNELS:
            fixture_lines = len(read_lines(FIXTURES, channel))
            total = len(read_lines(self.out / "dump", channel)) + fixture_lines
            self.assertEqual(self.combined["per_channel"][channel]["bronze_rows"], total, channel)

    def test_dump_contains_duplicate_sends_so_lines_exceed_distinct_events(self):
        lines = read_lines(self.out / "dump", "A")
        self.assertLess(len(set(lines)), len(lines))


class TestCombinedExpected(_Dump):
    def test_combined_is_one_compute_expected_call_over_fixtures_and_synthetic(self):
        again = expected.compute_expected(FIXTURE_SCENARIOS, list(SKUS.values()), channel_scenarios=self.synthetic)
        self.assertEqual(again, self.combined)

    def test_combined_adds_up_fixture_and_dump_for_additive_counts(self):
        fixture_only = expected.compute_expected(FIXTURE_SCENARIOS, list(SKUS.values()))
        for key in ("bronze_rows", "silver_orders", "silver_payments", "fact_sales_item", "total_sales", "quarantine"):
            self.assertEqual(fixture_only["total"][key] + self.dump_only["total"][key], self.combined["total"][key], key)

    def test_quarantine_comes_only_from_fixtures(self):
        self.assertEqual(0, self.dump_only["total"]["quarantine"])
        self.assertGreater(self.combined["total"]["quarantine"], 0)

    def test_channels_differ_in_scale_with_the_configured_shares(self):
        orders = {c: self.dump_only["per_channel"][c]["silver_orders"] for c in CHANNELS}
        self.assertGreater(orders["B"], orders["C"])
        self.assertGreater(orders["C"], orders["A"])
        total = sum(orders.values())
        for channel in CHANNELS:      # 해 평균 비중은 시작 비중에서 성장 차이만큼 움직인다 (A 20->21%, B 50->46%, C 30->34%)
            self.assertAlmostEqual(CONFIG["channels"][channel]["share"], orders[channel] / total, delta=0.04, msg=channel)

    def test_each_channel_matches_the_model_expectation_of_its_own_stream(self):
        for channel in CHANNELS:
            stream = synth.channel_config(CONFIG, channel)
            model = sum(mean for _, mean in synth.daily_mean_orders(stream))
            self.assertAlmostEqual(1.0, self.dump_only["per_channel"][channel]["silver_orders"] / model, delta=0.04, msg=channel)

    def test_channel_scenarios_are_different_streams(self):
        self.assertNotEqual(self.synthetic["A"][100]["orders"], self.synthetic["B"][100]["orders"])

    def test_channel_mix_shifts_toward_the_faster_growing_channel(self):
        def share(channel, months):
            count = lambda c: sum(len(s["orders"]) for s in self.synthetic[c] if s["start_kst"][:7] in months)
            return count(channel) / sum(count(c) for c in CHANNELS)
        early, late = ("2025-11", "2025-12", "2026-01"), ("2026-07", "2026-08", "2026-09")
        self.assertGreater(share("C", late), share("C", early))
        self.assertLess(share("B", late), share("B", early))

    def test_fixture_only_expected_still_equal_across_channels(self):
        fixture_only = expected.compute_expected(FIXTURE_SCENARIOS, list(SKUS.values()))["per_channel"]
        self.assertEqual(fixture_only["A"], fixture_only["B"])
        self.assertEqual(fixture_only["A"], fixture_only["C"])

    def test_written_file_equals_returned_expected(self):
        written = json.loads((self.out / "expected_dump.json").read_text(encoding="utf-8"))
        self.assertEqual(self.combined, written)


class TestReproducibility(_Dump):
    def test_same_config_gives_identical_bytes(self):
        with tempfile.TemporaryDirectory() as other:
            build_dump.write_dump(other, CONFIG, SKUS, FIXTURE_SCENARIOS)
            names = [Path("expected_dump.json")] + [Path("dump") / c / "events.jsonl" for c in CHANNELS]
            for name in names:
                self.assertEqual((self.out / name).read_bytes(), (Path(other) / name).read_bytes(), str(name))

    def test_different_seed_changes_event_ids(self):
        with tempfile.TemporaryDirectory() as other:
            build_dump.write_dump(other, dict(CONFIG, seed=CONFIG["seed"] + 1), SKUS, FIXTURE_SCENARIOS)
            self.assertNotEqual((self.out / "dump" / "A" / "events.jsonl").read_bytes(),
                                (Path(other) / "dump" / "A" / "events.jsonl").read_bytes())


class TestIds(_Dump):
    def test_event_ids_are_unique_within_a_channel_and_duplicates_only_by_resend(self):
        for channel in CHANNELS:
            lines = read_lines(self.out / "dump", channel)
            by_line = Counter(lines)
            self.assertTrue(all(count <= 2 for count in by_line.values()), channel)
            ids = [self._event_id(channel, line) for line in by_line]
            self.assertEqual(len(ids), len(set(ids)), channel)
            self.assertEqual(len(lines) - len(by_line), sum(1 for c in by_line.values() if c == 2))

    def test_dump_ids_are_lowercase_uuid_strings_and_do_not_collide_with_fixture_ids(self):
        fixture_ids = {self._event_id("A", line) for line in read_lines(FIXTURES, "A")}
        for line in set(read_lines(self.out / "dump", "A")):
            event_id = self._event_id("A", line)
            self.assertRegex(event_id, r"^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
            self.assertNotIn(event_id, fixture_ids)

    @staticmethod
    def _event_id(channel, line):
        """원본 payload의 이벤트 ID. 채널별 필드 이름이 다르다 (A event_id, B eventId, C evt_id)."""
        payload = json.loads(json.loads(line)["payload"])
        return payload[{"A": "event_id", "B": "eventId", "C": "evt_id"}[channel]]


if __name__ == "__main__":
    unittest.main()
