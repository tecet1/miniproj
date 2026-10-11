import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "01_contracts" / "src"))
CONTRACTS = ROOT / "01_contracts" / "contracts"

import hashlib
import json
import tempfile

import generate
import scenario_loader


def parse(line):
    outer = json.loads(line)
    return outer["channel"], json.loads(outer["payload"])


class TestGenerate(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.skus = scenario_loader.load_skus(CONTRACTS / "skus.json")
        cls.scenarios = scenario_loader.load_scenarios(CONTRACTS / "scenarios.json")

    def lines(self, channel):
        return generate.build_lines(channel, self.scenarios, self.skus)

    def test_24_lines_per_channel(self):
        for channel in "ABC":
            with self.subTest(channel=channel):
                self.assertEqual(len(self.lines(channel)), 24)

    def test_line_is_channel_and_payload_string(self):
        for channel in "ABC":
            outer = json.loads(self.lines(channel)[0])
            self.assertEqual(set(outer), {"channel", "payload"})
            self.assertEqual(outer["channel"], channel)
            self.assertIsInstance(outer["payload"], str)   # 원본 본문을 문자열 그대로 보존

    def test_only_s19_payment_is_duplicated(self):
        lines = self.lines("A")
        self.assertEqual(len(lines) - len(set(lines)), 1)
        dup = [l for l in lines if lines.count(l) == 2]
        self.assertEqual({parse(l)[1]["event_id"] for l in dup}, {"A-S19-1-E2"})

    def test_korean_text_is_not_escaped(self):
        self.assertIn("러닝화", self.lines("A")[0])

    def test_scenario_order_is_kept(self):
        order_ids = []
        for line in self.lines("A"):
            order_id = parse(line)[1]["order_id"]
            if order_id not in order_ids:
                order_ids.append(order_id)
        self.assertEqual(order_ids[:3], ["A-S01-1", "A-S02-1", "A-S02-2"])
        self.assertEqual(order_ids[-1], "A-S23-1")

    def test_s21_faults_in_channel_a(self):
        by_id = {parse(l)[1]["event_id"]: parse(l)[1] for l in self.lines("A")}
        self.assertNotIn("amount", by_id["A-S21-1-E1"])
        self.assertIsInstance(by_id["A-S21-2-E1"]["items"][0]["quantity"], str)
        self.assertEqual(by_id["A-S21-3-E1"]["event_type"], "shipping.started")

    def test_s23_midnight_boundary_in_each_format(self):
        pay_id = "-S23-1-E2"
        a = next(parse(l)[1] for l in self.lines("A") if parse(l)[1]["event_id"] == "A" + pay_id)
        b = next(parse(l)[1] for l in self.lines("B") if parse(l)[1]["eventId"] == "B" + pay_id)
        c = next(parse(l)[1] for l in self.lines("C") if parse(l)[1]["evt_id"] == "C" + pay_id)
        self.assertEqual(a["occurred_at"], "2026-10-11T15:30:00.000Z")
        self.assertEqual(b["occurredAtMs"], 1791732600000)
        self.assertEqual(c["evt_dt"], "2026-10-12 00:30:00")

    def test_same_scenario_has_same_event_ids_across_channels(self):
        def ids(channel, key):
            return [parse(l)[1][key][2:] for l in self.lines(channel)]
        self.assertEqual(ids("A", "event_id"), ids("B", "eventId"))
        self.assertEqual(ids("A", "event_id"), ids("C", "evt_id"))

    def test_output_is_deterministic(self):
        self.assertEqual(self.lines("B"), self.lines("B"))

    def test_write_fixtures_files_and_hash(self):
        def digest(path):
            return hashlib.sha256(path.read_bytes()).hexdigest()

        with tempfile.TemporaryDirectory() as t1, tempfile.TemporaryDirectory() as t2:
            generate.write_fixtures(Path(t1), self.scenarios, self.skus)
            generate.write_fixtures(Path(t2), self.scenarios, self.skus)
            for channel in "ABC":
                path = Path(t1) / channel / "events.jsonl"
                raw = path.read_bytes()
                self.assertNotIn(b"\r", raw)                       # 줄바꿈은 LF로 고정
                self.assertEqual(len(raw.decode("utf-8").splitlines()), 24)
                self.assertEqual(digest(path), digest(Path(t2) / channel / "events.jsonl"))


if __name__ == "__main__":
    unittest.main()
