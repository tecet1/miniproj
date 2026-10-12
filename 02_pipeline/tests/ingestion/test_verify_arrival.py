import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "ingestion"))

import json
import os
import tempfile
from collections import Counter

import verify_arrival


class TestCompare(unittest.TestCase):
    def test_identical_has_no_problems(self):
        c = Counter({("A", "p1"): 1, ("B", "p2"): 2})
        self.assertEqual(verify_arrival.compare(c, Counter(c)), [])

    def test_missing_event_is_reported(self):
        problems = verify_arrival.compare(Counter({("A", "p1"): 1}), Counter())
        self.assertEqual(len(problems), 1)
        self.assertIn("missing", problems[0])

    def test_unexpected_event_is_reported(self):
        problems = verify_arrival.compare(Counter(), Counter({("C", "px"): 1}))
        self.assertEqual(len(problems), 1)
        self.assertIn("unexpected", problems[0])

    def test_duplicate_count_difference_is_reported(self):
        # S19: 같은 이벤트가 2건 도착해야 한다. 1건만 오면 부족으로 보고한다.
        problems = verify_arrival.compare(Counter({("A", "p"): 2}), Counter({("A", "p"): 1}))
        self.assertEqual(len(problems), 1)
        self.assertIn("x1", problems[0])


class TestLoadExpected(unittest.TestCase):
    def test_counts_channel_payload_pairs_including_duplicates(self):
        with tempfile.TemporaryDirectory() as tmp:
            for channel, payloads in {"A": ["p1", "p1"], "B": ["p2"], "C": ["p3"]}.items():
                path = f"{tmp}/{channel}"
                os.makedirs(path)
                with open(f"{path}/events.jsonl", "w", encoding="utf-8", newline="\n") as f:
                    f.write("".join(json.dumps({"channel": channel, "payload": p}) + "\n" for p in payloads))
            expected = verify_arrival.load_expected(tmp)
        self.assertEqual(expected, Counter({("A", "p1"): 2, ("B", "p2"): 1, ("C", "p3"): 1}))


if __name__ == "__main__":
    unittest.main()
