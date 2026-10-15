"""지연 관찰 함수(latency_probe)를 가짜 시계·가짜 조회로 확인한다."""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "02_pipeline" / "quality")]

import latency_probe as L  # noqa: E402


class FakeWorld:
    """시각이 sleep으로만 흐르고, 계층마다 정해진 시각 이후에 주문이 보이는 세계."""

    def __init__(self, visible_at):
        self.now, self.visible_at = 0.0, visible_at

    def clock(self):
        return self.now

    def sleep(self, seconds):
        self.now += seconds

    def count_of(self, sql):
        layer = next(name for name in ("bronze", "silver", "gold") if f"commerce_poc.{name}." in sql)
        at = self.visible_at[layer]
        return 1 if at is not None and self.now >= at else 0


class TestWatch(unittest.TestCase):
    def run_watch(self, visible_at, **kwargs):
        world = FakeWorld(visible_at)
        return L.watch(world.count_of, "P1", clock=world.clock, sleep=world.sleep, interval_seconds=2, **kwargs)

    def test_records_the_first_polling_time_each_layer_sees_the_order(self):
        seen = self.run_watch({"bronze": 3, "silver": 11, "gold": 30})
        self.assertEqual({"bronze": 4.0, "silver": 12.0, "gold": 30.0}, seen)

    def test_missing_layer_stays_none_after_timeout(self):
        seen = self.run_watch({"bronze": 1, "silver": 5, "gold": None}, timeout_seconds=20)
        self.assertEqual({"bronze": 2.0, "silver": 6.0, "gold": None}, seen)

    def test_summarize_gives_per_stage_delay_and_none_after_a_gap(self):
        out = L.summarize({"bronze": 4.0, "silver": 12.0, "gold": 30.0})
        self.assertEqual((4.0, 8.0, 18.0), (out["bronze 구간(초)"], out["silver 구간(초)"], out["gold 구간(초)"]))
        out = L.summarize({"bronze": 4.0, "silver": None, "gold": 30.0})
        self.assertEqual((4.0, None, None), (out["bronze 구간(초)"], out["silver 구간(초)"], out["gold 구간(초)"]))

    def test_order_id_is_validated_because_it_goes_into_sql(self):
        for bad in ("", "x' OR '1'='1", "a b", "a;DROP"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                L.probe_sql("bronze", bad)
        self.assertIn("'ORD-1_a'", L.probe_sql("silver", "ORD-1_a"))


if __name__ == "__main__":
    unittest.main()
