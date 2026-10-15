"""ID 생성 방식(ids.py) 테스트. 규칙은 event_spec.md 1.1절 "ID 규칙"이다.

- ReadableIds는 고정 픽스처의 기존 규칙과 같고, 생성기가 다시 만든 픽스처가 저장소 파일과 바이트까지 같다.
- NamedIds는 같은 입력이면 같은 UUID, 시드·채널·번호가 다르면 다른 UUID이며 만드는 순서와 무관하다.
- RandomIds는 호출마다 다른 UUID(v4)다.
- UUID ID로 만든 원본 이벤트도 파이프라인의 행 변환(convert_row)을 격리 없이 통과하고 event_id가 그대로 나온다.
"""
import json
import re
import sys
import unittest
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "01_contracts" / "src"))
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))
CONTRACTS = ROOT / "01_contracts" / "contracts"
FIXTURES = ROOT / "01_contracts" / "data_fixtures"

import canonical  # noqa: E402
import generate  # noqa: E402
import ids  # noqa: E402
import scenario_loader  # noqa: E402
from convert_row import convert_row  # noqa: E402
from validate_skus import load_known_skus  # noqa: E402

UUID_TEXT = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
SKUS = scenario_loader.load_skus(CONTRACTS / "skus.json")
SCENARIOS = scenario_loader.load_scenarios(CONTRACTS / "scenarios.json")
KNOWN = load_known_skus(CONTRACTS / "skus.json")


def all_ids(events: list) -> list:
    """이벤트 목록의 event_id, order_id, payment_id, refund_id를 순서대로 모은다 (order_item_id는 따로)."""
    found = []
    for event in events:
        found += [event["event_id"], event["order_id"]]
        found += [event[key] for key in ("payment_id", "refund_id") if key in event]
    return found


def raw_event_id(payload: str) -> str:
    """채널별 원본 payload에서 이벤트 ID 필드를 꺼낸다 (A event_id, B eventId, C evt_id)."""
    raw = json.loads(payload)
    return next(raw[key] for key in ("event_id", "eventId", "evt_id") if key in raw)


class TestReadableIds(unittest.TestCase):
    def test_rules_match_scenarios_md(self):
        r = ids.ReadableIds()
        order = r.order_id("A", "S01", 1)
        self.assertEqual(("A-S01-1", "A-S01-1-E1", "A-S01-1-P1", "A-S09-1-R1", "A-S01-1-1"),
                         (order, r.event_id(order, 1), r.payment_id(order, 1), r.refund_id("A-S09-1", 1),
                          r.order_item_id(order, 1)))

    def test_regenerated_fixtures_are_identical_to_the_checked_in_files(self):
        for channel in "ABC":
            with self.subTest(channel=channel):
                lines = generate.build_lines(channel, SCENARIOS, SKUS)
                text = "\n".join(lines) + "\n"
                self.assertEqual((FIXTURES / channel / "events.jsonl").read_bytes(), text.encode("utf-8"))


class TestNamedIds(unittest.TestCase):
    def test_same_input_gives_same_uuid_v5(self):
        a, b = ids.NamedIds(7), ids.NamedIds(7)
        order = a.order_id("A", "L00001", 1)
        self.assertEqual(order, b.order_id("A", "L00001", 1))
        self.assertTrue(UUID_TEXT.match(order))
        self.assertEqual(5, uuid.UUID(order).version)

    def test_seed_channel_and_number_change_the_id(self):
        base = ids.NamedIds(7).order_id("A", "L00001", 1)
        self.assertNotEqual(base, ids.NamedIds(8).order_id("A", "L00001", 1))
        self.assertNotEqual(base, ids.NamedIds(7).order_id("B", "L00001", 1))
        self.assertNotEqual(base, ids.NamedIds(7).order_id("A", "L00001", 2))
        self.assertNotEqual(base, ids.NamedIds(7).order_id("A", "L00002", 1))

    def test_kinds_and_numbers_do_not_collide(self):
        n = ids.NamedIds(1)
        order = n.order_id("A", "L1", 1)
        values = [order, n.event_id(order, 1), n.event_id(order, 2), n.payment_id(order, 1), n.refund_id(order, 1)]
        self.assertEqual(len(values), len(set(values)))

    def test_order_item_id_is_derived_from_the_order(self):
        n = ids.NamedIds(1)
        order = n.order_id("A", "L1", 1)
        self.assertEqual(f"{order}-2", n.order_item_id(order, 2))

    def test_building_order_does_not_change_ids(self):
        forward = canonical.build_events(SCENARIOS[1], SKUS, "B", ids.NamedIds(3))
        canonical.build_events(SCENARIOS[0], SKUS, "B", ids.NamedIds(3))   # 다른 시나리오를 먼저 만들어도
        again = canonical.build_events(SCENARIOS[1], SKUS, "B", ids.NamedIds(3))
        self.assertEqual(all_ids(forward), all_ids(again))


class TestRandomIds(unittest.TestCase):
    def test_every_call_gives_a_new_uuid_v4(self):
        r = ids.RandomIds()
        values = [r.event_id("x", 1) for _ in range(1000)]
        self.assertEqual(1000, len(set(values)))
        self.assertTrue(all(UUID_TEXT.match(v) and uuid.UUID(v).version == 4 for v in values))

    def test_order_item_id_keeps_the_order_prefix(self):
        r = ids.RandomIds()
        order = r.order_id("A", "S01", 1)
        self.assertEqual(f"{order}-1", r.order_item_id(order, 1))


class TestUuidIdsInTheGenerator(unittest.TestCase):
    """UUID ID로 만든 이벤트의 참조(결제·환불·상품 행)가 이어지고, 채널과 방식이 달라도 ID가 유일하다."""

    def build(self, scenario, channel, style):
        return canonical.build_events(scenario, SKUS, channel, style)

    def test_refund_points_to_the_succeeded_payment_and_the_item_row(self):
        s09 = next(s for s in SCENARIOS if s["id"] == "S09")
        for style in (ids.NamedIds(1), ids.RandomIds()):
            with self.subTest(style=type(style).__name__):
                events = self.build(s09, "A", style)
                created = next(e for e in events if e["event_type"] == "ORDER_CREATED")
                paid = next(e for e in events if e["event_type"] == "PAYMENT_SUCCEEDED")
                refund = next(e for e in events if e["event_type"] == "REFUND_SUCCEEDED")
                self.assertEqual(paid["payment_id"], refund["payment_id"])
                self.assertIn(refund["order_item_id"], [i["order_item_id"] for i in created["items"]])
                self.assertTrue(all(e["order_id"] == created["order_id"] for e in events))

    def test_channels_get_different_ids_for_the_same_scenario(self):
        s01 = next(s for s in SCENARIOS if s["id"] == "S01")
        a = all_ids(self.build(s01, "A", ids.NamedIds(1)))
        b = all_ids(self.build(s01, "B", ids.NamedIds(1)))
        self.assertTrue(set(a).isdisjoint(b))

    def test_all_event_ids_are_unique_within_a_channel(self):
        """정상 시나리오만 본다. 오류 사례 S25(생성 이벤트 중복)·S28(상품 ID 중복)은 order_item_id가 겹치도록 만든 것이다."""
        for channel in "ABC":
            events = [e for s in SCENARIOS if s["id"] not in ("S25", "S28")
                      for e in self.build(s, channel, ids.NamedIds(1))]
            event_ids = [e["event_id"] for e in events]
            self.assertEqual(len(event_ids), len(set(event_ids)), channel)
            item_ids = [i["order_item_id"] for e in events if e["event_type"] == "ORDER_CREATED" for i in e["items"]]
            self.assertEqual(len(item_ids), len(set(item_ids)), channel)

    def test_uuid_events_pass_the_pipeline_row_conversion(self):
        """Silver 행 변환이 ID 형식에 의존하지 않는다: 격리 없이 통과하고 event_id가 같다."""
        for style in (ids.NamedIds(5), ids.RandomIds()):
            for channel in "ABC":
                lines = generate.build_lines(channel, [s for s in SCENARIOS if s["id"] != "S28" and not any(
                    ev.get("fault") for o in s["orders"] for ev in o["events"])], SKUS, style)    # S28은 INVALID_VALUE로 격리되는 오류 사례
                seen = 0
                for line in lines:
                    outer = json.loads(line)
                    result = convert_row(channel, outer["payload"], KNOWN)
                    with self.subTest(style=type(style).__name__, channel=channel):
                        self.assertIsNone(result["reason_code"], outer["payload"][:120])
                        self.assertEqual(raw_event_id(outer["payload"]), result["event_id"])
                        self.assertTrue(UUID_TEXT.match(result["event_id"]))
                    seen += 1
                self.assertGreater(seen, 0)


if __name__ == "__main__":
    unittest.main()
