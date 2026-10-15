"""Bronze 행 하나 -> Spark UDF가 돌려줄 결과(convert_row) 테스트.

[계약] convert_row(channel, payload, known_skus) -> dict
  성공: {"event_id", "event_type", "event_json", "reason_code": None}
  실패: {"event_id": None, "event_type": None, "event_json": None, "reason_code": <사유>}
  event_json은 공통 이벤트를 JSON으로 쓴 문자열이며 event_time은 "2026-10-12T01:00:00.000Z" 형식이다.
"""
import json
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "02_pipeline" / "silver"), str(ROOT / "01_contracts" / "src")]

import generate  # noqa: E402
import scenario_loader  # noqa: E402
from convert_row import convert_row  # noqa: E402
from silver_tables import EVENT_SCHEMA  # noqa: E402
from validate_skus import load_known_skus  # noqa: E402

CONTRACTS = ROOT / "01_contracts" / "contracts"
KNOWN = load_known_skus(CONTRACTS / "skus.json")
SKUS = scenario_loader.load_skus(CONTRACTS / "skus.json")
SCENARIOS = scenario_loader.load_scenarios(CONTRACTS / "scenarios.json")

ORDER_A = json.dumps({
    "event_id": "A-X-E1", "event_type": "order.created", "occurred_at": "2026-10-12T01:00:00.000Z",
    "order_id": "A-X", "user_id": None, "shipping_fee": 0,
    "items": [{"order_item_id": "A-X-1", "sku": "SKU-004", "product_name": "러닝화", "category": "신발",
               "quantity": 1, "unit_price": 89000, "discount_amount": 0}]})


def scenario_rows(channel: str) -> list:
    """시나리오에서 생성기가 만든 채널 원본 줄 ({"channel", "payload"}). 파일을 읽지 않는다."""
    return [json.loads(line) for line in generate.build_lines(channel, SCENARIOS, SKUS)]


def first_payload(channel: str) -> str:
    return scenario_rows(channel)[0]["payload"]


class TestSuccess(unittest.TestCase):
    def test_result_shape(self):
        result = convert_row("A", ORDER_A, KNOWN)
        self.assertEqual({"event_id", "event_type", "event_json", "reason_code"}, set(result))
        self.assertEqual("A-X-E1", result["event_id"])
        self.assertEqual("ORDER_CREATED", result["event_type"])
        self.assertIsNone(result["reason_code"])

    def test_event_json_carries_common_event(self):
        event = json.loads(convert_row("A", ORDER_A, KNOWN)["event_json"])
        self.assertEqual("A", event["channel"])
        self.assertEqual("A-X", event["order_id"])
        self.assertIsNone(event["user_id"])                       # 비로그인은 null
        self.assertEqual("2026-10-12T01:00:00.000Z", event["event_time"])
        self.assertEqual("SKU-004", event["items"][0]["product_id"])

    def test_c_channel_time_is_converted_to_utc_string(self):
        result = convert_row("C", first_payload("C"), KNOWN)      # C 원본은 KST 문자열
        event = json.loads(result["event_json"])
        self.assertTrue(event["event_time"].endswith("Z"))
        self.assertEqual("2026-10-12T01:00:00.000Z", event["event_time"])   # KST 10:00 = UTC 01:00

    def test_event_json_keys_are_declared_in_event_schema(self):
        """event_json의 모든 키가 EVENT_SCHEMA에 있어야 Spark from_json이 값을 버리지 않는다."""
        declared = {top.strip().split(" ")[0] for top in EVENT_SCHEMA.split(", ") if " " in top.strip()}
        for channel in "ABC":
            for row in scenario_rows(channel):
                result = convert_row(row["channel"], row["payload"], KNOWN)
                if result["reason_code"]:
                    continue
                keys = set(json.loads(result["event_json"]))
                self.assertLessEqual(keys, declared | {"items"}, keys - declared)


class TestFailure(unittest.TestCase):
    def assertQuarantined(self, result, reason_code):
        self.assertEqual(reason_code, result["reason_code"])
        self.assertIsNone(result["event_id"])
        self.assertIsNone(result["event_type"])
        self.assertIsNone(result["event_json"])

    def test_unknown_channel(self):
        self.assertQuarantined(convert_row("Z", ORDER_A, KNOWN), "UNKNOWN_CHANNEL")

    def test_malformed_json(self):
        self.assertQuarantined(convert_row("A", "{not json", KNOWN), "MALFORMED_JSON")

    def test_missing_required(self):
        payload = json.dumps({**json.loads(ORDER_A), "order_id": None})
        self.assertQuarantined(convert_row("A", payload, KNOWN), "MISSING_REQUIRED")

    def test_invalid_value_from_range_check(self):
        raw = json.loads(ORDER_A)
        raw["items"][0]["quantity"] = 0
        self.assertQuarantined(convert_row("A", json.dumps(raw), KNOWN), "INVALID_VALUE")

    def test_unknown_sku_is_invalid_value(self):
        raw = json.loads(ORDER_A)
        raw["items"][0]["sku"] = "SKU-999"
        self.assertQuarantined(convert_row("A", json.dumps(raw), KNOWN), "INVALID_VALUE")


if __name__ == "__main__":
    unittest.main()
