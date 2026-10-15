"""silver_row_to_event.py 테스트. 오류 입력의 사유 코드를 확인한다."""
import json
import sys
import unittest
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "02_pipeline" / "silver"), str(ROOT / "01_contracts" / "src")]

import generate
import scenario_loader
from conversion_error import ConversionError
from silver_row_to_event import to_common_event

CONTRACTS = ROOT / "01_contracts" / "contracts"


def bronze_lines(channel):
    """시나리오에서 생성기가 만든 줄 ({"channel", "payload"}, Bronze 행과 같은 두 값). 파일을 읽지 않는다."""
    skus = scenario_loader.load_skus(CONTRACTS / "skus.json")
    scenarios = scenario_loader.load_scenarios(CONTRACTS / "scenarios.json")
    return [json.loads(line) for line in generate.build_lines(channel, scenarios, skus)]


def reason_of(channel, payload):
    """변환 오류의 reason_code를 돌려준다. 오류가 없으면 None."""
    try:
        to_common_event(channel, payload)
    except ConversionError as exc:
        return exc.reason_code
    return None


class TestBadInput(unittest.TestCase):
    def test_unknown_channel(self):
        for channel in ("X", "a", "", None):
            self.assertEqual(reason_of(channel, "{}"), "UNKNOWN_CHANNEL")

    def test_unknown_channel_wins_over_bad_payload(self):
        self.assertEqual(reason_of("X", "not json"), "UNKNOWN_CHANNEL")

    def test_malformed_json(self):
        for payload in ("not json", "", "{\"a\":", None):
            self.assertEqual(reason_of("A", payload), "MALFORMED_JSON")

    def test_json_but_not_object(self):
        for payload in ("[]", "123", "\"text\"", "null"):
            self.assertEqual(reason_of("A", payload), "MALFORMED_JSON")

    def test_channel_conversion_error_passes_through(self):
        self.assertEqual(reason_of("A", "{}"), "MISSING_REQUIRED")

    def test_invalid_value_from_validation(self):
        line = bronze_lines("A")[1]            # A-S01-1-E2 결제 성공
        payload = json.loads(line["payload"])
        payload["amount"] = -1
        self.assertEqual(reason_of("A", json.dumps(payload)), "INVALID_VALUE")

    def test_invalid_value_zero_quantity(self):
        line = bronze_lines("B")[0]            # B-S01-1-E1 주문 생성
        payload = json.loads(line["payload"])
        payload["orderItems"][0]["qty"] = 0
        self.assertEqual(reason_of("B", json.dumps(payload)), "INVALID_VALUE")


if __name__ == "__main__":
    unittest.main()
