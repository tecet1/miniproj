"""native_convert(Spark 표현식 변환)를 기존 파이썬 변환(convert_row)과 행 단위로 대조한다 (로컬 Spark가 없으면 건너뛴다).

[방법] 같은 Bronze 줄 (시나리오 전체, 오류를 높은 비율로 주입한 합성 데이터, 필드를 하나씩 망가뜨린 변형 줄, 알 수 없는 채널·깨진 JSON)에
  파이썬 변환(convert_row, 기준 구현)과 네이티브 변환을 돌려 격리 사유, event_id, event_type, 공통 이벤트의 모든 필드가 같은지 본다.
[확인하지 못하는 것] Databricks(ANSI 모드, Delta) 실행. 숫자가 문자열로 오는 경우 등 native_convert 머리말의 한계는 입력에 넣지 않았다.
"""
import json
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "02_pipeline" / "silver"), str(ROOT / "02_pipeline" / "tests"), str(ROOT / "02_pipeline" / "tests" / "gold"), str(ROOT / "04_experiments" / "src"),
                str(ROOT / "01_contracts" / "src")]

import generate  # noqa: E402
import _silver_rows as sr  # noqa: E402
import native_convert as N  # noqa: E402
import silver_stream  # noqa: E402
import silver_tables  # noqa: E402
import scenario_loader  # noqa: E402
import synth_scenarios as synth  # noqa: E402
from convert_row import convert_row  # noqa: E402
from ids import NamedIds  # noqa: E402
from spark_support import local_spark  # noqa: E402
from synthetic_inputs import crafted_lines, scenario_lines  # noqa: E402
from validate_skus import load_known_skus  # noqa: E402

SPARK = local_spark()
CONTRACTS = ROOT / "01_contracts" / "contracts"
KNOWN = load_known_skus(CONTRACTS / "skus.json")
CONFIG = json.loads((ROOT / "04_experiments" / "configs" / "dump.json").read_text(encoding="utf-8"))
HIGH_RATES = {"fault": 0.03, "double_payment": 0.03, "duplicate_order_created": 0.03, "refund_to_failed_payment": 0.2,
              "refund_order_mismatch": 0.2, "duplicate_item_id": 0.05}
UTC = timezone.utc
EVENT_FIELDS = ("event_id", "event_type", "channel", "order_id", "user_id", "event_time", "shipping_fee", "items", "cancel_reason_code",
                "payment_id", "payment_method", "payment_amount", "fail_reason_code", "refund_id", "order_item_id", "refund_quantity",
                "refund_amount", "refund_type", "refund_reason_code")


def iso(moment: datetime) -> str:
    return moment.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.") + f"{moment.microsecond // 1000:03d}Z"


def odd_lines():
    """채널·JSON 형태가 이상한 줄들."""
    return [{"channel": "A", "payload": "not json"}, {"channel": "X", "payload": "{}"}, {"channel": "B", "payload": "{\"eventName\": \"UNKNOWN\"}"},
            {"channel": "C", "payload": "[1, 2]"}, {"channel": "A", "payload": "{}"}, {"channel": "A", "payload": ""},
            {"channel": "B", "payload": "123"}, {"channel": "C", "payload": "{\"evt_cd\": \"O01\"}"}]


def normalized_native(e):
    """네이티브 e 구조체를 파이썬 변환 결과(event_json)와 비교할 수 있는 dict로."""
    if e is None:
        return None
    data = e.asDict(recursive=True)
    data["event_time"] = iso(data["event_time"]) if data.get("event_time") is not None else None
    return {key: data.get(key) for key in EVENT_FIELDS}


def normalized_python(result):
    if result["event_json"] is None:
        return None
    event = json.loads(result["event_json"])
    return {key: event.get(key) for key in EVENT_FIELDS}


@unittest.skipUnless(SPARK, "pyspark/Java가 없으면 로컬 Spark 검증을 건너뛴다")
class TestNativeMatchesPython(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        skus = scenario_loader.load_skus(CONTRACTS / "skus.json")
        scenarios = scenario_loader.load_scenarios(CONTRACTS / "scenarios.json")
        lines = []
        for channel in "ABC":
            lines += [json.loads(l) for l in scenario_lines(channel)]
        sample = synth.build_scenarios(skus, dict(CONFIG, start="2025-11-03", end="2025-11-05", error_rates=HIGH_RATES, base_orders_per_day=60))
        for channel in "ABC":
            lines += [json.loads(l) for l in generate.build_lines(channel, sample, skus, NamedIds(channel))]
        lines += crafted_lines() + odd_lines()
        cls.lines = lines
        frame = SPARK.createDataFrame([(l["channel"], l["payload"], i) for i, l in enumerate(lines)], "channel string, payload string, eh_offset bigint")
        cls.native = {row["eh_offset"]: row for row in N.convert(frame, KNOWN).collect()}
        cls.python = {i: convert_row(l["channel"], l["payload"], KNOWN) for i, l in enumerate(lines)}

    def test_the_input_covers_normal_and_every_kind_of_rejection(self):
        reasons = {r["reason_code"] for r in self.python.values()}
        self.assertEqual({None, "UNKNOWN_CHANNEL", "MALFORMED_JSON", "MISSING_REQUIRED", "INVALID_TYPE", "UNKNOWN_EVENT_TYPE", "INVALID_VALUE"}, reasons)
        self.assertGreater(len(self.lines), 900)

    def test_every_row_gets_the_same_reason_code(self):
        wrong = [(i, self.python[i]["reason_code"], self.native[i]["result"]["reason_code"], self.lines[i]["payload"][:90])
                 for i in self.python if self.python[i]["reason_code"] != self.native[i]["result"]["reason_code"]]
        self.assertEqual([], wrong[:5], f"{len(wrong)}행이 다르다")

    def test_event_id_and_type_follow_the_python_result(self):
        for i, result in self.python.items():
            native = self.native[i]["result"]
            self.assertEqual((result["event_id"], result["event_type"]), (native["event_id"], native["event_type"]), self.lines[i]["payload"][:90])

    def test_accepted_rows_have_the_same_common_event(self):
        wrong = [(i, normalized_python(self.python[i]), normalized_native(self.native[i]["e"]))
                 for i in self.python if self.python[i]["reason_code"] is None
                 and normalized_python(self.python[i]) != normalized_native(self.native[i]["e"])]
        self.assertEqual([], wrong[:2], f"{len(wrong)}행이 다르다")

    def test_rejected_rows_have_no_event(self):
        for i, result in self.python.items():
            if result["reason_code"]:
                self.assertIsNone(self.native[i]["e"])

    def test_native_conversion_uses_no_python_udf(self):
        text = (ROOT / "02_pipeline" / "silver" / "native_convert.py").read_text(encoding="utf-8")
        self.assertNotIn("udf(", text.replace("UDF 없이", "").replace("UDF를 쓰지 않는다", ""))
        self.assertNotIn("pandas_udf", text)
        self.assertNotIn("convert_row", text.split('"""', 2)[2])      # 코드에서 파이썬 변환을 부르지 않는다


def as_utc(value):
    """TIMESTAMP 값을 UTC 시각으로 맞춘다. 기준 구현의 시각 문자열("2026-10-12T01:00:00.000Z")과 Spark datetime 모두 받는다."""
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value.astimezone(UTC)      # collect()가 돌려주는 naive datetime은 이 컴퓨터의 로컬 시각이다 (astimezone이 로컬로 읽어 UTC로 바꾼다)


@unittest.skipUnless(SPARK, "pyspark/Java가 없으면 로컬 Spark 검증을 건너뛴다")
class TestSilverTablesFromNativeConversion(unittest.TestCase):
    """native 변환 -> 중복 제거 -> 테이블 빌더를 끝까지 돌려, 기준 구현(convert_row + Silver 행 나누기)이 만든 Silver 5개 테이블과 같은지 본다 (Delta MERGE 제외)."""

    @classmethod
    def setUpClass(cls):
        skus = scenario_loader.load_skus(CONTRACTS / "skus.json")
        lines = []
        for channel in "ABC":
            lines += [json.loads(l) for l in scenario_lines(channel)]
        sample = synth.build_scenarios(skus, dict(CONFIG, start="2025-11-03", end="2025-11-04", error_rates=HIGH_RATES, base_orders_per_day=50))
        for channel in "ABC":
            lines += [json.loads(l) for l in generate.build_lines(channel, sample, skus, NamedIds(channel))]
        lines += crafted_lines() + odd_lines()
        base = datetime(2026, 10, 12, tzinfo=UTC)
        rows = [(l["channel"], base + timedelta(seconds=i), l["payload"], 0, i, base, base) for i, l in enumerate(lines)]
        frame = SPARK.createDataFrame(rows, "channel string, received_at timestamp, payload string, eh_partition int, eh_offset bigint, "
                                             "eh_enqueued_at timestamp, ingested_at timestamp")
        converted = silver_stream.convert_bronze(frame, KNOWN)
        events = silver_stream.parse_events(silver_stream.dedupe_events(converted))
        cls.native = {table: [r.asDict(recursive=True) for r in build(events).collect()] for table, build in silver_stream.BUILDERS.items()}
        cls.native["quarantine"] = [r.asDict() for r in silver_stream.build_quarantine(converted).collect()]
        kept, seen = [], set()
        cls.rejected = []
        for i, line in enumerate(lines):
            result = convert_row(line["channel"], line["payload"], KNOWN)
            if result["reason_code"]:
                cls.rejected.append((i, result["reason_code"]))
            elif (line["channel"], result["event_id"]) not in seen:
                seen.add((line["channel"], result["event_id"]))
                kept.append(json.loads(result["event_json"]))
        cls.python = sr.to_silver(kept)

    def keyed(self, table, rows):
        columns = [name for name, sql_type in silver_tables.TABLES[table]["columns"] if name != "received_at"]
        timestamps = {name for name, sql_type in silver_tables.TABLES[table]["columns"] if sql_type == "TIMESTAMP"}
        return sorted(tuple(as_utc(row.get(name)) if name in timestamps and row.get(name) is not None else row.get(name) for name in columns)
                      for row in rows)

    def test_every_event_table_has_the_same_rows(self):
        for table in silver_stream.BUILDERS:
            with self.subTest(table=table):
                native, python = self.keyed(table, self.native[table]), self.keyed(table, self.python[table])
                self.assertEqual(len(python), len(native), table)
                self.assertEqual(python, native)

    def test_quarantine_has_the_same_positions_and_reasons(self):
        native = sorted((row["eh_offset"], row["reason_code"]) for row in self.native["quarantine"])
        self.assertEqual(sorted(self.rejected), native)
        self.assertTrue(all(row["payload"] is not None or row["channel"] is not None for row in self.native["quarantine"]))


class TestWithoutSpark(unittest.TestCase):
    def test_module_imports_without_pyspark_and_has_the_mapping_for_every_channel(self):
        from channel_maps import CHANNEL_MAPS
        self.assertEqual({"A", "B", "C"}, set(CHANNEL_MAPS))
        for table in CHANNEL_MAPS.values():
            self.assertEqual({"ORDER_CREATED", "ORDER_CANCELLED", "PAYMENT_SUCCEEDED", "PAYMENT_FAILED", "REFUND_SUCCEEDED"}, set(table["events"].values()))
            self.assertEqual({"CANCEL", "RETURN"}, set(table["refund_types"].values()))


if __name__ == "__main__":
    unittest.main()
