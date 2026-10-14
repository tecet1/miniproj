"""load_dump 테스트 (pyspark 없이). 합성 Bronze 행의 컬럼·규칙·재생성 시 같은 PK, MERGE 문, 호출 흐름을 확인한다.

received_at의 기대값은 payload를 읽지 않고 canonical이 만든 중립 이벤트의 event_time에서 가져온다 (파싱 결과와 독립).
Databricks에서 같은 입력으로 재실행해도 행이 늘지 않는지는 로컬로 확인할 수 없다 (미검증).
"""
import itertools
import json
import sys
import tempfile
import unittest
import unittest.mock
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "04_experiments" / "src"), str(ROOT / "01_contracts" / "src")]

import build_dump  # noqa: E402
import canonical  # noqa: E402
import generate  # noqa: E402
import load_dump  # noqa: E402
import scenario_loader  # noqa: E402
import synth_scenarios as synth  # noqa: E402
from ids import NamedIds  # noqa: E402

CONTRACTS = ROOT / "01_contracts" / "contracts"
SKUS = scenario_loader.load_skus(CONTRACTS / "skus.json")
FIXTURE_SCENARIOS = scenario_loader.load_scenarios(CONTRACTS / "scenarios.json")
FULL = json.loads(build_dump.DEFAULT_CONFIG.read_text(encoding="utf-8"))
SMALL = dict(FULL, start="2025-10-15", end="2025-10-17")      # 3일치만 만들어 빠르게 확인한다
INGESTED = datetime(2026, 10, 14, 12, 0, 0, tzinfo=UTC)
SYNTHETIC = synth.build_channel_scenarios(SKUS, SMALL)
IDS = NamedIds(SMALL["seed"])
LINES = {c: generate.build_lines(c, SYNTHETIC[c], SKUS, IDS) for c in load_dump.CHANNELS}
ROWS = load_dump.build_rows(LINES, INGESTED)


def expected_times(channel):
    """채널의 줄마다의 이벤트 시각. 줄은 이벤트를 send_count번 반복해 만든다."""
    times = []
    for scenario in SYNTHETIC[channel]:
        for event in canonical.build_events(scenario, SKUS, channel, IDS):
            times += [event["event_time"]] * event["send_count"]
    return times


class TestRows(unittest.TestCase):
    def test_one_row_per_line_with_the_seven_bronze_columns(self):
        self.assertEqual(sum(len(v) for v in LINES.values()), len(ROWS))
        self.assertTrue(all(tuple(row) == load_dump.COLUMNS for row in ROWS))

    def test_channel_and_payload_are_the_line_as_it_is(self):
        position = 0
        for channel in load_dump.CHANNELS:
            for line in LINES[channel]:
                row = ROWS[position]
                self.assertEqual(channel, row["channel"])
                self.assertEqual(json.loads(line)["payload"], row["payload"])
                position += 1

    def test_synthetic_partition_and_global_sequential_offsets_across_channels(self):
        self.assertTrue(all(row["eh_partition"] == -1 for row in ROWS))
        self.assertEqual(list(range(len(ROWS))), [row["eh_offset"] for row in ROWS])
        self.assertEqual(["A", "B", "C"], [k for k, _ in itertools.groupby(row["channel"] for row in ROWS)])

    def test_pk_is_unique_even_for_resent_lines(self):
        resent = [c for c in load_dump.CHANNELS if len(set(LINES[c])) < len(LINES[c])]
        self.assertTrue(resent, "중복 전송 줄이 있어야 이 테스트가 의미 있다")
        self.assertEqual(len(ROWS), len({(r["eh_partition"], r["eh_offset"]) for r in ROWS}))

    def test_received_at_is_the_event_time_in_utc_for_every_channel(self):
        for channel in load_dump.CHANNELS:
            got = [row["received_at"] for row in ROWS if row["channel"] == channel]
            self.assertEqual(expected_times(channel), got, channel)
            self.assertTrue(all(t.tzinfo is not None and t.utcoffset().total_seconds() == 0 for t in got))

    def test_enqueued_at_equals_received_at_and_ingested_at_is_the_given_time(self):
        self.assertTrue(all(row["eh_enqueued_at"] == row["received_at"] for row in ROWS))
        self.assertTrue(all(row["ingested_at"] == INGESTED for row in ROWS))

    def test_rebuilding_from_regenerated_lines_gives_the_same_rows(self):
        again = {c: generate.build_lines(c, synth.build_channel_scenarios(SKUS, SMALL)[c], SKUS, NamedIds(SMALL["seed"]))
                 for c in load_dump.CHANNELS}
        self.assertEqual(ROWS, load_dump.build_rows(again, INGESTED))

    def test_a_line_in_the_wrong_channel_directory_is_rejected(self):
        swapped = dict(LINES, A=LINES["B"][:1])
        with self.assertRaises(ValueError):
            load_dump.build_rows(swapped, INGESTED)

    def test_unknown_channel_time_is_rejected(self):
        with self.assertRaises(ValueError):
            load_dump.event_time("Z", "{}")


class TestFileRoundTrip(unittest.TestCase):
    def test_lines_written_by_build_dump_are_read_back_in_order(self):
        with tempfile.TemporaryDirectory() as out:
            build_dump.write_dump(out, SMALL, SKUS, FIXTURE_SCENARIOS)
            read = load_dump.read_dump_lines(Path(out) / "dump")
        self.assertEqual(LINES, read)


class TestMerge(unittest.TestCase):
    def test_merge_is_insert_only_on_the_bronze_primary_key(self):
        sql = load_dump.merge_sql("c.b.events", "v")
        self.assertIn("MERGE INTO c.b.events", sql)
        self.assertIn("t.eh_partition = s.eh_partition AND t.eh_offset = s.eh_offset", sql)
        self.assertIn("WHEN NOT MATCHED THEN INSERT *", sql)
        self.assertNotIn("WHEN MATCHED", sql)
        self.assertNotIn("UPDATE", sql)
        self.assertNotIn("DELETE", sql)


class _FakeFrame:
    def __init__(self, rows, schema, log):
        self.rows, self.schema, self.log = rows, schema, log

    def createOrReplaceTempView(self, name):
        self.log.append(("view", name, self.rows, self.schema))


class _FakeSpark:
    def __init__(self):
        self.log = []

    def createDataFrame(self, rows, schema):
        return _FakeFrame(rows, schema, self.log)

    def sql(self, statement):
        self.log.append(("sql", statement))


class _Type:
    def __init__(self, *args):
        self.args = args


class _Field:
    def __init__(self, name, data_type):
        self.name, self.data_type = name, data_type


class _Struct:
    def __init__(self, fields):
        self.fields = fields


def fake_pyspark():
    """pyspark가 없는 로컬에서 load_dump의 호출 흐름만 보려고 types 모듈을 흉내 낸다."""
    types = type(sys)("pyspark.sql.types")
    types.IntegerType = types.LongType = types.StringType = types.TimestampType = _Type
    types.StructField, types.StructType = _Field, _Struct
    sql = type(sys)("pyspark.sql")
    sql.types = types
    root = type(sys)("pyspark")
    root.sql = sql
    return {"pyspark": root, "pyspark.sql": sql, "pyspark.sql.types": types}


class TestLoadFlow(unittest.TestCase):
    def test_load_builds_the_view_then_merges_and_returns_the_row_count(self):
        spark = _FakeSpark()
        with tempfile.TemporaryDirectory() as out, unittest.mock.patch.dict(sys.modules, fake_pyspark()):
            build_dump.write_dump(out, SMALL, SKUS, FIXTURE_SCENARIOS)
            count = load_dump.load_dump(spark, "c.b.events", Path(out) / "dump", INGESTED, "src")
        self.assertEqual(len(ROWS), count)
        self.assertEqual(["view", "sql"], [entry[0] for entry in spark.log])
        self.assertEqual(len(ROWS), len(spark.log[0][2]))
        self.assertEqual(list(load_dump.COLUMNS), [f.name for f in spark.log[0][3].fields])
        self.assertEqual(load_dump.merge_sql("c.b.events", "src"), spark.log[1][1])
        self.assertEqual(ROWS[0]["payload"], spark.log[0][2][0][2])


if __name__ == "__main__":
    unittest.main()
