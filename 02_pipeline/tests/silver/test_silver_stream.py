"""silver_stream 구조·배선 테스트 (pyspark 없이, 가짜 Spark 객체 사용).

[확인하는 것] 테이블 컬럼 순서, MERGE 문장, foreachBatch 처리 순서와 대상 테이블·키.
[확인하지 못하는 것] 실제 Spark 변환·UDF·MERGE 결과. 이것은 Databricks에서 silver_check로 확인한다 (미검증).
"""
import inspect
import sys
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

import silver_stream  # noqa: E402
from silver_tables import TABLES, column_names  # noqa: E402


# ── 가짜 Spark 객체 ────────────────────────────────────────────────────────────────
class FakeCol:
    def __init__(self, name):
        self.name = name

    def alias(self, name):
        return FakeCol(name)

    def __eq__(self, other):
        return FakeCol("eq")

    def __hash__(self):
        return id(self)

    def isin(self, *values):
        return FakeCol("isin")

    def otherwise(self, value):
        return FakeCol("otherwise")


class FakeF:
    col = staticmethod(lambda name: FakeCol(name))
    to_timestamp = staticmethod(lambda column: FakeCol("to_timestamp"))
    explode = staticmethod(lambda column: FakeCol("explode"))
    when = staticmethod(lambda condition, value: FakeCol("when"))


class FakeDF:
    """where는 그대로 돌려주고 select는 마지막 컬럼 목록을 기억한다."""

    def __init__(self):
        self.selected = []
        self.views = []
        self.session = FakeSession()
        self.sparkSession = self.session

    def where(self, condition):
        return self

    def select(self, *columns):
        self.selected = [c if isinstance(c, str) else c.name for c in columns]
        return self

    def createOrReplaceTempView(self, name):
        self.views.append(name)


class FakeSession:
    def __init__(self):
        self.statements = []

    def sql(self, statement):
        self.statements.append(statement)


class TestSelectTable(unittest.TestCase):
    def test_columns_follow_tables_order_not_dict_order(self):
        names = column_names("orders")
        columns = {name: FakeCol(name) for name in reversed(names)}      # 일부러 거꾸로 준다
        df = silver_stream.select_table(FakeDF(), "orders", columns)
        self.assertEqual(names, df.selected)

    def test_missing_column_is_reported(self):
        with self.assertRaises(KeyError):
            silver_stream.select_table(FakeDF(), "orders", {"channel": FakeCol("channel")})


class TestBuilders(unittest.TestCase):
    def test_every_builder_outputs_exactly_the_table_columns(self):
        with mock.patch.object(silver_stream, "F", FakeF):
            for table, build in silver_stream.BUILDERS.items():
                with self.subTest(table=table):
                    self.assertEqual(column_names(table), build(FakeDF()).selected)


class TestMergeInsert(unittest.TestCase):
    def test_statement_is_insert_only_with_all_keys(self):
        df = FakeDF()
        silver_stream.merge_insert(df, "commerce_poc.silver.order_items", ["channel", "order_item_id"])
        self.assertEqual(["batch_order_items"], df.views)
        self.assertEqual(
            ["MERGE INTO commerce_poc.silver.order_items AS t USING batch_order_items AS s "
             "ON t.channel = s.channel AND t.order_item_id = s.order_item_id WHEN NOT MATCHED THEN INSERT *"],
            df.session.statements)

    def test_never_updates_or_deletes(self):
        df = FakeDF()
        silver_stream.merge_insert(df, "s.t", ["k"])
        statement = df.session.statements[0]
        self.assertNotIn("WHEN MATCHED", statement)
        self.assertNotIn("DELETE", statement)


class TestBatchHandler(unittest.TestCase):
    def test_quarantine_first_then_every_table_with_its_pk(self):
        calls = []
        with mock.patch.object(silver_stream, "convert_bronze", return_value="converted"), \
             mock.patch.object(silver_stream, "build_quarantine", return_value="quarantine_df"), \
             mock.patch.object(silver_stream, "dedupe_events", return_value="deduped"), \
             mock.patch.object(silver_stream, "parse_events", return_value="events"), \
             mock.patch.dict(silver_stream.BUILDERS, {t: (lambda events, t=t: f"{t}_df") for t in TABLES if t != "quarantine"}), \
             mock.patch.object(silver_stream, "merge_insert", side_effect=lambda df, table, keys: calls.append((df, table, keys))):
            silver_stream.make_batch_handler("cat.silver", {"SKU-001"})("batch", 0)

        expected = [("quarantine_df", "cat.silver.quarantine", TABLES["quarantine"]["pk"])] + [
            (f"{t}_df", f"cat.silver.{t}", TABLES[t]["pk"]) for t in silver_stream.BUILDERS]
        self.assertEqual(expected, calls)

    def test_known_skus_reach_convert_bronze(self):
        skus = {"SKU-001"}
        with mock.patch.object(silver_stream, "convert_bronze", return_value="c") as convert, \
             mock.patch.object(silver_stream, "build_quarantine"), mock.patch.object(silver_stream, "dedupe_events"), \
             mock.patch.object(silver_stream, "parse_events"), mock.patch.dict(silver_stream.BUILDERS, {}, clear=True), \
             mock.patch.object(silver_stream, "merge_insert"):
            silver_stream.make_batch_handler("s", skus)("batch", 0)
        convert.assert_called_once_with("batch", skus)


class TestAfterBatchHook(unittest.TestCase):
    def run_handler(self, after_batch):
        """처리 순서를 기록해 돌려준다. 병합은 "merge", 훅은 after_batch가 남기는 값."""
        order = []
        with mock.patch.object(silver_stream, "convert_bronze", return_value="converted"),              mock.patch.object(silver_stream, "build_quarantine"), mock.patch.object(silver_stream, "dedupe_events"),              mock.patch.object(silver_stream, "parse_events"),              mock.patch.dict(silver_stream.BUILDERS, {"orders": lambda events: "orders_df"}, clear=True),              mock.patch.object(silver_stream, "merge_insert", side_effect=lambda df, table, keys: order.append("merge")):
            silver_stream.make_batch_handler("s", set(), after_batch(order) if after_batch else None)("batch", 7)
        return order

    def test_hook_gets_converted_and_batch_id_after_every_merge(self):
        hook = lambda order: (lambda converted, batch_id: order.append(("hook", converted, batch_id)))   # noqa: E731
        self.assertEqual(["merge", "merge", ("hook", "converted", 7)], self.run_handler(hook))   # 격리 + orders

    def test_no_hook_is_fine(self):
        self.assertEqual(["merge", "merge"], self.run_handler(None))


class TestModuleShape(unittest.TestCase):
    def test_imports_without_pyspark(self):
        self.assertTrue(callable(silver_stream.run_silver))

    def test_run_silver_has_no_time_cutoff(self):
        """읽은 위치는 체크포인트가 기억한다. 시각 인자(since)로 범위를 자르지 않는다 (2026-10-14)."""
        for function in (silver_stream.run_silver, silver_stream.read_bronze):
            self.assertNotIn("since", inspect.signature(function).parameters, function.__name__)

    def test_builders_cover_every_event_table(self):
        self.assertEqual(set(TABLES) - {"quarantine"}, set(silver_stream.BUILDERS))

    def test_bronze_columns_are_quarantine_prefix(self):
        names = [name for name, _ in TABLES["quarantine"]["columns"]]
        self.assertEqual(silver_stream.BRONZE_COLUMNS + ["reason_code", "quarantined_at"], names)

    def test_result_schema_matches_convert_row_keys(self):
        keys = [part.split(" ")[0] for part in silver_stream.RESULT_SCHEMA.split(", ")]
        self.assertEqual(["event_id", "event_type", "event_json", "reason_code"], keys)


if __name__ == "__main__":
    unittest.main()
