"""Gold 증분 재계산(gold_incremental, 계획 07 카드 7-E)을 로컬(sqlite)에서 확인한다.

[핵심 성질] Silver 이벤트가 어떤 순서·묶음으로 도착해도, 매 묶음마다 "바뀐 주문만 다시 계산해 범위 안의 Fact 행을 바꾸는" 증분 반영이
  그 시점의 Silver 전체를 다시 계산한 Gold와 같다 (그리고 같은 묶음을 한 번 더 처리해도 변하지 않는다).
[방법] 시나리오 + 오류를 높은 비율로 주입한 합성 데이터를 파이썬 파이프라인으로 Silver 행으로 만든 뒤, 행 단위로 섞어 여러 묶음으로 나눠 도착시킨다.
  MERGE는 sqlite에 없으므로 MERGE 소스(merge_source_sql)를 실행해 같은 의미(삭제 표시는 지우고 나머지는 갱신·추가)로 파이썬이 적용한다.
[확인하지 못하는 것] Delta MERGE 문장 실행, 변경 데이터 피드 읽기, foreachBatch (Databricks 전용, 미검증). MERGE 문장은 구조만 본다.
"""
import json
import random
import sqlite3
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "02_pipeline" / "gold"), str(ROOT / "02_pipeline" / "tests"), str(ROOT / "02_pipeline" / "tests" / "gold"), str(ROOT / "04_experiments" / "src"),
                str(ROOT / "01_contracts" / "src")]

import _silver_rows as silver  # noqa: E402
import generate  # noqa: E402
import gold_incremental as G  # noqa: E402
import gold_sql  # noqa: E402
import gold_tables  # noqa: E402
import scenario_loader  # noqa: E402
import synth_scenarios as synth  # noqa: E402
from ids import NamedIds  # noqa: E402
from synthetic_inputs import crafted_lines, scenario_lines  # noqa: E402

CONFIG = json.loads((ROOT / "04_experiments" / "configs" / "dump.json").read_text(encoding="utf-8"))
HIGH_RATES = {"fault": 0.03, "double_payment": 0.04, "duplicate_order_created": 0.04, "refund_to_failed_payment": 0.2,
              "refund_order_mismatch": 0.2, "duplicate_item_id": 0.04}
SILVER = "silver"
GOLD = "gold"
DATE = silver.sqlite_date
FACTS = ("fact_sales_item", "fact_refund")


def build_silver_rows():
    """파이썬 파이프라인(convert_row)으로 만든 Silver 행 (시나리오 + 오류를 많이 주입한 합성 하루치)."""
    skus = scenario_loader.load_skus(ROOT / "01_contracts" / "contracts" / "skus.json")
    lines = []
    for channel in "ABC":
        lines += [json.loads(l) for l in scenario_lines(channel)]
    sample = synth.build_scenarios(skus, dict(CONFIG, start="2025-11-03", end="2025-11-03", error_rates=HIGH_RATES, base_orders_per_day=40))
    for channel in "ABC":
        lines += [json.loads(l) for l in generate.build_lines(channel, sample, skus, NamedIds(channel))]
    kept, seen = [], set()
    for line in lines:
        result = silver.convert_row(line["channel"], line["payload"], silver.KNOWN)
        if not result["reason_code"] and (line["channel"], result["event_id"]) not in seen:
            seen.add((line["channel"], result["event_id"]))
            kept.append(json.loads(result["event_json"]))
    return silver.to_silver(kept)


def gold_connection():
    """Silver 빈 테이블 5개 + Gold Fact 빈 테이블 2개를 가진 sqlite."""
    con = silver.connect(silver.rows_of())
    con.execute("ATTACH DATABASE ':memory:' AS gold")
    for table in FACTS:
        con.execute(f"CREATE TABLE gold.{table} ({', '.join(gold_tables.column_names(table))})")
    return con


def insert(con, table, rows):
    columns = silver.COLUMNS[table]
    con.executemany(f"INSERT INTO silver.{table} VALUES ({', '.join('?' for _ in columns)})", [[row.get(c) for c in columns] for row in rows])


def changes_of(table, rows):
    """Silver 테이블에 새로 쌓인 행을 변경 행 (channel, order_id, order_item_id, payment_id, refund_id)으로 (Spark 스트림 read_changes_stream과 같은 규칙)."""
    out = []
    for r in rows:
        out.append((r["channel"], r["order_id"], r.get("order_item_id") if table in ("order_items", "refunds") else None,
                    r.get("payment_id") if table in ("payments", "refunds") else None, r.get("refund_id") if table == "refunds" else None))
    return out


def set_temp_table(con, name, sql):
    con.execute(f"DROP TABLE IF EXISTS temp.{name}")
    con.execute(f"CREATE TEMP TABLE {name} AS {sql}")


def apply_batch(con, batch):
    """batch = {테이블: 행 목록}. Silver에 쌓고 변경 행으로 범위를 정해 Gold Fact를 증분 반영한다 (MERGE 의미를 파이썬으로 적용)."""
    changes = []
    for table, rows in batch.items():
        insert(con, table, rows)
        changes += changes_of(table, rows)
    con.execute("DROP TABLE IF EXISTS temp.gold_changes")
    con.execute("CREATE TEMP TABLE gold_changes (channel, order_id, order_item_id, payment_id, refund_id)")
    con.executemany("INSERT INTO gold_changes VALUES (?, ?, ?, ?, ?)", sorted(set(changes), key=str))
    set_temp_table(con, G.AFFECTED_ORDERS, G.affected_orders_sql(SILVER))
    set_temp_table(con, G.AFFECTED_REFUNDS, G.affected_refunds_sql(SILVER))
    for table in FACTS:
        keys = gold_tables.TABLES[table]["pk"]
        columns = gold_tables.column_names(table)
        for row in con.execute(G.merge_source_sql(table, SILVER, GOLD, DATE)).fetchall():
            row = dict(row)
            where = " AND ".join(f"{k} = ?" for k in keys)
            con.execute(f"DELETE FROM gold.{table} WHERE {where}", [row[k] for k in keys])
            if not row["_delete"]:
                con.execute(f"INSERT INTO gold.{table} VALUES ({', '.join('?' for _ in columns)})", [row[c] for c in columns])


def full_rows(con, table):
    sql = gold_sql.sales_item_sql(SILVER, DATE) if table == "fact_sales_item" else gold_sql.refund_sql(SILVER, DATE)
    return sorted(tuple(r.values()) for r in silver.run(con, sql))


def gold_rows(con, table):
    return sorted(tuple(r.values()) for r in silver.run(con, f"SELECT {', '.join(gold_tables.column_names(table))} FROM gold.{table}"))


def row_events(rows):
    """Silver 행을 (테이블, 행) 하나씩으로 편다 (도착 단위)."""
    return [(table, row) for table, part in rows.items() for row in part]


def batches_of(events, seed, count):
    rng = random.Random(seed)
    events = list(events)
    rng.shuffle(events)
    cuts = sorted(rng.sample(range(1, len(events)), count - 1))
    pieces = [events[a:b] for a, b in zip([0, *cuts], [*cuts, len(events)])]
    return [{table: [row for t, row in piece if t == table] for table in silver.COLUMNS} for piece in pieces]


class TestIncrementalEqualsFull(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows = build_silver_rows()
        cls.events = row_events(cls.rows)

    def test_the_data_has_the_cases_that_make_the_test_meaningful(self):
        full = gold_connection()
        for table, part in self.rows.items():
            insert(full, table, part)
        self.assertGreater(len(full_rows(full, "fact_sales_item")), 150)
        self.assertGreater(len(full_rows(full, "fact_refund")), 2)
        self.assertGreater(len(self.rows["refunds"]), len(full_rows(full, "fact_refund")))      # 제외된 환불이 있다
        self.assertGreater(len(self.rows["orders"]), len({(r["channel"], r["order_id"]) for r in self.rows["orders"]}))   # 주문 생성 중복이 있다

    def test_gold_equals_the_full_recompute_after_every_batch_for_any_arrival_order(self):
        for seed in (1, 2, 3, 4):
            con = gold_connection()
            for number, batch in enumerate(batches_of(self.events, seed, 10), 1):
                apply_batch(con, batch)
                for table in FACTS:
                    self.assertEqual(full_rows(con, table), gold_rows(con, table), f"seed={seed} batch={number} {table}")
            self.assertGreater(len(gold_rows(con, "fact_sales_item")), 150)

    def test_arriving_one_event_at_a_time_gives_the_same_result(self):
        con = gold_connection()
        events = list(self.events)
        random.Random(7).shuffle(events)
        small = [e for e in events[:120]]
        for table, row in small:
            apply_batch(con, {table: [row]})
        for table in FACTS:
            self.assertEqual(full_rows(con, table), gold_rows(con, table), table)

    def test_processing_the_same_changes_again_changes_nothing(self):
        con = gold_connection()
        batches = batches_of(self.events, 5, 6)
        for batch in batches:
            apply_batch(con, batch)
        before = {table: gold_rows(con, table) for table in FACTS}
        for batch in batches:
            con.executemany  # 같은 변경을 다시 반영한다: Silver는 이미 있으므로 변경 행만 다시 넣는다
            changes = []
            for table, rows in batch.items():
                changes += changes_of(table, rows)
            con.execute("DELETE FROM temp.gold_changes")
            con.executemany("INSERT INTO gold_changes VALUES (?, ?, ?, ?, ?)", sorted(set(changes), key=str))
            set_temp_table(con, G.AFFECTED_ORDERS, G.affected_orders_sql(SILVER))
            set_temp_table(con, G.AFFECTED_REFUNDS, G.affected_refunds_sql(SILVER))
            for table in FACTS:
                for row in con.execute(G.merge_source_sql(table, SILVER, GOLD, DATE)).fetchall():
                    row = dict(row)
                    keys = gold_tables.TABLES[table]["pk"]
                    con.execute(f"DELETE FROM gold.{table} WHERE " + " AND ".join(f"{k} = ?" for k in keys), [row[k] for k in keys])
                    if not row["_delete"]:
                        columns = gold_tables.column_names(table)
                        con.execute(f"INSERT INTO gold.{table} VALUES ({', '.join('?' for _ in columns)})", [row[c] for c in columns])
        self.assertEqual(before, {table: gold_rows(con, table) for table in FACTS})

    def test_consistency_check_is_zero_after_applying_and_positive_when_gold_is_wrong(self):
        con = gold_connection()
        for batch in batches_of(self.events, 9, 5):
            apply_batch(con, batch)
        con.execute("DROP TABLE IF EXISTS temp.gold_changes")
        con.execute("CREATE TEMP TABLE gold_changes (channel, order_id, order_item_id, payment_id, refund_id)")
        con.executemany("INSERT INTO gold_changes VALUES (?, ?, ?, ?, ?)", sorted({c for t, r in self.events for c in changes_of(t, [r])}, key=str))
        set_temp_table(con, G.AFFECTED_ORDERS, G.affected_orders_sql(SILVER))
        set_temp_table(con, G.AFFECTED_REFUNDS, G.affected_refunds_sql(SILVER))
        for table in FACTS:
            self.assertEqual(0, silver.run(con, G.consistency_sql(table, SILVER, GOLD, DATE))[0]["violations"], table)
        con.execute("UPDATE gold.fact_sales_item SET item_paid_amount = item_paid_amount + 1 WHERE rowid = (SELECT MIN(rowid) FROM gold.fact_sales_item)")
        con.execute("DELETE FROM gold.fact_refund WHERE rowid = (SELECT MIN(rowid) FROM gold.fact_refund)")
        self.assertEqual(2, silver.run(con, G.consistency_sql("fact_sales_item", SILVER, GOLD, DATE))[0]["violations"])
        self.assertEqual(1, silver.run(con, G.consistency_sql("fact_refund", SILVER, GOLD, DATE))[0]["violations"])


class TestTargetedCases(unittest.TestCase):
    def setUp(self):
        self.con = gold_connection()

    def order(self, order_id="A-X1", **parts):
        base = dict(orders=[silver.order(order_id)], order_items=[silver.item(order_id)], payments=[silver.payment(order_id)])
        base.update(parts)
        return silver.rows_of(**base)

    def arrive(self, rows):
        for table, part in rows.items():
            for row in part:
                apply_batch(self.con, {table: [row]})

    def sales_orders(self):
        return {r["order_id"] for r in silver.run(self.con, "SELECT order_id FROM gold.fact_sales_item")}

    def test_a_cancellation_arriving_later_removes_the_order_rows(self):
        self.arrive(self.order())
        self.assertEqual({"A-X1"}, self.sales_orders())
        self.arrive(silver.rows_of(order_cancellations=[silver.cancel("A-X1")]))
        self.assertEqual(set(), self.sales_orders())

    def test_a_second_payment_arriving_later_turns_the_order_into_a_violation(self):
        self.arrive(self.order())
        self.arrive(silver.rows_of(payments=[silver.payment("A-X1", no=2)]))
        self.assertEqual(set(), self.sales_orders())

    def test_a_refund_before_its_payment_waits_and_then_appears(self):
        rows = self.order(refunds=[silver.refund("A-X1")], payments=[])
        self.arrive(rows)
        self.assertEqual([], gold_rows(self.con, "fact_refund"))
        self.arrive(silver.rows_of(payments=[silver.payment("A-X1")]))
        self.assertEqual(1, len(gold_rows(self.con, "fact_refund")))
        self.assertEqual(1000, silver.run(self.con, "SELECT refund_amount FROM gold.fact_sales_item")[0]["refund_amount"])

    def test_a_refund_to_another_order_changes_when_the_other_order_becomes_a_violation(self):
        """환불의 order_id가 다른 주문이면 무효다. 그 주문이 위반 주문이 되면 환불은 위반 주문 쪽으로 빠져 둘 다 Fact에 없다. 어느 쪽이든 전체 재계산과 같다."""
        rows = self.order("A-X1", refunds=[silver.refund("A-X1")])
        other = self.order("A-X2")
        self.arrive(rows)
        self.arrive(other)
        self.arrive(silver.rows_of(refunds=[{**silver.refund("A-X1", no=2), "refund_id": "A-X1-R2", "order_id": "A-X2"}]))
        self.arrive(silver.rows_of(payments=[silver.payment("A-X2", no=2)]))
        for table in FACTS:
            self.assertEqual(full_rows(self.con, table), gold_rows(self.con, table), table)

    def test_only_the_changed_orders_are_recomputed(self):
        self.arrive(self.order("A-X1"))
        self.arrive(self.order("A-X2"))
        before = silver.run(self.con, "SELECT rowid, order_id FROM gold.fact_sales_item ORDER BY order_id")
        apply_batch(self.con, {"order_cancellations": [silver.cancel("A-X2")]})
        self.assertEqual({"A-X1"}, self.sales_orders())
        affected = {r["order_id"] for r in silver.run(self.con, f"SELECT order_id FROM {G.AFFECTED_ORDERS}")}
        self.assertEqual({"A-X2"}, affected)                      # 취소가 온 주문만 범위에 든다
        self.assertEqual([before[0]["order_id"]], [r["order_id"] for r in silver.run(self.con, "SELECT order_id FROM gold.fact_sales_item")])


class TestSqlText(unittest.TestCase):
    def test_merge_has_delete_update_and_insert_branches_and_uses_every_column(self):
        for table in FACTS:
            sql = G.scoped_merge_sql(table, "s", "g", f"g.{table}")
            self.assertIn("WHEN MATCHED AND s._delete THEN DELETE", sql)
            self.assertIn("WHEN MATCHED THEN UPDATE SET", sql)
            self.assertIn("WHEN NOT MATCHED AND NOT s._delete THEN INSERT", sql)
            for column in gold_tables.column_names(table):
                self.assertIn(f"s.{column}", sql)
            for key in gold_tables.TABLES[table]["pk"]:
                self.assertIn(f"t.{key} = s.{key}", sql)

    def test_restricted_sql_is_the_full_sql_plus_a_scope_join(self):
        self.assertNotIn(G.AFFECTED_ORDERS, gold_sql.sales_item_sql("s"))
        self.assertIn(G.AFFECTED_ORDERS, gold_sql.sales_item_sql("s", orders=G.AFFECTED_ORDERS))
        self.assertNotIn(G.AFFECTED_REFUNDS, gold_sql.refund_sql("s"))
        self.assertIn(G.AFFECTED_REFUNDS, gold_sql.refund_sql("s", refunds=G.AFFECTED_REFUNDS))

    def test_scope_covers_the_three_orders_a_refund_can_depend_on(self):
        sql = G.affected_orders_sql("s")
        self.assertIn("s.order_items", sql)
        self.assertIn("s.payments", sql)
        refunds = G.affected_refunds_sql("s")
        for table in ("s.refunds", "s.order_items", "s.payments"):
            self.assertIn(table, refunds)

    def test_spark_only_functions_are_there_and_the_module_imports_without_pyspark(self):
        for name in ("enable_change_feed", "bootstrap", "read_changes_stream", "apply_changes", "run_gold_stream", "latest_versions"):
            self.assertTrue(callable(getattr(G, name)), name)


if __name__ == "__main__":
    unittest.main()
