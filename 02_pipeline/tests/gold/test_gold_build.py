"""build_gold의 호출 순서 테스트 (가짜 Spark 객체, pyspark 없이). 실제 실행·결과는 Databricks에서 확인한다 (미검증)."""
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "gold"))

import gold_build  # noqa: E402
import gold_tables  # noqa: E402


class FakeFrame:
    def __init__(self, log, label, row=None, rows=None):
        self.log, self.label, self.row, self.rows = log, label, row, rows

    def createOrReplaceTempView(self, name):
        self.log.append(("view", name, self.label))

    def first(self):
        return self.row


class FakeSpark:
    """호출을 순서대로 기록한다. 일자 범위 조회에는 bounds를 돌려준다."""

    def __init__(self, bounds):
        self.log, self.bounds, self.dataframes = [], bounds, []

    def sql(self, statement):
        self.log.append(("sql", statement))
        if "MIN(d)" in statement:
            return FakeFrame(self.log, "bounds", row=self.bounds)
        return FakeFrame(self.log, statement)

    def createDataFrame(self, rows, schema):
        self.dataframes.append((rows, schema))
        return FakeFrame(self.log, f"df:{schema}")


def merges(spark):
    """MERGE 문장만 (대상 테이블, 문장) 순서대로."""
    result = []
    for kind, *rest in spark.log:
        if kind == "sql" and rest[0].startswith("MERGE INTO"):
            result.append((rest[0].split()[2], rest[0]))
    return result


class TestBuildGold(unittest.TestCase):
    def run_build(self, bounds=None):
        spark = FakeSpark(bounds or {"first_date": date(2026, 10, 12), "last_date": date(2026, 10, 14)})
        gold_build.build_gold(spark, "c.silver", "c.gold")
        return spark

    def test_creates_the_seven_tables_first(self):
        spark = self.run_build()
        first = [s for kind, s, *_ in spark.log[:7]]
        self.assertEqual([f"CREATE TABLE IF NOT EXISTS c.gold.{t} ({gold_tables.schema_ddl(t)})"
                          for t in gold_tables.TABLES], first)

    def test_merge_order_is_dimensions_then_facts_then_dim_date(self):
        targets = [t for t, _ in merges(self.run_build())]
        self.assertEqual(["c.gold.dim_channel", "c.gold.dim_payment_method", "c.gold.dim_refund_profile",
                          "c.gold.dim_product", "c.gold.fact_sales_item", "c.gold.fact_refund", "c.gold.dim_date"],
                         targets)

    def test_only_facts_delete_rows_missing_from_the_new_result(self):
        for target, sql in merges(self.run_build()):
            self.assertEqual(target.startswith("c.gold.fact_"), "BY SOURCE" in sql, target)

    def test_merge_keys_follow_table_pks(self):
        for target, sql in merges(self.run_build()):
            pk = gold_tables.TABLES[target.split(".")[-1]]["pk"]
            self.assertIn(" AND ".join(f"t.{k} = s.{k}" for k in pk), sql)

    def test_facts_are_built_from_silver_sql_and_merged_through_views(self):
        spark = self.run_build()
        views = {name: label for kind, name, label in (e for e in spark.log if e[0] == "view")}
        self.assertIn("FROM c.silver.order_items", views["gold_src_fact_sales_item"])
        self.assertIn("FROM c.silver.refunds", views["gold_src_fact_refund"])

    def test_dimension_frames_use_table_schemas(self):
        spark = self.run_build()
        self.assertEqual([gold_tables.schema_ddl(t) for t in ("dim_channel", "dim_payment_method",
                                                              "dim_refund_profile", "dim_product", "dim_date")],
                         [schema for _, schema in spark.dataframes])

    def test_dim_date_covers_the_fact_date_range(self):
        spark = self.run_build()
        rows = spark.dataframes[-1][0]
        self.assertEqual([date(2026, 10, 12), date(2026, 10, 13), date(2026, 10, 14)], [r[0] for r in rows])

    def test_dim_date_is_skipped_when_facts_are_empty(self):
        spark = self.run_build({"first_date": None, "last_date": None})
        self.assertNotIn("c.gold.dim_date", [t for t, _ in merges(spark)])
        self.assertEqual(4, len(spark.dataframes))

    def test_date_range_is_read_after_both_fact_merges(self):
        spark = self.run_build()
        statements = [e[1] for e in spark.log if e[0] == "sql"]
        bounds = next(i for i, s in enumerate(statements) if "MIN(d)" in s)
        refund_merge = next(i for i, s in enumerate(statements) if s.startswith("MERGE INTO c.gold.fact_refund"))
        self.assertLess(refund_merge, bounds)


if __name__ == "__main__":
    unittest.main()
