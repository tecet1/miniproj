"""build_gold의 호출 순서 테스트 (가짜 Spark 객체, pyspark 없이). 실제 실행·결과는 Databricks에서 확인한다 (미검증)."""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "gold"))

import gold_build  # noqa: E402
import gold_tables  # noqa: E402


class FakeFrame:
    def __init__(self, log, label):
        self.log, self.label = log, label

    def createOrReplaceTempView(self, name):
        self.log.append(("view", name, self.label))


class FakeSpark:
    """호출을 순서대로 기록한다."""

    def __init__(self):
        self.log, self.dataframes = [], []

    def sql(self, statement):
        self.log.append(("sql", statement))
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
    def run_build(self):
        spark = FakeSpark()
        gold_build.build_gold(spark, "c.silver", "c.gold")
        return spark

    def test_creates_the_six_tables_first(self):
        spark = self.run_build()
        first = [s for kind, s, *_ in spark.log[:6]]
        self.assertEqual([f"CREATE TABLE IF NOT EXISTS c.gold.{t} ({gold_tables.schema_ddl(t)})"
                          for t in gold_tables.TABLES], first)

    def test_merge_order_is_dimensions_then_facts(self):
        targets = [t for t, _ in merges(self.run_build())]
        self.assertEqual(["c.gold.dim_channel", "c.gold.dim_payment_method", "c.gold.dim_refund_profile",
                          "c.gold.dim_product", "c.gold.fact_sales_item", "c.gold.fact_refund"],
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
                                                              "dim_refund_profile", "dim_product")],
                         [schema for _, schema in spark.dataframes])

    def test_no_date_dimension_is_built(self):
        """일자 차원은 두지 않는다 (event_spec.md 6.2절): 테이블 생성·MERGE·일자 범위 조회가 없다."""
        spark = self.run_build()
        self.assertEqual(4, len(spark.dataframes))
        self.assertFalse([e for e in spark.log if e[0] == "sql" and "dim_date" in e[1]])


if __name__ == "__main__":
    unittest.main()
