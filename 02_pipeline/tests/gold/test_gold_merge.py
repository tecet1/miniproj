"""MERGE 문장 테스트 (pyspark 없이, 문장 모양과 호출 순서만). 실제 MERGE 실행은 Databricks에서 확인한다."""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "gold"))

import gold_merge  # noqa: E402


class TestMergeSql(unittest.TestCase):
    def test_upsert_without_delete(self):
        sql = gold_merge.merge_sql("c.gold.dim_channel", "src", ["channel"])
        self.assertEqual("MERGE INTO c.gold.dim_channel AS t USING src AS s ON t.channel = s.channel "
                         "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *", sql)

    def test_composite_key_is_anded(self):
        sql = gold_merge.merge_sql("t", "v", ["channel", "order_item_id"])
        self.assertIn("ON t.channel = s.channel AND t.order_item_id = s.order_item_id ", sql)

    def test_delete_missing_adds_not_matched_by_source_last(self):
        sql = gold_merge.merge_sql("t", "v", ["k"], delete_missing=True)
        self.assertTrue(sql.endswith("WHEN NOT MATCHED THEN INSERT * WHEN NOT MATCHED BY SOURCE THEN DELETE"))

    def test_dimension_merge_never_deletes(self):
        self.assertNotIn("BY SOURCE", gold_merge.merge_sql("t", "v", ["k"]))


class TestMergeRows(unittest.TestCase):
    def test_registers_view_then_merges(self):
        log = []

        class Frame:
            def createOrReplaceTempView(self, name):
                log.append(("view", name))

        class Spark:
            def sql(self, statement):
                log.append(("sql", statement))

        gold_merge.merge_rows(Spark(), Frame(), "c.gold.fact_refund", ["channel", "refund_id"], delete_missing=True)
        self.assertEqual(("view", "gold_src_fact_refund"), log[0])
        self.assertEqual("sql", log[1][0])
        self.assertIn("USING gold_src_fact_refund AS s", log[1][1])
        self.assertIn("BY SOURCE", log[1][1])


if __name__ == "__main__":
    unittest.main()
