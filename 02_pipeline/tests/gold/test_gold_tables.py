"""Gold 테이블 정의 테스트. event_spec.md 6.1·6.2절 표와 TABLES를 대조한다 (pyspark 없이)."""
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "gold"))

import gold_tables  # noqa: E402

SPEC = (ROOT / "01_contracts" / "contracts" / "event_spec.md").read_text(encoding="utf-8")
GOLD_NAMES = ("fact_sales_item", "fact_refund", "dim_date", "dim_channel", "dim_product", "dim_payment_method",
              "dim_refund_profile")


def spec_columns(table: str) -> set:
    """6절 표에서 테이블 행을 찾아 PK와 컬럼 이름을 모은다. channel은 Fact 표에서 컬럼 칸에서 빠져 있다."""
    section = SPEC[SPEC.index("## 6. Gold와 지표"):]
    lines = [line for line in section.splitlines() if line.startswith(f"| {table} |")]
    assert len(lines) == 1, table
    cells = [cell.strip() for cell in lines[0].strip("|").split("|")]
    names = {name.strip() for name in cells[2].split(",")}                  # PK
    for part in re.sub(r"\([^)]*\)", "", cells[3]).split(","):             # 컬럼 칸: 괄호 설명 제거
        if part.strip():
            names.add(part.strip())
    if table.startswith("fact_"):
        names.add("channel")
    return names


class TestTables(unittest.TestCase):
    def test_the_seven_gold_tables(self):
        self.assertEqual(set(GOLD_NAMES), set(gold_tables.TABLES))

    def test_dimensions_are_created_before_facts(self):
        order = list(gold_tables.TABLES)
        self.assertTrue(all(name.startswith("dim_") for name in order[:5]))
        self.assertTrue(all(name.startswith("fact_") for name in order[5:]))

    def test_columns_match_event_spec(self):
        for table in GOLD_NAMES:
            with self.subTest(table=table):
                self.assertEqual(spec_columns(table), set(gold_tables.column_names(table)))

    def test_primary_keys_match_event_spec(self):
        want = {"fact_sales_item": ["channel", "order_item_id"], "fact_refund": ["channel", "refund_id"],
                "dim_date": ["date"], "dim_channel": ["channel"], "dim_product": ["product_id"],
                "dim_payment_method": ["payment_method"], "dim_refund_profile": ["refund_profile_key"]}
        for table, pk in want.items():
            self.assertEqual(pk, gold_tables.TABLES[table]["pk"], table)

    def test_pk_columns_exist_and_names_are_unique(self):
        for table, definition in gold_tables.TABLES.items():
            names = gold_tables.column_names(table)
            self.assertEqual(len(names), len(set(names)), table)
            self.assertTrue(set(definition["pk"]) <= set(names), table)

    def test_create_table_sql(self):
        sql = gold_tables.create_table_sql("dim_channel", "c.gold")
        self.assertEqual("CREATE TABLE IF NOT EXISTS c.gold.dim_channel (channel STRING, channel_name STRING)", sql)
        self.assertEqual("channel STRING, channel_name STRING", gold_tables.schema_ddl("dim_channel"))

    def test_amount_columns_are_bigint_and_dates_are_dates(self):
        types = dict(gold_tables.TABLES["fact_sales_item"]["columns"])
        for name in ("item_gross_amount", "item_discount", "item_paid_amount", "refund_amount"):
            self.assertEqual("BIGINT", types[name])
        for name in ("payment_date", "last_refund_date"):
            self.assertEqual("DATE", types[name])
        self.assertEqual("TIMESTAMP", types["payment_at"])

    def test_create_tables_runs_one_statement_per_table(self):
        calls = []

        class Spark:
            def sql(self, statement):
                calls.append(statement)

        gold_tables.create_tables(Spark(), "c.gold")
        self.assertEqual(7, len(calls))
        self.assertTrue(all(c.startswith("CREATE TABLE IF NOT EXISTS c.gold.") for c in calls))


if __name__ == "__main__":
    unittest.main()
