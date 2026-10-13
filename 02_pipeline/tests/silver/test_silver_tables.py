"""Silver 테이블 정의(silver_tables.TABLES)를 명세 event_spec.md 1.3절 표와 대조한다. 코드가 아니라 문서가 기준이다."""
import re
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

from silver_tables import TABLES, column_names, create_table_sql  # noqa: E402

SPEC = ROOT / "01_contracts" / "contracts" / "event_spec.md"


def spec_rows() -> dict:
    """1.3절 표에서 {테이블: (PK 목록, 나머지 컬럼 목록)}을 읽는다. quarantine은 '4절 참조'라 제외한다."""
    text = SPEC.read_text(encoding="utf-8")
    section = text.split("### 1.3 Silver 테이블")[1].split("\n- ")[0]
    rows = {}
    for line in section.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) == 4 and cells[0] in TABLES and cells[0] != "quarantine":
            pk = [c.strip() for c in cells[2].split(",")]
            columns = [re.sub(r"\(.*?\)", "", c).strip() for c in cells[3].split(",")]
            rows[cells[0]] = (pk, columns)
    return rows


class TestAgainstSpec(unittest.TestCase):
    def test_spec_table_was_parsed(self):
        self.assertEqual({"orders", "order_items", "order_cancellations", "payments", "refunds"}, set(spec_rows()))

    def test_pk_matches_spec(self):
        for table, (pk, _) in spec_rows().items():
            self.assertEqual(pk, TABLES[table]["pk"], table)

    def test_columns_match_spec(self):
        """명세 컬럼 = channel + PK 컬럼 + 나머지(표의 컬럼 칸). 순서는 보지 않는다."""
        for table, (pk, columns) in spec_rows().items():
            expected = {"channel"} | set(pk) | set(columns)
            self.assertEqual(expected, set(column_names(table)), table)

    def test_quarantine_is_bronze_columns_plus_reason(self):
        self.assertEqual(
            ["channel", "received_at", "payload", "eh_partition", "eh_offset", "eh_enqueued_at", "ingested_at",
             "reason_code", "quarantined_at"], column_names("quarantine"))
        self.assertEqual(["eh_partition", "eh_offset"], TABLES["quarantine"]["pk"])

    def test_pk_columns_exist_in_columns(self):
        for table, definition in TABLES.items():
            self.assertTrue(set(definition["pk"]) <= set(column_names(table)), table)


class TestSql(unittest.TestCase):
    def test_create_table_sql(self):
        sql = create_table_sql("order_cancellations", "commerce_poc.silver")
        self.assertTrue(sql.startswith("CREATE TABLE IF NOT EXISTS commerce_poc.silver.order_cancellations ("))
        self.assertIn("cancel_created_at TIMESTAMP", sql)


if __name__ == "__main__":
    unittest.main()
