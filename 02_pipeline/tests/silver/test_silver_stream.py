"""silver_stream 모듈의 구조 확인 (pyspark 없이). Spark 동작은 Databricks에서 silver_check로 확인한다 (미검증)."""
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

import silver_stream  # noqa: E402
from silver_tables import TABLES  # noqa: E402


class TestModuleShape(unittest.TestCase):
    def test_imports_without_pyspark(self):
        self.assertTrue(callable(silver_stream.run_silver))

    def test_builders_cover_every_event_table(self):
        """quarantine을 뺀 모든 Silver 테이블에 만드는 함수가 하나씩 있어야 한다."""
        self.assertEqual(set(TABLES) - {"quarantine"}, set(silver_stream.BUILDERS))

    def test_bronze_columns_are_quarantine_prefix(self):
        """quarantine은 Bronze 7컬럼 + reason_code + quarantined_at 이다 (event_spec.md 4절)."""
        names = [name for name, _ in TABLES["quarantine"]["columns"]]
        self.assertEqual(silver_stream.BRONZE_COLUMNS + ["reason_code", "quarantined_at"], names)

    def test_result_schema_matches_convert_row_keys(self):
        keys = [part.split(" ")[0] for part in silver_stream.RESULT_SCHEMA.split(", ")]
        self.assertEqual(["event_id", "event_type", "event_json", "reason_code"], keys)


if __name__ == "__main__":
    unittest.main()
