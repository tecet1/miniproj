"""Bronze 테이블이 이번 송신분과 일치하는지 확인한다. 노트북에서 호출만 한다.

기대값은 fixtures의 (channel, payload) 건수이고, 도착 확인(verify_arrival)과 같은 함수로 센다.
비교 대상은 channel이 있는 행이다. (봉투가 없는 이전 테스트 메시지는 제외) 시각으로 자르지 않으므로 Bronze는 이번 데이터만 담고 있어야 한다.
pyspark는 함수 안에서만 import한다 (로컬·CI 테스트용).
"""
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_FIXTURES = ROOT / "01_contracts" / "data_fixtures"
sys.path.insert(0, str(ROOT / "02_pipeline" / "ingestion"))

from verify_arrival import compare, load_expected  # noqa: E402


def load_bronze_pairs(spark, table: str) -> Counter:
    """Bronze에서 channel이 있는 행의 {(channel, payload): 건수}를 센다."""
    import pyspark.sql.functions as F

    rows = (spark.table(table)
            .where(F.col("channel").isNotNull())
            .select("channel", "payload")
            .collect())
    return Counter((row["channel"], row["payload"]) for row in rows)


def check_bronze(spark, table: str, fixtures_dir=DEFAULT_FIXTURES) -> list[str]:
    """fixtures와 Bronze의 차이를 문장 목록으로 돌려준다. 빈 목록이면 일치한다."""
    return compare(load_expected(fixtures_dir), load_bronze_pairs(spark, table))
