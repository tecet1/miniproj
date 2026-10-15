"""Silver 테이블이 독립 기대값(expected.json)과 맞는지 확인한다. 노트북에서 호출만 한다.

[입력] Silver 스키마 이름(예: "commerce_poc.silver"), Bronze 테이블, expected.json 경로
[출력] 문제 문장 목록. 빈 목록이면 모두 일치한다.
[확인 항목]
  1. 테이블별 건수 = expected.json total (orders, order_items, payments, order_cancellations, refunds, quarantine)
  2. Bronze 건수 = 정상 이벤트 테이블 합 + quarantine + 제거된 중복 (기대값 안에서 계산한 중복 수와 같다)
  3. 격리 사유 = 채널마다 MISSING_REQUIRED·INVALID_TYPE·UNKNOWN_EVENT_TYPE 각 1건
  4. PK 중복 0건 (Delta는 PK를 강제하지 않는다)
  5. 참조 누락 0건: refunds -> order_items(channel, order_item_id), refunds -> payments(SUCCEEDED, channel+payment_id)
  6. 결제 성공 금액 = 주문 상품 결제금액 합 + 배송비 (불일치 주문 0건)
  7. 업무 ID 중복 0건: orders·order_cancellations는 (channel, order_id), refunds는 (channel, refund_id)
  8. 격리 행이 Bronze 원본과 같다: 같은 (eh_partition, eh_offset)의 Bronze 행과 channel·payload가 일치 (불일치 0건)
  9. 시나리오 기대값 (plans/03 완료 조건): S23 결제 성공 3건의 결제 시각이 모두 UTC 2026-10-11T15:30:00Z,
     S06 주문 3건(채널당 1건)은 결제 실패 1건 + 성공 1건
운영 기준(픽스처 + 덤프)은 expected_path에 04_experiments/results/expected_dump.json을 넘긴다 (total이 합친 값).
비교 함수(compare_*)는 pyspark 없이 로컬에서 테스트한다. Spark 조회 함수는 Databricks에서만 돈다 (미검증).
"""
import json
from pathlib import Path

from silver_tables import TABLES

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EXPECTED = ROOT / "01_contracts" / "data_fixtures" / "expected" / "expected.json"
EXPECTED_KEY = {"orders": "silver_orders", "order_items": "silver_order_items", "payments": "silver_payments",
                "order_cancellations": "silver_order_cancellations", "refunds": "silver_refunds",
                "quarantine": "quarantine"}
EVENT_TABLES = ("orders", "order_cancellations", "payments", "refunds")   # 이벤트 1건 = 1행인 테이블
EXPECTED_REASONS = {"MISSING_REQUIRED", "INVALID_TYPE", "UNKNOWN_EVENT_TYPE"}


# ── 비교 (pyspark 없음) ────────────────────────────────────────────────────────────
def compare_counts(actual: dict, expected_total: dict) -> list[str]:
    """테이블별 건수를 expected.json의 total과 비교한다. actual = {테이블: 건수}."""
    problems = []
    for table, key in EXPECTED_KEY.items():
        if actual.get(table) != expected_total[key]:
            problems.append(f"{table} 건수: 기대 {expected_total[key]}, 실제 {actual.get(table)}")
    return problems


def compare_bronze_balance(bronze_rows: int, actual: dict, expected_total: dict) -> list[str]:
    """Bronze = 이벤트 테이블 합 + quarantine + 제거된 중복. 제거된 중복 수는 기대값에서 계산한다."""
    expected_dropped = expected_total["bronze_rows"] - sum(expected_total[EXPECTED_KEY[t]] for t in EVENT_TABLES) \
        - expected_total["quarantine"]
    dropped = bronze_rows - sum(actual[t] for t in EVENT_TABLES) - actual["quarantine"]
    if dropped != expected_dropped:
        return [f"제거된 중복: 기대 {expected_dropped}, 실제 {dropped} (Bronze {bronze_rows})"]
    return []


def compare_reasons(actual: dict) -> list[str]:
    """actual = {(channel, reason_code): 건수}. 채널마다 기대 사유가 한 건씩이어야 한다."""
    problems = []
    for channel in ("A", "B", "C"):
        found = {reason: n for (ch, reason), n in actual.items() if ch == channel}
        if found != {reason: 1 for reason in EXPECTED_REASONS}:
            problems.append(f"격리 사유({channel}): 기대 {sorted(EXPECTED_REASONS)} 각 1건, 실제 {found}")
    return problems


def compare_equal(checks: dict) -> list[str]:
    """checks = {이름: (실제, 기대)}. 다른 것만 문제로 돌려준다."""
    return [f"{name}: 기대 {expected}, 실제 {actual}" for name, (actual, expected) in checks.items()
            if actual != expected]


def compare_zero(label: str, counts: dict) -> list[str]:
    """{이름: 건수} 중 0이 아닌 것을 문제로 돌려준다 (PK 중복, 참조 누락, 금액 불일치)."""
    return [f"{label} {name}: {n}건" for name, n in counts.items() if n != 0]


# ── Spark 조회 (Databricks에서만 실행) ───────────────────────────────────────────────
def count_tables(spark, schema: str) -> dict:
    return {table: spark.table(f"{schema}.{table}").count() for table in TABLES}


def count_bronze(spark, table: str) -> int:
    """check_bronze와 같은 조건: channel이 있는 행."""
    import pyspark.sql.functions as F

    return (spark.table(table)
            .where(F.col("channel").isNotNull())
            .count())


def count_reasons(spark, schema: str) -> dict:
    rows = spark.sql(f"SELECT channel, reason_code, COUNT(*) AS n FROM {schema}.quarantine "
                     f"GROUP BY channel, reason_code").collect()
    return {(row["channel"], row["reason_code"]): row["n"] for row in rows}


def count_pk_duplicates(spark, schema: str) -> dict:
    """PK가 겹치는 키 수. 테이블마다 0이어야 한다."""
    result = {}
    for table, definition in TABLES.items():
        keys = ", ".join(definition["pk"])
        result[table] = spark.sql(f"SELECT COUNT(*) AS n FROM (SELECT {keys} FROM {schema}.{table} "
                                  f"GROUP BY {keys} HAVING COUNT(*) > 1)").first()["n"]
    return result


def count_orphans(spark, schema: str) -> dict:
    """환불이 가리키는 상품 행·성공 결제가 없는 건수."""
    def one(sql: str) -> int:
        return spark.sql(sql).first()["n"]

    return {
        "refunds->order_items": one(
            f"SELECT COUNT(*) AS n FROM {schema}.refunds r LEFT ANTI JOIN {schema}.order_items i "
            f"ON r.channel = i.channel AND r.order_item_id = i.order_item_id"),
        "refunds->payments(SUCCEEDED)": one(
            f"SELECT COUNT(*) AS n FROM {schema}.refunds r LEFT ANTI JOIN "
            f"(SELECT * FROM {schema}.payments WHERE status = 'SUCCEEDED') p "
            f"ON r.channel = p.channel AND r.payment_id = p.payment_id"),
    }


def count_business_id_duplicates(spark, schema: str) -> dict:
    """업무 ID가 겹치는 키 수. payment_id는 실패·성공 이벤트가 공유할 수 있어 검사하지 않는다."""
    targets = {"orders": "channel, order_id", "order_cancellations": "channel, order_id", "refunds": "channel, refund_id"}
    return {table: spark.sql(f"SELECT COUNT(*) AS n FROM (SELECT {keys} FROM {schema}.{table} "
                             f"GROUP BY {keys} HAVING COUNT(*) > 1)").first()["n"]
            for table, keys in targets.items()}


def count_quarantine_mismatches(spark, schema: str, bronze_table: str) -> dict:
    """격리 행 중 같은 Bronze 위치의 원본과 channel·payload가 다른 건수 (원본 보존 확인)."""
    sql = (f"SELECT COUNT(*) AS n FROM {schema}.quarantine q LEFT ANTI JOIN {bronze_table} b "
           f"ON q.eh_partition = b.eh_partition AND q.eh_offset = b.eh_offset "
           f"AND q.channel <=> b.channel AND q.payload <=> b.payload")
    return {"격리 행 != Bronze 원본": spark.sql(sql).first()["n"]}


def scenario_checks(spark, schema: str) -> dict:
    """S23·S06 시나리오 기대값. {이름: (실제, 기대)} 형태로 돌려준다."""
    s23 = spark.sql(f"SELECT COUNT(*) AS n FROM {schema}.payments WHERE order_id LIKE '%-S23-%' "
                    f"AND status = 'SUCCEEDED' AND payment_created_at = to_timestamp('2026-10-11T15:30:00Z')").first()["n"]
    s06 = spark.sql(f"SELECT COUNT(*) AS n FROM (SELECT channel, order_id FROM {schema}.payments "
                    f"WHERE order_id LIKE '%-S06-%' GROUP BY channel, order_id "
                    f"HAVING SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) = 1 "
                    f"AND SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) = 1)").first()["n"]
    return {"S23 결제 시각이 UTC 15:30인 성공 결제 수": (s23, 3), "S06 실패1+성공1 주문 수": (s06, 3)}


def count_amount_mismatches(spark, schema: str) -> dict:
    """결제 성공 금액이 (상품 결제금액 합 + 배송비)와 다른 결제 건수."""
    sql = (f"SELECT COUNT(*) AS n FROM {schema}.payments p "
           f"JOIN {schema}.orders o ON p.channel = o.channel AND p.order_id = o.order_id "
           f"JOIN (SELECT channel, order_id, SUM(unit_price * quantity - item_discount) AS items_total "
           f"      FROM {schema}.order_items GROUP BY channel, order_id) i "
           f"ON o.channel = i.channel AND o.order_id = i.order_id "
           f"WHERE p.status = 'SUCCEEDED' AND p.payment_amount <> i.items_total + o.shipping_fee")
    return {"결제금액 != 상품합+배송비": spark.sql(sql).first()["n"]}


def check_silver(spark, schema: str, bronze_table: str, expected_path=DEFAULT_EXPECTED) -> list[str]:
    """Silver 전체를 기대값과 비교해 문제 문장 목록을 돌려준다. 빈 목록이면 일치한다."""
    expected_total = json.loads(Path(expected_path).read_text(encoding="utf-8"))["total"]
    actual = count_tables(spark, schema)
    return (compare_counts(actual, expected_total)
            + compare_bronze_balance(count_bronze(spark, bronze_table), actual, expected_total)
            + compare_reasons(count_reasons(spark, schema))
            + compare_zero("PK 중복", count_pk_duplicates(spark, schema))
            + compare_zero("참조 누락", count_orphans(spark, schema))
            + compare_zero("금액 불일치", count_amount_mismatches(spark, schema))
            + compare_zero("업무 ID 중복", count_business_id_duplicates(spark, schema))
            + compare_zero("원본 보존", count_quarantine_mismatches(spark, schema, bronze_table))
            + compare_equal(scenario_checks(spark, schema)))
