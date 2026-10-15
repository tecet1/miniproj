"""Gold 테이블의 자체 정합성을 확인한다. 정답(기대값) 비교는 하지 않는다. 노트북에서 호출만 한다.

[입력] Silver·Gold 스키마 이름(예: "commerce_poc.silver", "commerce_poc.gold")
[출력] 문제 문장 목록. 빈 목록이면 모두 통과한다.
[확인 항목] 모두 데이터 자체로 성립해야 하는 규칙이다 (event_spec.md 6.6절).
  1. 대조: fact_refund 합계 = fact_sales_item의 환불 합계 (금액·수량)
  2. 대조: 성공 결제금액 = 상품 결제금액 합 + 배송비, 배송비 규칙 (2만 원 이상 0원, 미만 4,000원)
  3. PK 중복 0건, Dimension 키 누락 0건
건수·금액·지표·분포를 정답과 비교하는 검사는 두지 않는다 (검증은 별도로 진행한다).
report_exclusions는 업무 키 위반 주문 수와 무효 환불 수를 돌려준다 (event_spec.md 6.6절: 0이 아니면 보고하며 해당 주문·환불은 Gold에서 제외된 상태다).
검사 실패가 아니라 보고라서 check_gold의 문제 목록에는 넣지 않는다.
비교 함수는 pyspark 없이 로컬에서 테스트한다. Spark 조회 함수는 Databricks에서만 돈다 (미검증).
"""
import gold_sql
from gold_tables import TABLES

# Fact의 Dimension 참조: (Fact, Fact 컬럼, Dimension, Dimension 키)
REFERENCES = [
    ("fact_sales_item", "channel", "dim_channel", "channel"),
    ("fact_sales_item", "product_id", "dim_product", "product_id"),
    ("fact_sales_item", "payment_method", "dim_payment_method", "payment_method"),
    ("fact_refund", "channel", "dim_channel", "channel"),
    ("fact_refund", "product_id", "dim_product", "product_id"),
    ("fact_refund", "refund_profile_key", "dim_refund_profile", "refund_profile_key"),
]


# ── 비교 (pyspark 없음) ────────────────────────────────────────────────────────────
def compare_equal(checks: dict) -> list[str]:
    """checks = {이름: (실제, 기대)}. 다른 것만 문제로 돌려준다."""
    return [f"{name}: 기대 {expected}, 실제 {actual}" for name, (actual, expected) in checks.items()
            if actual != expected]


def compare_zero(label: str, counts: dict) -> list[str]:
    """{이름: 건수} 중 0이 아닌 것을 문제로 돌려준다 (PK 중복, 키 누락, 대조 불일치)."""
    return [f"{label} {name}: {n}건" for name, n in counts.items() if n != 0]


# ── Spark 조회 (Databricks에서만 실행) ───────────────────────────────────────────────
def sales_item_refund_totals(spark, gold: str) -> dict:
    """fact_sales_item의 환불금액·환불수량 합계 (행이 없으면 0)."""
    row = spark.sql(f"SELECT COALESCE(SUM(refund_amount), 0) AS refund_amount, "
                    f"COALESCE(SUM(refund_quantity), 0) AS refund_quantity FROM {gold}.fact_sales_item").first()
    return {"refund_amount": row["refund_amount"], "refund_quantity": row["refund_quantity"]}


def refund_totals(spark, gold: str) -> dict:
    """fact_refund의 환불금액·환불수량 합계 (행이 없으면 0)."""
    row = spark.sql(f"SELECT COALESCE(SUM(refund_amount), 0) AS refund_amount, "
                    f"COALESCE(SUM(refund_quantity), 0) AS refund_quantity FROM {gold}.fact_refund").first()
    return {"refund_amount": row["refund_amount"], "refund_quantity": row["refund_quantity"]}


def count_pk_duplicates(spark, gold: str) -> dict:
    """PK가 겹치는 키 수. 테이블마다 0이어야 한다."""
    result = {}
    for table, definition in TABLES.items():
        keys = ", ".join(definition["pk"])
        result[table] = spark.sql(f"SELECT COUNT(*) AS n FROM (SELECT {keys} FROM {gold}.{table} "
                                  f"GROUP BY {keys} HAVING COUNT(*) > 1)").first()["n"]
    return result


def count_missing_dimension_keys(spark, gold: str) -> dict:
    """Dimension에 없는 키를 가진 Fact 행 수 (NULL은 제외)."""
    result = {}
    for fact, column, dim, key in REFERENCES:
        result[f"{fact}.{column}->{dim}"] = spark.sql(
            f"SELECT COUNT(*) AS n FROM {gold}.{fact} f LEFT ANTI JOIN {gold}.{dim} d ON f.{column} = d.{key} "
            f"WHERE f.{column} IS NOT NULL").first()["n"]
    return result


def count_payment_mismatches(spark, silver: str, gold: str) -> dict:
    """Fact에 있는 주문 중 성공 결제금액 != 상품 결제금액 합 + 배송비인 주문 수, 배송비 규칙을 어긴 주문 수."""
    base = (f"SELECT COUNT(*) AS n FROM (SELECT channel, order_id, SUM(item_paid_amount) AS items_total "
            f"FROM {gold}.fact_sales_item GROUP BY channel, order_id) g "
            f"JOIN {silver}.orders o ON o.channel = g.channel AND o.order_id = g.order_id "
            f"JOIN {silver}.payments p ON p.channel = g.channel AND p.order_id = g.order_id AND p.status = 'SUCCEEDED' ")
    return {
        "결제금액 != 상품합+배송비": spark.sql(base + "WHERE p.payment_amount <> g.items_total + o.shipping_fee").first()["n"],
        "배송비 규칙 위반": spark.sql(
            base + "WHERE o.shipping_fee <> CASE WHEN g.items_total >= 20000 THEN 0 ELSE 4000 END").first()["n"],
    }


def report_exclusions(spark, silver_schema: str) -> dict:
    """Gold에서 제외된 업무 키 위반 주문 수와 무효 환불 수."""
    return {"업무 키 위반 주문": spark.sql(gold_sql.violation_order_count_sql(silver_schema)).first()["n"],
            "무효 환불": spark.sql(gold_sql.invalid_refund_count_sql(silver_schema)).first()["n"]}


def check_gold(spark, silver_schema: str, gold_schema: str) -> list[str]:
    """Gold 전체를 자체 정합성 규칙으로 확인해 문제 문장 목록을 돌려준다. 빈 목록이면 통과한다."""
    sales, refund = sales_item_refund_totals(spark, gold_schema), refund_totals(spark, gold_schema)
    reconcile = {"환불금액 fact_refund vs fact_sales_item": (refund["refund_amount"], sales["refund_amount"]),
                 "환불수량 fact_refund vs fact_sales_item": (refund["refund_quantity"], sales["refund_quantity"])}
    return (compare_equal(reconcile)
            + compare_zero("PK 중복", count_pk_duplicates(spark, gold_schema))
            + compare_zero("Dimension 키 누락", count_missing_dimension_keys(spark, gold_schema))
            + compare_zero("대조 불일치", count_payment_mismatches(spark, silver_schema, gold_schema)))
