"""Silver 테이블의 자체 정합성을 확인한다. 정답(기대값) 비교는 하지 않는다. 노트북에서 호출만 한다.

[입력] Silver 스키마 이름(예: "commerce_poc.silver"), Bronze 테이블
[출력] 문제 문장 목록. 빈 목록이면 모두 통과한다.
[확인 항목] 모두 데이터 자체로 성립해야 하는 규칙이다.
  1. PK 중복 0건 (Delta는 PK를 강제하지 않는다)
  2. 참조 누락 0건: refunds -> order_items(channel, order_item_id), refunds -> payments(SUCCEEDED, channel+payment_id)
  3. 결제 성공 금액 = 주문 상품 결제금액 합 + 배송비 (불일치 결제 0건)
  4. 업무 ID 중복 0건: orders·order_cancellations는 (channel, order_id), refunds는 (channel, refund_id)
  5. 격리 행이 Bronze 원본과 같다: 같은 (eh_partition, eh_offset)의 Bronze 행과 channel·payload가 일치 (불일치 0건)
배치별 균형("들어온 수 = 반영 + 중복 + 격리")과 규칙 증분 실행은 02_pipeline/quality(batch_check, run_checks)가 맡는다.
비교 함수(compare_zero)는 pyspark 없이 로컬에서 테스트한다. Spark 조회 함수는 Databricks에서만 돈다 (미검증).
"""
from silver_tables import TABLES


# ── 비교 (pyspark 없음) ────────────────────────────────────────────────────────────
def compare_zero(label: str, counts: dict) -> list[str]:
    """{이름: 건수} 중 0이 아닌 것을 문제로 돌려준다 (PK 중복, 참조 누락, 금액 불일치)."""
    return [f"{label} {name}: {n}건" for name, n in counts.items() if n != 0]


# ── Spark 조회 (Databricks에서만 실행) ───────────────────────────────────────────────
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


def count_amount_mismatches(spark, schema: str) -> dict:
    """결제 성공 금액이 (상품 결제금액 합 + 배송비)와 다른 결제 건수."""
    sql = (f"SELECT COUNT(*) AS n FROM {schema}.payments p "
           f"JOIN {schema}.orders o ON p.channel = o.channel AND p.order_id = o.order_id "
           f"JOIN (SELECT channel, order_id, SUM(unit_price * quantity - item_discount) AS items_total "
           f"      FROM {schema}.order_items GROUP BY channel, order_id) i "
           f"ON o.channel = i.channel AND o.order_id = i.order_id "
           f"WHERE p.status = 'SUCCEEDED' AND p.payment_amount <> i.items_total + o.shipping_fee")
    return {"결제금액 != 상품합+배송비": spark.sql(sql).first()["n"]}


def check_silver(spark, schema: str, bronze_table: str) -> list[str]:
    """Silver 전체를 자체 정합성 규칙으로 확인해 문제 문장 목록을 돌려준다. 빈 목록이면 통과한다."""
    return (compare_zero("PK 중복", count_pk_duplicates(spark, schema))
            + compare_zero("참조 누락", count_orphans(spark, schema))
            + compare_zero("금액 불일치", count_amount_mismatches(spark, schema))
            + compare_zero("업무 ID 중복", count_business_id_duplicates(spark, schema))
            + compare_zero("원본 보존", count_quarantine_mismatches(spark, schema, bronze_table)))
