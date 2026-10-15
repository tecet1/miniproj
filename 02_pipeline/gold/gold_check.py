"""Gold에서 제외된 업무 키 위반 주문·무효 환불의 건수를 보고한다. 노트북에서 호출만 한다.

Gold의 자체 정합성 검사(환불 합계 일치, 결제금액 대조, 배송비 규칙, PK 중복, Dimension 키 누락)는 규칙 목록(02_pipeline/quality/rules.json)으로 옮겼다 (계획 07 카드 7-C).
노트북은 run_checks.check_layer(spark, schemas, "gold")로 그 규칙을 돌리고, 결과는 commerce_poc.quality.results에 남는다.

report_exclusions는 업무 키 위반 주문 수, 무효 환불 수, 위반 주문에 붙은 환불 수를 돌려준다 (event_spec.md 6.6절: 0이 아니면 보고하며 해당 주문·환불은 Gold에서 제외된 상태다).
겹치는 환불은 위반 주문 쪽에만 센다. 검사 실패가 아니라 보고다. Spark 조회는 Databricks에서만 돈다.
"""
import gold_sql


def report_exclusions(spark, silver_schema: str) -> dict:
    """Gold에서 제외된 업무 키 위반 주문 수, 무효 환불 수, 위반 주문에 붙은 환불 수."""
    return {"업무 키 위반 주문": spark.sql(gold_sql.violation_order_count_sql(silver_schema)).first()["n"],
            "무효 환불": spark.sql(gold_sql.invalid_refund_count_sql(silver_schema)).first()["n"],
            "위반 주문에 붙은 환불": spark.sql(gold_sql.violation_order_refund_count_sql(silver_schema)).first()["n"]}
