"""Metric View의 자체 정합성을 확인한다. 정답(기대값) 비교는 하지 않는다. 노트북에서 호출만 한다.

[입력] Serving 스키마 이름(예: "commerce_poc.serving")
[출력] 문제 문장 목록. 빈 목록이면 통과한다.
[확인 항목]
  1. 두 Metric View의 환불금액이 같다 (sales_metrics 합계 = refund_metrics 합계)
지표 값을 정답과 비교하는 검사는 두지 않는다 (검증은 별도로 진행한다).
Spark 조회(MEASURE)는 Databricks에서만 돈다 (미검증).
"""


def compare_equal(checks: dict) -> list[str]:
    """checks = {이름: (실제, 기대)}. 다른 것만 문제로 돌려준다."""
    return [f"{name}: 기대 {expected}, 실제 {actual}" for name, (actual, expected) in checks.items() if actual != expected]


def measure_sql(view: str, measures: dict, group_by: str = "") -> str:
    """MEASURE()로 지표를 조회하는 문장. measures = {결과 열 이름: 지표 이름}. group_by가 있으면 그 열도 함께 조회한다."""
    columns = ", ".join(f"MEASURE({measure}) AS {alias}" for alias, measure in measures.items())
    if group_by:
        return f"SELECT {group_by}, {columns} FROM {view} GROUP BY {group_by}"
    return f"SELECT {columns} FROM {view}"


def refund_amount_pair(spark, serving: str) -> tuple:
    """(sales_metrics 환불금액, refund_metrics 환불금액) 전체 합."""
    sales = spark.sql(measure_sql(f"{serving}.sales_metrics", {"v": "refund_amount"})).first()["v"]
    refund = spark.sql(measure_sql(f"{serving}.refund_metrics", {"v": "refund_amount"})).first()["v"]
    return sales or 0, refund or 0


def check_metric_views(spark, serving_schema: str) -> list[str]:
    """Metric View를 자체 정합성 규칙으로 확인해 문제 문장 목록을 돌려준다. 빈 목록이면 통과한다."""
    sales_refund, refund_refund = refund_amount_pair(spark, serving_schema)
    return compare_equal({"환불금액 sales_metrics vs refund_metrics": (sales_refund, refund_refund)})
