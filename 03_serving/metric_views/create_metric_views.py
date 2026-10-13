"""Metric View 2개를 만든다 (sales_metrics, refund_metrics). 노트북에서 호출만 한다.

[입력] spark, Gold 스키마("commerce_poc.gold"), Serving 스키마("commerce_poc.serving")
[순서] 스키마 생성(없으면) -> 정의마다 CREATE OR REPLACE VIEW ... WITH METRICS. 다시 실행해도 안전하다.
[필요 조건] Metric View를 지원하는 Databricks Runtime(서버리스 포함)과 Unity Catalog 권한. Databricks에서만 실행된다 (미검증).
"""
from metric_defs import DEFINITIONS, create_view_sql


def create_metric_views(spark, gold_schema: str, serving_schema: str) -> list:
    """Metric View를 만들고 만든 뷰 이름 목록을 돌려준다."""
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {serving_schema}")
    for definition in DEFINITIONS.values():
        spark.sql(create_view_sql(definition, gold_schema, serving_schema))
    return [f"{serving_schema}.{name}" for name in DEFINITIONS]
