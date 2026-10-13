"""Metric View가 독립 기대값(expected.json)과 같은 값을 내는지 확인한다. 노트북에서 호출만 한다.

[입력] Serving 스키마 이름(예: "commerce_poc.serving"), expected.json 경로
[출력] 문제 문장 목록. 빈 목록이면 모두 일치한다.
[확인 항목] (plans/05 완료 조건)
  1. 지표 9개(총·순판매금액, 환불금액, 환불수량, 환불금액비율, 상품결제건수, 상품결제단가, 결제고객수, 객단가)가
     expected.json의 채널별·total과 같다. 판매 지표는 sales_metrics, 환불수량은 refund_metrics에서 읽는다.
  2. 두 Metric View의 환불금액이 같다 (판매 쪽 합계 = 환불 쪽 합계).
  3. 비가산 지표가 조회 범위에서 다시 계산된다: 채널별 객단가를 더하지 않고 total을 직접 조회한 값이 기대 total과 같다 (1에 포함).
비교는 gold_check.compare_metrics를 다시 쓴다. 노트북이 02_pipeline/gold를 import 경로에 넣어 둔다.
Spark 조회(MEASURE)는 Databricks에서만 돈다 (미검증).
"""
import json
from pathlib import Path

from gold_check import CHANNELS, DEFAULT_EXPECTED, RAW_KEYS, compare_equal, compare_metrics

SALES_MEASURES = {"total_sales": "total_sales", "refund_amount": "refund_amount", "paid_item_count": "item_count",
                  "paying_customers": "paying_customers", "net_sales": "net_sales", "refund_ratio": "refund_ratio",
                  "unit_price_per_item": "unit_price_per_item", "average_order_value": "average_order_value"}


def measure_sql(view: str, measures: dict, group_by: str = "") -> str:
    """MEASURE()로 지표를 조회하는 문장. measures = {결과 열 이름: 지표 이름}. group_by가 있으면 그 열도 함께 조회한다."""
    columns = ", ".join(f"MEASURE({measure}) AS {alias}" for alias, measure in measures.items())
    if group_by:
        return f"SELECT {group_by}, {columns} FROM {view} GROUP BY {group_by}"
    return f"SELECT {columns} FROM {view}"


def read_scopes(spark, serving: str) -> dict:
    """{채널 또는 "total": 지표 dict}. 없는 채널은 값이 None인 dict라 비교에서 문제로 나온다."""
    sales_view, refund_view = f"{serving}.sales_metrics", f"{serving}.refund_metrics"
    refund_quantity = {row["channel"]: row["refund_quantity"] for row in
                       spark.sql(measure_sql(refund_view, {"refund_quantity": "refund_quantity"}, "channel")).collect()}
    by_channel = {row["channel"]: row.asDict() for row in
                  spark.sql(measure_sql(sales_view, SALES_MEASURES, "channel")).collect()}
    total = spark.sql(measure_sql(sales_view, SALES_MEASURES)).first().asDict()
    total_refund_quantity = spark.sql(measure_sql(refund_view, {"refund_quantity": "refund_quantity"})).first()["refund_quantity"]
    keys = (*RAW_KEYS, "net_sales", "refund_ratio", "unit_price_per_item", "average_order_value")
    scopes = {}
    for channel in CHANNELS:
        row = by_channel.get(channel)
        scopes[channel] = ({**row, "refund_quantity": refund_quantity.get(channel, 0)} if row
                           else dict.fromkeys(keys))
    scopes["total"] = {**total, "refund_quantity": total_refund_quantity or 0}
    return scopes


def refund_amount_pair(spark, serving: str) -> tuple:
    """(sales_metrics 환불금액, refund_metrics 환불금액) 전체 합."""
    sales = spark.sql(measure_sql(f"{serving}.sales_metrics", {"v": "refund_amount"})).first()["v"]
    refund = spark.sql(measure_sql(f"{serving}.refund_metrics", {"v": "refund_amount"})).first()["v"]
    return sales or 0, refund or 0


def check_metric_views(spark, serving_schema: str, expected_path=DEFAULT_EXPECTED) -> list[str]:
    """Metric View 전체를 기대값과 비교해 문제 문장 목록을 돌려준다. 빈 목록이면 일치한다."""
    expected = json.loads(Path(expected_path).read_text(encoding="utf-8"))
    sales_refund, refund_refund = refund_amount_pair(spark, serving_schema)
    return (compare_metrics(read_scopes(spark, serving_schema), expected)
            + compare_equal({"환불금액 sales_metrics vs refund_metrics": (sales_refund, refund_refund)}))
