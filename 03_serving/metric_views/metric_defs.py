"""Metric View 정의 2개와 YAML·CREATE 문장 생성. 지표 정의의 기준은 event_spec.md 6.5절이다.

  SALES_METRICS   fact_sales_item 기반: 총판매금액, 환불금액, 순판매금액, 환불금액비율, 상품결제건수, 상품결제단가,
                  결제고객수, 객단가. 분석 기준은 결제일·결제주·채널·상품·결제수단
  REFUND_METRICS  fact_refund 기반: 환불금액, 환불수량. 분석 기준은 결제일·결제주·환불일·채널·상품·환불 유형·사유
  render_yaml     정의 -> Metric View YAML 문자열
  create_view_sql 정의 -> CREATE OR REPLACE VIEW ... WITH METRICS 문장

정의는 이름-식 쌍의 목록이다. 식은 Spark SQL이며, 같은 식을 로컬(sqlite)에서도 실행해 값을 대조한다
(조인·COALESCE 없이 SUM·COUNT·NULLIF·CAST만 쓰는 이유). Fact 컬럼 중 Dimension에도 있는 이름(채널·상품·결제수단·환불 프로필)은
`source.`를 붙여 어느 쪽인지 분명히 한다.
비가산 지표(비율·단가·결제고객수·객단가)는 식에 분자·분모를 모두 넣어 조회 범위마다 다시 계산되게 한다.
"""
import json

# 비율 분모는 DOUBLE로 바꿔 정수 나눗셈을 피하고, 0이면 NULLIF로 NULL이 되게 한다.
_SALES = "SUM(item_paid_amount)"
# 결제주 = 결제일이 속한 주의 월요일(event_spec 6.5절). weekday()는 월요일이 0이다. 로컬 테스트는 같은 이름의 함수를 sqlite에 등록해 같은 식을 실행한다.
_WEEK = "date_sub(payment_date, weekday(payment_date))"
_CUSTOMERS = "COUNT(DISTINCT source.channel || '|' || source.user_id)"   # user_id가 NULL이면 식이 NULL이라 세지 않는다

SALES_METRICS = {
    "name": "sales_metrics",
    "source": "fact_sales_item",
    "joins": [
        ("dim_channel", "source.channel = dim_channel.channel"),
        ("dim_product", "source.product_id = dim_product.product_id"),
        ("dim_payment_method", "source.payment_method = dim_payment_method.payment_method"),
    ],
    "dimensions": [
        ("payment_date", "payment_date"),
        ("payment_week", _WEEK),
        ("channel", "source.channel"),
        ("channel_name", "dim_channel.channel_name"),
        ("product_id", "source.product_id"),
        ("product_name", "dim_product.product_name"),
        ("category", "dim_product.category"),
        ("payment_method", "source.payment_method"),
        ("method_name", "dim_payment_method.method_name"),
    ],
    "measures": [
        ("total_sales", _SALES),
        ("refund_amount", "SUM(refund_amount)"),
        ("net_sales", f"{_SALES} - SUM(refund_amount)"),
        ("refund_ratio", f"SUM(refund_amount) / NULLIF(CAST({_SALES} AS DOUBLE), 0)"),
        ("item_count", "COUNT(*)"),
        ("unit_price_per_item", f"{_SALES} / NULLIF(CAST(COUNT(*) AS DOUBLE), 0)"),
        ("paying_customers", _CUSTOMERS),
        ("average_order_value", f"{_SALES} / NULLIF(CAST({_CUSTOMERS} AS DOUBLE), 0)"),
    ],
}

REFUND_METRICS = {
    "name": "refund_metrics",
    "source": "fact_refund",
    "joins": [
        ("dim_channel", "source.channel = dim_channel.channel"),
        ("dim_product", "source.product_id = dim_product.product_id"),
        ("dim_refund_profile", "source.refund_profile_key = dim_refund_profile.refund_profile_key"),
    ],
    "dimensions": [
        ("payment_date", "payment_date"),
        ("payment_week", _WEEK),
        ("refund_date", "refund_date"),
        ("channel", "source.channel"),
        ("channel_name", "dim_channel.channel_name"),
        ("product_name", "dim_product.product_name"),
        ("category", "dim_product.category"),
        ("refund_type", "dim_refund_profile.refund_type"),
        ("refund_reason", "dim_refund_profile.refund_reason"),
    ],
    "measures": [
        ("refund_amount", "SUM(refund_amount)"),
        ("refund_quantity", "SUM(refund_quantity)"),
    ],
}

DEFINITIONS = {definition["name"]: definition for definition in (SALES_METRICS, REFUND_METRICS)}


def _entry(name: str, key: str, value: str) -> str:
    """YAML 목록 항목 하나. 값은 큰따옴표로 감싸 `|`·`'` 같은 문자가 YAML 문법과 섞이지 않게 한다."""
    return f"  - name: {name}\n    {key}: {json.dumps(value)}"


def render_yaml(definition: dict, gold_schema: str) -> str:
    """정의를 Metric View YAML 문자열로 만든다. gold_schema는 "commerce_poc.gold" 같은 카탈로그.스키마."""
    lines = ["version: 0.1", f"source: {gold_schema}.{definition['source']}", "joins:"]
    for table, condition in definition["joins"]:
        lines.append(f"  - name: {table}\n    source: {gold_schema}.{table}\n    on: {json.dumps(condition)}")
    lines.append("dimensions:")
    lines += [_entry(name, "expr", expr) for name, expr in definition["dimensions"]]
    lines.append("measures:")
    lines += [_entry(name, "expr", expr) for name, expr in definition["measures"]]
    return "\n".join(lines)


def create_view_sql(definition: dict, gold_schema: str, serving_schema: str) -> str:
    """CREATE OR REPLACE VIEW 문장. 다시 실행해도 같은 결과다."""
    return (f"CREATE OR REPLACE VIEW {serving_schema}.{definition['name']} WITH METRICS LANGUAGE YAML AS $$\n"
            f"{render_yaml(definition, gold_schema)}\n$$")
