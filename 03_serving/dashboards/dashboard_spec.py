"""대시보드에 무엇을 보여줄지 정의한다: 데이터셋(조회)과 위젯(KPI·차트·표). Lakeview JSON 모양은 build_dashboard.py가 만든다.

[데이터셋] 이름, Metric View, 분석 기준(dims), 지표(measures), 정렬, 구성비 열(share_of)
  쿼리는 Metric View를 MEASURE()로 조회한다: SELECT 기준열, MEASURE(지표) AS 지표 FROM 뷰 GROUP BY ALL ORDER BY ...
  share_of를 주면 "그 열 / 전체 합" 열(<열>_share)을 더한다 (사유별 비중). 구성비는 비가산이라 지표가 아니라 조회 단계에서 계산한다.
[위젯] kind = counter(KPI 숫자 하나) | bar(막대) | table(표). 위치(x, y, width, height)는 6칸 격자 기준이다.
[페이지] 판매(sales_*) / 환불(refund_*)
[필터] 기간(결제일)과 채널을 모든 페이지 맨 위(전역 필터)에 둔다. 필터는 데이터셋 쿼리의 매개변수(:date_range, :channels)다.
  결과 열에 필터 기준을 넣어 화면에서 거르는 방식은 쓰지 않는다: 이미 집계된 행을 다시 더하면 결제고객수·객단가·비율이 틀린다.
  매개변수는 Metric View 조회의 WHERE에 들어가므로 선택한 범위에서 지표가 다시 계산된다. 환불 지표의 기간도 원결제일 기준이다 (event_spec 6.3).
"""

SERVING = "commerce_poc.serving"
GOLD = "commerce_poc.gold"

# 채널 필터의 선택지와 기본값. 값은 dim_channel.channel_name과 같아야 한다 (테스트가 대조한다).
CHANNEL_NAMES = ["자사몰", "종합 오픈마켓", "스포츠 전문몰"]
DEFAULT_DATE_RANGE = ("now-1y/d", "now/d")   # Lakeview 상대 날짜 표현: 1년 전 ~ 오늘

# 필터: param = 매개변수 이름, dimension = Metric View 분석 기준. 선택지 데이터셋(options)은 채널 필터만 쓴다.
FILTERS = [
    {"param": "date_range", "kind": "date_range", "title": "기간 (결제일)", "dimension": "payment_date"},
    {"param": "channels", "kind": "multi_select", "title": "채널", "dimension": "channel_name", "options": "channel_options"},
]
OPTION_DATASETS = [
    {"name": "channel_options", "query": f"SELECT channel_name FROM {GOLD}.dim_channel ORDER BY channel_name",
     "columns": ["channel_name"]},
]

# 열 이름 -> 화면에 보일 이름
LABELS = {
    "total_sales": "총판매금액", "refund_amount": "환불금액", "net_sales": "순판매금액", "refund_ratio": "환불금액비율",
    "item_count": "상품결제건수", "unit_price_per_item": "상품결제단가", "paying_customers": "결제고객수",
    "average_order_value": "객단가", "refund_quantity": "환불수량", "refund_amount_share": "환불금액 비중",
    "payment_date": "결제일", "channel_name": "채널", "method_name": "결제수단", "category": "카테고리",
    "refund_reason": "환불 사유", "refund_type": "환불 유형",
}

SALES_ALL = ["total_sales", "refund_amount", "net_sales", "refund_ratio", "item_count", "unit_price_per_item",
             "paying_customers", "average_order_value"]

DATASETS = [
    {"name": "sales_kpi", "view": "sales_metrics", "dims": [], "measures": SALES_ALL},
    {"name": "sales_by_date", "view": "sales_metrics", "dims": ["payment_date"],
     "measures": ["total_sales", "refund_amount", "net_sales"], "order": "payment_date"},
    {"name": "sales_by_channel", "view": "sales_metrics", "dims": ["channel_name"], "measures": SALES_ALL,
     "order": "channel_name"},
    {"name": "sales_by_method", "view": "sales_metrics", "dims": ["method_name"],
     "measures": ["total_sales", "item_count"], "order": "method_name"},
    {"name": "sales_by_category", "view": "sales_metrics", "dims": ["category"],
     "measures": ["total_sales", "refund_amount", "net_sales"], "order": "category"},
    {"name": "refund_kpi", "view": "refund_metrics", "dims": [], "measures": ["refund_amount", "refund_quantity"]},
    {"name": "refund_by_date", "view": "refund_metrics", "dims": ["payment_date"],
     "measures": ["refund_amount", "refund_quantity"], "order": "payment_date"},
    {"name": "refund_by_reason", "view": "refund_metrics", "dims": ["refund_reason"],
     "measures": ["refund_amount", "refund_quantity"], "share_of": "refund_amount", "order": "refund_reason"},
    {"name": "refund_by_channel", "view": "refund_metrics", "dims": ["channel_name"],
     "measures": ["refund_amount", "refund_quantity"], "order": "channel_name"},
]

# 위젯: dataset, kind, title, 보여줄 열(counter는 1개, bar는 x와 y 목록, table은 열 목록), 위치
PAGES = [
    {"name": "sales", "title": "판매", "widgets": [
        *[{"dataset": "sales_kpi", "kind": "counter", "title": LABELS[m], "value": m,
           "x": 2 * (i % 3), "y": 3 * (i // 3), "width": 2, "height": 3} for i, m in enumerate(SALES_ALL)],
        {"dataset": "sales_by_date", "kind": "bar", "title": "결제일별 판매·환불", "x_field": "payment_date",
         "y_fields": ["total_sales", "refund_amount"], "x": 0, "y": 9, "width": 6, "height": 5},
        {"dataset": "sales_by_channel", "kind": "table", "title": "채널별 지표", "columns": ["channel_name", *SALES_ALL],
         "x": 0, "y": 14, "width": 6, "height": 4},
        {"dataset": "sales_by_method", "kind": "bar", "title": "결제수단별 총판매금액", "x_field": "method_name",
         "y_fields": ["total_sales"], "x": 0, "y": 18, "width": 3, "height": 5},
        {"dataset": "sales_by_category", "kind": "bar", "title": "카테고리별 총판매금액", "x_field": "category",
         "y_fields": ["total_sales"], "x": 3, "y": 18, "width": 3, "height": 5},
    ]},
    {"name": "refund", "title": "환불", "widgets": [
        {"dataset": "refund_kpi", "kind": "counter", "title": "환불금액", "value": "refund_amount",
         "x": 0, "y": 0, "width": 3, "height": 3},
        {"dataset": "refund_kpi", "kind": "counter", "title": "환불수량", "value": "refund_quantity",
         "x": 3, "y": 0, "width": 3, "height": 3},
        {"dataset": "refund_by_reason", "kind": "table", "title": "사유별 환불",
         "columns": ["refund_reason", "refund_amount", "refund_quantity", "refund_amount_share"],
         "x": 0, "y": 3, "width": 3, "height": 5},
        {"dataset": "refund_by_reason", "kind": "bar", "title": "사유별 환불금액", "x_field": "refund_reason",
         "y_fields": ["refund_amount"], "x": 3, "y": 3, "width": 3, "height": 5},
        {"dataset": "refund_by_date", "kind": "bar", "title": "결제일별 환불금액 (원결제일 귀속)", "x_field": "payment_date",
         "y_fields": ["refund_amount"], "x": 0, "y": 8, "width": 3, "height": 5},
        {"dataset": "refund_by_channel", "kind": "bar", "title": "채널별 환불금액", "x_field": "channel_name",
         "y_fields": ["refund_amount"], "x": 3, "y": 8, "width": 3, "height": 5},
    ]},
]


def dataset_columns(dataset: dict) -> list:
    """데이터셋 쿼리가 돌려주는 열 이름 순서."""
    share = [f"{dataset['share_of']}_share"] if dataset.get("share_of") else []
    return [*dataset["dims"], *dataset["measures"], *share]


def wrap_query(dataset: dict, inner_sql: str) -> str:
    """inner_sql(기준열+지표 열)에 구성비 열과 정렬을 붙여 최종 쿼리를 만든다. 로컬 테스트도 같은 함수를 쓴다."""
    sql = inner_sql
    if dataset.get("share_of"):
        column = dataset["share_of"]
        sql = (f"SELECT *, CAST({column} AS DOUBLE) / SUM({column}) OVER () AS {column}_share "
               f"FROM ({inner_sql}) AS base")
    if dataset.get("order"):
        sql += f" ORDER BY {dataset['order']}"
    return sql


def where_clause() -> str:
    """필터를 거는 WHERE 조건 (매개변수 표기). 모든 데이터셋이 같은 조건을 쓴다."""
    return "payment_date BETWEEN :date_range.min AND :date_range.max AND channel_name IN (:channels)"


def dataset_query(dataset: dict, serving: str = SERVING, filters: bool = True) -> str:
    """Databricks에서 실행할 데이터셋 쿼리 (Metric View를 MEASURE()로 조회하고 필터를 WHERE로 건다).

    filters=False면 WHERE 없이 전체 범위를 조회한다 (필터를 붙이기 전의 첫 대시보드 모양)."""
    columns = [*dataset["dims"], *(f"MEASURE({m}) AS {m}" for m in dataset["measures"])]
    inner = f"SELECT {', '.join(columns)} FROM {serving}.{dataset['view']}"
    if filters:
        inner += f" WHERE {where_clause()}"
    if dataset["dims"]:
        inner += " GROUP BY ALL"
    return wrap_query(dataset, inner)
