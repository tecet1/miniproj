"""대시보드에 무엇을 보여줄지 정의한다: 데이터셋(조회)과 위젯(KPI·차트·표). Lakeview JSON 모양은 build_dashboard.py가 만든다. (plans/05 카드 5-F·5-G)

[필터] 채널·카테고리(다중 선택)와 조회 기간(결제일 범위)을 모든 페이지 맨 위(전역 필터)에 둔다. 데이터셋 쿼리의 매개변수
  (:channels, :categories, :date_range.min/.max)이며 Metric View 조회의 WHERE에 들어간다. 이미 집계된 행을 화면에서 다시 거르면
  결제고객수·객단가·비율이 틀리므로 쓰지 않는다. 환불 지표의 기간도 원결제일 기준이다 (event_spec 6.3).
[비교 기간] 쿼리 안에서 조회 기간으로 계산한다 (Databricks 매개변수는 다른 매개변수로 만들 수 없다).
  이전 기간 = 바로 앞의 같은 길이 구간, 전년 동기 = 1년 전 같은 구간. KPI 증감은 기간마다 Metric View를 다시 조회해 비교한다.
[데이터셋] 두 가지다.
  일반   이름, Metric View, 분석 기준(dims), 지표(measures), 정렬(order), 구성비 열(share_of). 쿼리는 MEASURE()로 조회한다.
  KPI    "kpi": {지표: 표시 방식}. 한 행에 현재값 `지표`, 화면용 문장 `지표_text`("12,345원"), 증감 문장 `지표_delta`를 낸다.
         큰 카드: "▲ 12.3% 직전 기간 대비 · 전년 동기: ▲ 5.0%" (환불금액비율은 %p, 전년 동기 줄 없음), 작은 카드: "▲ 12.3%".
         비교 기간에 상품결제건수가 0이면 "실적이 없습니다."다. 상품결제건수는 sales_metrics에서 읽는다.
[위젯] kind = counter(숫자·문장 하나) | bar(막대, horizontal이면 가로) | line(선) | table(표). 위치는 6칸 격자 기준이다.
[로컬 검증] 쿼리가 쓰는 date_sub·datediff·add_months·weekday·date_format·format_number는 Spark 함수다. 로컬 테스트는 같은 이름의
  함수를 sqlite에 등록해 같은 문장을 실행한다 (등록한 함수의 동작이 Spark와 같은지는 Databricks에서만 확인된다).
"""

SERVING = "commerce_poc.serving"
GOLD = "commerce_poc.gold"

# 필터 선택지와 기본값. 값은 Dimension 테이블과 같아야 한다 (테스트가 대조한다).
CHANNEL_NAMES = ["자사몰", "종합 오픈마켓", "스포츠 전문몰"]
CATEGORIES = ["가방", "신발", "액세서리", "의류", "캠핑·등산 장비"]
DEFAULT_DATE_RANGE = ("now-30d/d", "now/d")   # Lakeview 상대 날짜 표현: 30일 전 ~ 오늘

# 채널 색은 모든 차트에서 고정한다 (색약 시뮬레이션 검증을 통과한 값; 청록은 대비 경고가 있어 범례·직접 라벨이 필요하다).
CHANNEL_COLORS = {"자사몰": "#2a78d6", "종합 오픈마켓": "#eb6834", "스포츠 전문몰": "#1baf7a"}
NEUTRAL_COLOR = "#5b6573"   # 채널이 아닌 단일 계열

# 필터: param = 매개변수 이름, dimension = Metric View 분석 기준. options = 선택지 데이터셋(날짜 범위는 없다).
FILTERS = [
    {"param": "channels", "kind": "multi_select", "title": "채널", "dimension": "channel_name", "options": "channel_options"},
    {"param": "categories", "kind": "multi_select", "title": "카테고리", "dimension": "category", "options": "category_options"},
    {"param": "date_range", "kind": "date_range", "title": "조회 기간 (결제일)", "dimension": "payment_date"},
]
OPTION_DATASETS = [
    {"name": "channel_options", "query": f"SELECT channel_name FROM {GOLD}.dim_channel ORDER BY channel_name",
     "columns": ["channel_name"]},
    {"name": "category_options", "query": f"SELECT DISTINCT category FROM {GOLD}.dim_product ORDER BY category",
     "columns": ["category"]},
]

# 열 이름 -> 화면에 보일 이름
LABELS = {
    "total_sales": "총판매금액", "refund_amount": "환불금액", "net_sales": "순판매금액", "refund_ratio": "환불금액비율",
    "item_count": "상품결제건수", "unit_price_per_item": "상품결제단가", "paying_customers": "결제고객수",
    "average_order_value": "객단가", "refund_quantity": "환불수량", "refund_amount_share": "환불금액 비중",
    "net_sales_share": "순판매금액 비중", "total_sales_share": "총판매금액 비중", "payment_week": "결제주(월요일)", "channel_name": "채널",
    "method_name": "결제수단", "category": "카테고리", "refund_reason": "환불 사유", "refund_type": "환불 유형",
}

def _kpi(view, unit, yoy=False, points=False, short=False) -> dict:
    """KPI 하나의 표시 방식. unit = 숫자 뒤 단위(%면 비율을 퍼센트로), yoy = 전년 동기 줄을 둔다, points = 증감을 %p(비율의 차)로,
    short = 작은 카드(증감 숫자만)."""
    return {"view": view, "unit": unit, "yoy": yoy, "points": points, "short": short}


SALES_KPI = {
    "total_sales": _kpi("sales_metrics", "원", yoy=True), "net_sales": _kpi("sales_metrics", "원", yoy=True),
    "refund_ratio": _kpi("sales_metrics", "%", points=True),
    "paying_customers": _kpi("sales_metrics", "명", short=True),
    "average_order_value": _kpi("sales_metrics", "원", short=True),
    "item_count": _kpi("sales_metrics", "건", short=True), "unit_price_per_item": _kpi("sales_metrics", "원", short=True),
    "refund_amount": _kpi("sales_metrics", "원", short=True),
}
REFUND_KPI = {"refund_amount": _kpi("refund_metrics", "원", yoy=True),
              "refund_quantity": _kpi("refund_metrics", "개", yoy=True),
              "refund_ratio": _kpi("sales_metrics", "%", points=True)}

DATASETS = [
    {"name": "sales_kpi", "kpi": SALES_KPI},
    {"name": "refund_kpi", "kpi": REFUND_KPI},
    {"name": "sales_trend_by_channel_week", "view": "sales_metrics", "dims": ["payment_week", "channel_name"],
     "measures": ["total_sales"], "order": "payment_week, channel_name"},
    {"name": "refund_ratio_by_week", "view": "sales_metrics", "dims": ["payment_week"], "measures": ["refund_ratio"],
     "order": "payment_week"},
    {"name": "channel_net_share", "view": "sales_metrics", "dims": ["channel_name"], "measures": ["net_sales"],
     "share_of": "net_sales", "order": "net_sales DESC, channel_name"},
    {"name": "sales_by_method", "view": "sales_metrics", "dims": ["method_name"], "measures": ["total_sales"],
     "share_of": "total_sales", "order": "total_sales DESC, method_name"},
    {"name": "sales_by_category", "view": "sales_metrics", "dims": ["category"], "measures": ["total_sales"],
     "share_of": "total_sales", "order": "total_sales DESC, category"},
    {"name": "refund_ratio_by_channel", "view": "sales_metrics", "dims": ["channel_name"], "measures": ["refund_ratio"],
     "order": "refund_ratio DESC, channel_name"},
    {"name": "sales_by_channel", "view": "sales_metrics", "dims": ["channel_name"],
     "measures": ["total_sales", "net_sales", "refund_ratio", "paying_customers", "average_order_value"],
     "order": "total_sales DESC, channel_name"},
    {"name": "refund_trend_by_week", "view": "refund_metrics", "dims": ["payment_week"], "measures": ["refund_amount"],
     "order": "payment_week"},
    {"name": "refund_by_reason", "view": "refund_metrics", "dims": ["refund_reason"],
     "measures": ["refund_amount", "refund_quantity"], "share_of": "refund_amount",
     "order": "refund_amount DESC, refund_reason"},
]

# 와이어프레임의 문구 (카드 아래 작은 글씨)
NOTE_COMPARE = "비교 기간(자동): 직전 같은 길이 · 전년 동기"
NOTE_REFUND_BASIS = "환불은 원결제일 기준으로 귀속합니다"
NOTE_LOWER_BETTER = "낮을수록 좋은 지표입니다."


def _kpi_card(dataset: str, measure: str, x: int, y: int, width: int, value_height: int, note: str = "") -> list:
    """KPI 카드 하나 = 값 위젯(`지표_text`, 제목 있음) + 바로 아래 증감 위젯(`지표_delta`, 제목 없음). 와이어프레임의 한 카드처럼 붙여 놓는다."""
    return [{"dataset": dataset, "kind": "counter", "title": LABELS[measure], "value": f"{measure}_text", "note": note,
             "x": x, "y": y, "width": width, "height": value_height},
            {"dataset": dataset, "kind": "counter", "title": "", "value": f"{measure}_delta",
             "x": x, "y": y + value_height, "width": width, "height": 1 if value_height <= 2 else 2}]


def _kpi_row(dataset: str, measures: list, y: int) -> list:
    """작은 카드 5개 한 줄 (폭 1,1,1,1,2)."""
    widths = [1, 1, 1, 1, 2]
    xs = [sum(widths[:i]) for i in range(len(widths))]
    return [w for m, x, wd in zip(measures, xs, widths) for w in _kpi_card(dataset, m, x, y, wd, 2)]


def _kpi_trio(dataset: str, measures: list, y: int, first_note: str) -> list:
    """큰 카드 3개: 첫 카드(가장 큼)는 왼쪽 절반, 나머지 둘은 오른쪽 절반에 위아래로 쌓는다 (와이어프레임의 2:1:1 비율을 6칸 격자에 맞춘 것)."""
    first, second, third = measures
    return [*_kpi_card(dataset, first, 0, y, 3, 4, first_note),
            *_kpi_card(dataset, second, 3, y, 3, 2),
            *_kpi_card(dataset, third, 3, y + 3, 3, 2, NOTE_LOWER_BETTER)]


# 위젯: dataset, kind, title, 보여줄 열(counter는 value 하나, bar·line은 x_field와 y_fields, table은 columns), 위치.
# 선택 항목: note(제목 아래 작은 설명), color_field(계열 열, 채널이면 고정 색), horizontal(가로 막대).
PAGES = [
    {"name": "sales", "title": "판매", "widgets": [
        *_kpi_trio("sales_kpi", ["total_sales", "net_sales", "refund_ratio"], 0, NOTE_COMPARE),
        *_kpi_row("sales_kpi", ["paying_customers", "average_order_value", "item_count", "unit_price_per_item",
                                "refund_amount"], 6),
        {"dataset": "sales_trend_by_channel_week", "kind": "line", "title": "채널별 총판매금액 추이 (주별)",
         "note": "첫 주와 마지막 주는 부분 기간일 수 있어 표시합니다.",
         "x_field": "payment_week", "y_fields": ["total_sales"], "color_field": "channel_name",
         "x": 0, "y": 9, "width": 6, "height": 6},
        {"dataset": "channel_net_share", "kind": "bar", "title": "채널별 순판매금액 비중 (큰 순)", "x_field": "channel_name",
         "y_fields": ["net_sales_share"], "horizontal": True, "color_field": "channel_name",
         "x": 0, "y": 15, "width": 3, "height": 6},
        {"dataset": "refund_ratio_by_week", "kind": "line", "title": "환불금액비율 추이 (주별, 채널 합계)",
         "note": "주별 값은 그 주의 분자·분모로 다시 계산한 값입니다.", "x_field": "payment_week",
         "y_fields": ["refund_ratio"], "x": 3, "y": 15, "width": 3, "height": 6},
        {"dataset": "sales_by_method", "kind": "bar", "title": "결제수단별 총판매금액 (큰 순)", "x_field": "method_name",
         "y_fields": ["total_sales_share"], "horizontal": True, "x": 0, "y": 21, "width": 3, "height": 6},
        {"dataset": "sales_by_category", "kind": "bar", "title": "카테고리별 총판매금액 (큰 순)", "x_field": "category",
         "y_fields": ["total_sales_share"], "horizontal": True, "x": 3, "y": 21, "width": 3, "height": 6},
        {"dataset": "sales_by_channel", "kind": "table", "title": "채널별 지표 (상세)",
         "columns": ["channel_name", "total_sales", "net_sales", "refund_ratio", "paying_customers",
                     "average_order_value"],
         "x": 0, "y": 27, "width": 6, "height": 4},
    ]},
    {"name": "refund", "title": "환불", "widgets": [
        *_kpi_trio("refund_kpi", ["refund_amount", "refund_quantity", "refund_ratio"], 0, NOTE_REFUND_BASIS),
        {"dataset": "refund_by_reason", "kind": "bar", "title": "사유별 환불금액 (큰 순 · 비중)", "x_field": "refund_reason",
         "note": "금액·수량 상세는 아래 표에서 봅니다.", "y_fields": ["refund_amount_share"], "horizontal": True,
         "x": 0, "y": 6, "width": 3, "height": 6},
        {"dataset": "refund_ratio_by_channel", "kind": "bar", "title": "채널별 환불금액비율 (큰 순)",
         "x_field": "channel_name", "note": "금액이 아니라 비율로 비교해야 채널 규모 차이가 가려지지 않습니다.",
         "y_fields": ["refund_ratio"], "horizontal": True, "color_field": "channel_name",
         "x": 3, "y": 6, "width": 3, "height": 6},
        {"dataset": "refund_trend_by_week", "kind": "line", "title": "결제일별 환불금액 추이 (원결제일 귀속 · 주별)",
         "note": "최근 주는 아직 환불이 접수 중일 수 있어 값이 낮게 보일 수 있습니다.", "x_field": "payment_week",
         "y_fields": ["refund_amount"], "x": 0, "y": 12, "width": 6, "height": 6},
        {"dataset": "refund_by_reason", "kind": "table", "title": "사유별 환불 (상세)",
         "columns": ["refund_reason", "refund_amount", "refund_quantity", "refund_amount_share"],
         "x": 0, "y": 18, "width": 6, "height": 5},
    ]},
]

# ── 쿼리 만들기 ──────────────────────────────────────────────────────────────────
MIN, MAX = ":date_range.min", ":date_range.max"
# 기간 이름 -> (시작 식, 끝 식). 모든 데이터셋과 "비교 기간" 표시가 같은 식을 쓴다.
PERIODS = {
    "cur": (MIN, MAX),
    "prev": (f"date_sub({MIN}, datediff({MAX}, {MIN}) + 1)", f"date_sub({MIN}, 1)"),
    "yoy": (f"add_months({MIN}, -12)", f"add_months({MAX}, -12)"),
}


def dataset_columns(dataset: dict) -> list:
    """데이터셋 쿼리가 돌려주는 열 이름 순서."""
    if dataset.get("kpi"):
        measures = list(dataset["kpi"])
        return [*measures, *(f"{m}_text" for m in measures), *(f"{m}_delta" for m in measures)]
    share = [f"{dataset['share_of']}_share"] if dataset.get("share_of") else []
    return [*dataset["dims"], *dataset["measures"], *share]


def where_clause(period: str = "cur") -> str:
    """필터를 거는 WHERE 조건 (매개변수 표기). period가 prev·yoy면 기간만 비교 기간으로 바뀌고 채널·카테고리는 같다."""
    low, high = PERIODS[period]
    return (f"payment_date BETWEEN {low} AND {high} AND channel_name IN (:channels) AND category IN (:categories)")


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


def delta_text(current: str, compare: str, compare_items: str, points: bool = False, prefix: str = "",
               suffix: str = "", empty_prefix: str = "") -> str:
    """증감 문장 한 조각. 비교 기간에 결제 건수가 없으면 "실적이 없습니다.", 비교할 수 없으면 "-", 아니면 "▲ 12.3%" 같은 방향 기호와 숫자.
    points면 비율의 차를 %p로 보여 준다(환불금액비율). prefix·suffix는 숫자 문장 앞뒤 글, empty_prefix는 실적 없음·"-" 앞 글이다.
    상태 색(좋음/나쁨)은 쓰지 않는다. 정수끼리 나누는 오차를 피하려 100.0을 먼저 곱한다."""
    arrow = f"(CASE WHEN {current} > {compare} THEN '▲ ' WHEN {current} < {compare} THEN '▼ ' ELSE '' END)"
    if points:
        body = f"format_number(ABS({current} - {compare}) * 100.0, 1) || '%p'"
        unusable = f"{current} IS NULL OR {compare} IS NULL"
    else:
        body = f"format_number(ABS({current} - {compare}) * 100.0 / ABS({compare}), 1) || '%'"
        unusable = f"{current} IS NULL OR {compare} IS NULL OR {compare} = 0"
    return (f"CASE WHEN {compare_items} IS NULL OR {compare_items} = 0 THEN '{empty_prefix}실적이 없습니다.' "
            f"WHEN {unusable} THEN '{empty_prefix}-' "
            f"ELSE '{prefix}' || {arrow} || {body} || '{suffix}' END")


def value_text(column: str, unit: str) -> str:
    """값을 화면 문장으로: 천 단위 쉼표 + 단위. 비율(%)은 퍼센트로 소수 한 자리. 값이 없으면 "-"."""
    if unit == "%":
        return f"COALESCE(format_number({column} * 100.0, 1) || '%', '-')"
    return f"COALESCE(format_number({column}, 0) || '{unit}', '-')"


def kpi_query(dataset: dict, select) -> str:
    """KPI 쿼리: 기간(현재·이전·전년)과 뷰마다 지표를 조회하는 CTE를 만들고, 한 행으로 합쳐 값 문장과 증감 문장을 붙인다.

    select(view, measures, where)는 지표 조회 문장을 돌려주는 함수다. Databricks는 MEASURE() 조회를, 로컬 테스트는 같은 정의의 일반 SQL을 넘긴다.
    상품결제건수(sales_metrics)는 비교 기간의 실적 유무 판단에 항상 함께 조회한다."""
    groups = {}
    for measure, shape in dataset["kpi"].items():
        groups.setdefault(shape["view"], []).append(measure)
    sales = groups.setdefault("sales_metrics", [])
    if "item_count" not in sales:
        sales.append("item_count")
    ctes = [f"{p}_{view} AS ({select(view, ms, where_clause(p))})" for p in PERIODS for view, ms in groups.items()]
    values, texts, deltas = [], [], []
    for m, shape in dataset["kpi"].items():
        cur = f"cur_{shape['view']}.{m}"
        values.append(f"{cur} AS {m}")
        texts.append(f"{value_text(cur, shape['unit'])} AS {m}_text")

        def part(period, **kw):
            return delta_text(cur, f"{period}_{shape['view']}.{m}", f"{period}_sales_metrics.item_count",
                              points=shape["points"], **kw)
        if shape["short"]:
            expr = part("prev")
        else:
            expr = part("prev", suffix=" 직전 기간 대비", empty_prefix="직전 기간: ")
            if shape["yoy"]:
                expr = f"{expr} || ' · ' || " + part("yoy", prefix="전년 동기: ", empty_prefix="전년 동기: ")
        deltas.append(f"{expr} AS {m}_delta")
    tables = " CROSS JOIN ".join(f"{p}_{view}" for p in PERIODS for view in groups)
    return f"WITH {', '.join(ctes)} SELECT {', '.join([*values, *texts, *deltas])} FROM {tables}"


def spark_select(serving: str = SERVING):
    """Databricks용 select 함수: Metric View를 MEASURE()로 조회한다."""
    def select(view: str, measures: list, where: str, dims: list = ()) -> str:
        columns = [*dims, *(f"MEASURE({m}) AS {m}" for m in measures)]
        sql = f"SELECT {', '.join(columns)} FROM {serving}.{view} WHERE {where}"
        return sql + (" GROUP BY ALL" if dims else "")
    return select


def dataset_query(dataset: dict, serving: str = SERVING) -> str:
    """Databricks에서 실행할 데이터셋 쿼리."""
    select = spark_select(serving)
    if dataset.get("kpi"):
        return kpi_query(dataset, select)
    return wrap_query(dataset, select(dataset["view"], dataset["measures"], where_clause(), dataset["dims"]))


def uses_parameter(dataset: dict, param: str) -> bool:
    """데이터셋 쿼리가 매개변수를 쓰는지 (쓰지 않는 매개변수는 선언하지 않는다)."""
    return f":{param}" in dataset_query(dataset)
