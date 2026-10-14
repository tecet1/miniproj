"""대시보드에 무엇을 보여줄지 정의한다: 데이터셋(조회)과 위젯(KPI·차트·표). Lakeview JSON 모양은 build_dashboard.py가 만든다. (plans/05 카드 5-F·5-G)

[선택기] 채널·카테고리(다중 선택), 조회 기간·비교 기간(결제일 범위)을 각 페이지 맨 위 캔버스에 둔다(전역 필터 패널은 쓰지 않는다).
  데이터셋 쿼리의 매개변수(:channels, :categories, :date_range.min/.max, :compare_range.min/.max)이며 Metric View 조회의 WHERE에
  들어간다. 이미 집계된 행을 화면에서 다시 거르면 결제고객수·객단가·비율이 틀리므로 쓰지 않는다. 환불 지표의 기간도 원결제일 기준이다 (event_spec 6.3).
[비교 기간] 사용자가 날짜 범위로 직접 고른다(2026-10-14 사용자 결정, 자동 계산 대체). 비교 표는 조회 기간과 비교 기간을 지표마다 비교한다.
[데이터셋] 두 가지다.
  일반   이름, Metric View, 분석 기준(dims), 지표(measures), 정렬(order), 구성비 열(share_of). 쿼리는 MEASURE()로 조회한다.
         KPI 숫자 카드도 일반 데이터셋(dims 없음)이다. 카드가 비교 쿼리에 의존하지 않게 해서, 비교 쿼리가 실패해도 카드는 뜬다.
  비교   "compare": {지표: 표시 방식}. 지표마다 한 행: 지표 이름, 현재 값 문장("12,345원"), 직전 기간 대비, 전년 동기 대비("▲ 12.3%").
         열: 지표, 조회 기간 값, 비교 기간 값, 증감. 비교 기간에 상품결제건수가 0이면 증감은 "실적이 없습니다.",
         비교할 수 없으면(비교값 0·없음) "-". 환불금액비율의 증감은 %p(비율의 차)다.
[위젯] kind = counter(숫자 하나) | bar(막대, horizontal이면 가로) | line(선) | table(표) | text(섹션 제목, 마크다운). 위치는 6칸 격자 기준이다.
  KPI 카드 하나는 counter 위젯 하나다. 증감은 카드에 붙이지 않고 비교 표 하나에 모은다 (Lakeview 위젯은 한 칸에 값 하나만 담는다).
  배치·문구의 기준은 plans/05 카드 5-G "최종 시안 기준 구현 명세"(Claude Design 시안)다.
[로컬 검증] 쿼리가 쓰는 weekday·date_sub(결제주)·format_number·array_contains는 Spark 함수다. 로컬 테스트는 같은 이름의
  함수를 sqlite에 등록해 같은 문장을 실행한다 (등록한 함수의 동작이 Spark와 같은지는 Databricks에서만 확인된다).
"""

SERVING = "commerce_poc.serving"
GOLD = "commerce_poc.gold"

# 필터 선택지와 기본값. 값은 Dimension 테이블과 같아야 한다 (테스트가 대조한다).
CHANNEL_NAMES = ["자사몰", "종합 오픈마켓", "스포츠 전문몰"]
CATEGORIES = ["가방", "신발", "액세서리", "의류", "캠핑·등산 장비"]
DEFAULT_DATE_RANGE = ("now-30d/d", "now/d")   # Lakeview 상대 날짜 표현: 30일 전 ~ 오늘 (31일)
DEFAULT_COMPARE_RANGE = ("now-61d/d", "now-31d/d")   # 기본 비교 기간: 바로 앞의 같은 길이 (사용자가 바꾼다)

# 채널 색은 모든 차트에서 고정한다 (색약 시뮬레이션 검증을 통과한 값; 청록은 대비 경고가 있어 범례·직접 라벨이 필요하다).
CHANNEL_COLORS = {"자사몰": "#2a78d6", "종합 오픈마켓": "#eb6834", "스포츠 전문몰": "#1baf7a"}
NEUTRAL_COLOR = "#5b6573"   # 채널이 아닌 단일 계열

# 선택기: param = 매개변수 이름, dimension = Metric View 분석 기준. options = 선택지 데이터셋(날짜 범위는 없다).
FILTERS = [
    {"param": "channels", "kind": "multi_select", "title": "채널", "dimension": "channel_name", "options": "channel_options"},
    {"param": "categories", "kind": "multi_select", "title": "카테고리", "dimension": "category", "options": "category_options"},
    {"param": "date_range", "kind": "date_range", "title": "조회 기간 (결제일)", "dimension": "payment_date"},
    {"param": "compare_range", "kind": "date_range", "title": "비교 기간 (결제일)", "dimension": "payment_date"},
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
    "net_sales_share": "순판매금액 비중", "total_sales_share": "총판매금액 비중", "payment_week": "결제주(월요일)",
    "channel_name": "채널", "method_name": "결제수단", "category": "카테고리", "refund_reason": "환불 사유",
    "refund_type": "환불 유형", "metric": "지표", "current_value": "조회 기간", "compare_value": "비교 기간",
    "change": "증감",
}

SALES_ALL = ["total_sales", "net_sales", "refund_ratio", "paying_customers", "average_order_value", "item_count",
             "unit_price_per_item", "refund_amount"]


def _shape(view: str, unit: str, points: bool = False) -> dict:
    """비교 표의 한 지표: 어느 뷰에서 읽는지, 값 뒤 단위(%면 비율을 퍼센트로), 증감을 %p(비율의 차)로 볼지."""
    return {"view": view, "unit": unit, "points": points}


SALES_COMPARE = {
    "total_sales": _shape("sales_metrics", "원"), "net_sales": _shape("sales_metrics", "원"),
    "refund_ratio": _shape("sales_metrics", "%", points=True), "paying_customers": _shape("sales_metrics", "명"),
    "average_order_value": _shape("sales_metrics", "원"), "item_count": _shape("sales_metrics", "건"),
    "unit_price_per_item": _shape("sales_metrics", "원"), "refund_amount": _shape("sales_metrics", "원"),
}
REFUND_COMPARE = {"refund_amount": _shape("refund_metrics", "원"), "refund_quantity": _shape("refund_metrics", "개"),
                  "refund_ratio": _shape("sales_metrics", "%", points=True)}

DATASETS = [
    {"name": "sales_kpi", "view": "sales_metrics", "dims": [], "measures": SALES_ALL},
    {"name": "refund_kpi", "view": "refund_metrics", "dims": [], "measures": ["refund_amount", "refund_quantity"]},
    {"name": "sales_compare", "compare": SALES_COMPARE},
    {"name": "refund_compare", "compare": REFUND_COMPARE},
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

# 시안의 문구 (제목 아래 작은 글씨)
NOTE_COMPARE = "조회 기간과 비교 기간은 맨 위 선택기에서 고릅니다."
NOTE_LOWER_BETTER = "낮을수록 좋은 지표입니다."
COMPARE_COLUMNS = ["metric", "current_value", "compare_value", "change"]
SELECTOR_ROWS = 2   # 맨 위 선택기 줄의 높이. 그 아래 위젯은 이만큼 내려간다


def _selectors() -> list:
    """페이지 맨 위 선택기 4개: 채널(폭 1), 카테고리(폭 1), 조회 기간(폭 2), 비교 기간(폭 2)."""
    places = {"channels": (0, 1), "categories": (1, 1), "date_range": (2, 2), "compare_range": (4, 2)}
    return [{"kind": "filter", "param": f["param"], "title": f["title"], "x": places[f["param"]][0], "y": 0,
             "width": places[f["param"]][1], "height": SELECTOR_ROWS} for f in FILTERS]


def _below_selectors(widgets: list) -> list:
    """시안 배치(y는 선택기 없이 0부터)를 선택기 줄 아래로 내린다."""
    return [{**w, "y": w["y"] + SELECTOR_ROWS} for w in widgets]


def _card(dataset: str, measure: str, x: int, y: int, width: int, height: int, note: str = "") -> dict:
    """KPI 카드 = counter 위젯 하나 (숫자 서식은 화면에서 지정한다)."""
    return {"dataset": dataset, "kind": "counter", "title": LABELS[measure], "value": measure, "note": note,
            "x": x, "y": y, "width": width, "height": height}


def _section(title: str, y: int, note: str = "") -> dict:
    """섹션 제목 = text 위젯 (쿼리 없음). 폭 6, 높이 1."""
    lines = [f"### {title}"] + ([note] if note else [])
    return {"kind": "text", "title": title, "lines": lines, "x": 0, "y": y, "width": 6, "height": 1}


# 위젯: dataset, kind, title, 보여줄 열(counter는 value 하나, bar·line은 x_field와 y_fields, table은 columns), 위치.
# 선택 항목: note(제목 아래 작은 설명), color_field(계열 열, 채널이면 고정 색), horizontal(가로 막대). text는 lines만 갖는다.
PAGES = [
    {"name": "sales", "title": "판매", "widgets": [*_selectors(), *_below_selectors([
        _section("핵심 지표", 0, NOTE_COMPARE),
        _card("sales_kpi", "total_sales", 0, 1, 2, 3, "조회 기간 합계"),
        _card("sales_kpi", "net_sales", 2, 1, 2, 3, "총판매금액 − 환불금액"),
        _card("sales_kpi", "refund_ratio", 4, 1, 2, 3, NOTE_LOWER_BETTER),
        _card("sales_kpi", "paying_customers", 0, 4, 1, 2),
        _card("sales_kpi", "average_order_value", 1, 4, 1, 2),
        _card("sales_kpi", "item_count", 2, 4, 1, 2),
        _card("sales_kpi", "unit_price_per_item", 3, 4, 1, 2),
        _card("sales_kpi", "refund_amount", 4, 4, 1, 2),
        _card("refund_kpi", "refund_quantity", 5, 4, 1, 2),   # sales_metrics에 환불수량이 없다. 같은 필터·원결제일 귀속
        {"dataset": "sales_compare", "kind": "table", "title": "조회 기간 · 비교 기간 대비",
         "note": "증감은 기호와 숫자만 씁니다. 비교 기간에 결제가 없으면 \"실적이 없습니다.\"",
         "columns": COMPARE_COLUMNS, "x": 0, "y": 6, "width": 6, "height": 6},
        _section("추이", 12),
        {"dataset": "sales_trend_by_channel_week", "kind": "line", "title": "채널별 총판매금액 추이 · 주별",
         "note": "첫 주와 마지막 주는 부분 기간일 수 있습니다.",
         "x_field": "payment_week", "y_fields": ["total_sales"], "color_field": "channel_name",
         "x": 0, "y": 13, "width": 6, "height": 6},
        {"dataset": "channel_net_share", "kind": "bar", "title": "채널별 순판매금액 비중 · 큰 순", "x_field": "channel_name",
         "y_fields": ["net_sales_share"], "horizontal": True, "color_field": "channel_name",
         "x": 0, "y": 19, "width": 3, "height": 6},
        {"dataset": "refund_ratio_by_week", "kind": "line", "title": "환불금액비율 추이 · 주별, 채널 합계",
         "note": "주마다 그 주의 분자·분모로 다시 계산한 값입니다.", "x_field": "payment_week",
         "y_fields": ["refund_ratio"], "x": 3, "y": 19, "width": 3, "height": 6},
        _section("구성", 25),
        {"dataset": "sales_by_method", "kind": "bar", "title": "결제수단별 총판매금액 · 큰 순", "x_field": "method_name",
         "y_fields": ["total_sales_share"], "horizontal": True, "x": 0, "y": 26, "width": 3, "height": 6},
        {"dataset": "sales_by_category", "kind": "bar", "title": "카테고리별 총판매금액 · 큰 순", "x_field": "category",
         "y_fields": ["total_sales_share"], "horizontal": True, "x": 3, "y": 26, "width": 3, "height": 6},
        _section("상세", 32),
        {"dataset": "sales_by_channel", "kind": "table", "title": "채널별 지표",
         "columns": ["channel_name", "total_sales", "net_sales", "refund_ratio", "paying_customers",
                     "average_order_value"],
         "x": 0, "y": 33, "width": 6, "height": 5},
    ])]},
    {"name": "refund", "title": "환불", "widgets": [*_selectors(), *_below_selectors([
        _section("핵심 지표", 0, NOTE_COMPARE),
        _card("refund_kpi", "refund_amount", 0, 1, 2, 3, "원결제일이 조회 기간에 든 환불"),
        _card("refund_kpi", "refund_quantity", 2, 1, 2, 3, "환불된 상품 수량 합계"),
        _card("sales_kpi", "refund_ratio", 4, 1, 2, 3, NOTE_LOWER_BETTER),
        {"dataset": "refund_compare", "kind": "table", "title": "조회 기간 · 비교 기간 대비",
         "columns": COMPARE_COLUMNS, "x": 0, "y": 4, "width": 6, "height": 3},
        _section("구성", 7),
        {"dataset": "refund_by_reason", "kind": "bar", "title": "사유별 환불금액 비중 · 큰 순", "x_field": "refund_reason",
         "note": "금액·수량은 아래 표에서 봅니다.", "y_fields": ["refund_amount_share"], "horizontal": True,
         "x": 0, "y": 8, "width": 3, "height": 6},
        {"dataset": "refund_ratio_by_channel", "kind": "bar", "title": "채널별 환불금액비율 · 큰 순",
         "x_field": "channel_name", "note": "비율로 비교해야 채널 규모 차이가 가려지지 않습니다.",
         "y_fields": ["refund_ratio"], "horizontal": True, "color_field": "channel_name",
         "x": 3, "y": 8, "width": 3, "height": 6},
        _section("추이", 14),
        {"dataset": "refund_trend_by_week", "kind": "line", "title": "결제일별 환불금액 추이 · 원결제일 귀속, 주별",
         "note": "최근 주는 아직 환불이 접수 중일 수 있어 낮게 보일 수 있습니다.", "x_field": "payment_week",
         "y_fields": ["refund_amount"], "x": 0, "y": 15, "width": 6, "height": 6},
        _section("상세", 21),
        {"dataset": "refund_by_reason", "kind": "table", "title": "사유별 환불",
         "columns": ["refund_reason", "refund_amount", "refund_quantity", "refund_amount_share"],
         "x": 0, "y": 22, "width": 6, "height": 4},
    ])]},
]

# ── 쿼리 만들기 ──────────────────────────────────────────────────────────────────
MIN, MAX = ":date_range.min", ":date_range.max"
# 기간 이름 -> (시작, 끝). 조회 기간과 사용자가 고른 비교 기간.
PERIODS = {
    "cur": (MIN, MAX),
    "cmp": (":compare_range.min", ":compare_range.max"),
}


def dataset_columns(dataset: dict) -> list:
    """데이터셋 쿼리가 돌려주는 열 이름 순서."""
    if dataset.get("compare"):
        return ["sort_order", *COMPARE_COLUMNS]
    share = [f"{dataset['share_of']}_share"] if dataset.get("share_of") else []
    return [*dataset["dims"], *dataset["measures"], *share]


def where_clause(period: str = "cur") -> str:
    """선택을 거는 WHERE 조건 (매개변수 표기). period가 cmp면 기간만 비교 기간으로 바뀌고 채널·카테고리는 같다."""
    low, high = PERIODS[period]
    # 다중 선택 매개변수는 배열로 전달된다. `IN (:channels)`는 쓸 수 없고 array_contains(배열, 열)로 거른다 (Databricks 문서).
    return (f"payment_date BETWEEN {low} AND {high} AND array_contains(:channels, channel_name) "
            f"AND array_contains(:categories, category)")


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


def delta_text(current: str, compare: str, compare_items: str, points: bool = False) -> str:
    """증감 문장. 비교 기간에 결제 건수가 없으면 "실적이 없습니다.", 비교할 수 없으면 "-", 아니면 "▲ 12.3%" 같은 방향 기호와 숫자.
    points면 비율의 차를 %p로 보여 준다(환불금액비율; 비교값이 0이어도 계산된다). 상태 색(좋음/나쁨)은 쓰지 않는다.
    정수끼리 나누는 오차를 피하려 100.0을 먼저 곱한다."""
    arrow = f"(CASE WHEN {current} > {compare} THEN '▲ ' WHEN {current} < {compare} THEN '▼ ' ELSE '' END)"
    if points:
        body = f"format_number(ABS({current} - {compare}) * 100.0, 1) || '%p'"
        unusable = f"{current} IS NULL OR {compare} IS NULL"
    else:
        body = f"format_number(ABS({current} - {compare}) * 100.0 / ABS({compare}), 1) || '%'"
        unusable = f"{current} IS NULL OR {compare} IS NULL OR {compare} = 0"
    return (f"CASE WHEN {compare_items} IS NULL OR {compare_items} = 0 THEN '실적이 없습니다.' "
            f"WHEN {unusable} THEN '-' ELSE {arrow} || {body} END")


def value_text(column: str, unit: str) -> str:
    """값을 표에 보일 문장으로: 천 단위 쉼표 + 단위. 비율(%)은 퍼센트로 소수 한 자리. 값이 없으면 "-"."""
    if unit == "%":
        return f"COALESCE(format_number({column} * 100.0, 1) || '%', '-')"
    return f"COALESCE(format_number({column}, 0) || '{unit}', '-')"


def compare_query(dataset: dict, select) -> str:
    """비교 표 쿼리: 기간(조회·비교)과 뷰마다 지표를 조회하는 CTE를 만들고, 지표마다 한 행으로 펼친다.

    select(view, measures, where)는 지표 조회 문장을 돌려주는 함수다. Databricks는 MEASURE() 조회를, 로컬 테스트는 같은 정의의 일반 SQL을 넘긴다.
    상품결제건수(sales_metrics)는 비교 기간의 실적 유무 판단에 항상 함께 조회한다."""
    groups = {}
    for measure, shape in dataset["compare"].items():
        groups.setdefault(shape["view"], []).append(measure)
    sales = groups.setdefault("sales_metrics", [])
    if "item_count" not in sales:
        sales.append("item_count")
    ctes = [f"{p}_{view} AS ({select(view, ms, where_clause(p))})" for p in PERIODS for view, ms in groups.items()]
    tables = " CROSS JOIN ".join(f"{p}_{view}" for p in PERIODS for view in groups)
    rows = []
    for order, (m, shape) in enumerate(dataset["compare"].items(), start=1):
        cur = f"cur_{shape['view']}.{m}"

        cmp = f"cmp_{shape['view']}.{m}"
        change = delta_text(cur, cmp, "cmp_sales_metrics.item_count", shape["points"])
        rows.append(f"SELECT {order} AS sort_order, '{LABELS[m]}' AS metric, {value_text(cur, shape['unit'])} AS current_value, "
                    f"{value_text(cmp, shape['unit'])} AS compare_value, {change} AS change FROM {tables}")
    return f"WITH {', '.join(ctes)} " + " UNION ALL ".join(rows) + " ORDER BY sort_order"


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
    if dataset.get("compare"):
        return compare_query(dataset, select)
    return wrap_query(dataset, select(dataset["view"], dataset["measures"], where_clause(), dataset["dims"]))


def uses_parameter(dataset: dict, param: str) -> bool:
    """데이터셋 쿼리가 매개변수를 쓰는지 (쓰지 않는 매개변수는 선언하지 않는다)."""
    return f":{param}" in dataset_query(dataset)
