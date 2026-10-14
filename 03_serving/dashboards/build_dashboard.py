"""dashboard_spec의 정의를 Lakeview 대시보드 JSON(.lvdash.json)으로 만든다. (plans/05 카드 5-G)

  build_dashboard()   정의 -> 대시보드 dict (datasets, pages). 판매·환불 페이지와 전역 필터 페이지(채널·카테고리·조회 기간)를 담는다
  write_dashboard()   dict -> 파일. 기본 위치는 이 폴더의 commerce_poc.lvdash.json
실행: python 03_serving/dashboards/build_dashboard.py  (파일을 다시 만든다)
[미검증] 선 차트(line)·가로 막대·채널 색 고정(mappings)·정렬(sort) spec은 Databricks에서 열어 확인하지 못했다. 필터 버전을 열었을 때 끊긴다는
이전 사용자 보고가 있어, 이 파일이 열리지 않으면 dashboard_spec.py의 데이터셋 쿼리와 화면 구성 가이드(화면_구성_가이드.md)로 위젯을 직접 만든다.
쿼리와 열 이름의 일치, 위젯 위치 겹침 여부는 로컬 테스트로 확인한다.
"""
import hashlib
import json
from pathlib import Path

from dashboard_spec import (CATEGORIES, CHANNEL_COLORS, CHANNEL_NAMES, DATASETS, DEFAULT_DATE_RANGE, FILTERS, LABELS,
                            OPTION_DATASETS, PAGES, dataset_query, uses_parameter)

DEFAULT_PATH = Path(__file__).resolve().parent / "commerce_poc.lvdash.json"
SPEC_VERSION = {"counter": 2, "table": 2, "bar": 3, "line": 3}   # 위젯 종류별 spec 버전


def short_id(text: str) -> str:
    """같은 이름이면 항상 같은 8자 ID (다시 만들어도 파일이 바뀌지 않게 한다)."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:8]


def label(column: str) -> str:
    return LABELS.get(column, column)


def dataset_id(name: str) -> str:
    return short_id(f"dataset-{name}")


def parameters(used: set) -> list:
    """데이터셋 매개변수 정의 중 쿼리가 쓰는 것만: 기간(날짜 범위), 채널·카테고리(다중 선택). 기본값은 최근 30일, 전체 선택."""
    low, high = DEFAULT_DATE_RANGE
    def multi(name, values):
        return {"displayName": name, "keyword": name, "dataType": "STRING", "complexType": "MULTI",
                "defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": v} for v in values]}}}
    every = [
        {"displayName": "date_range", "keyword": "date_range", "dataType": "DATE", "complexType": "RANGE",
         "defaultSelection": {"range": {"dataType": "DATE", "min": {"value": low}, "max": {"value": high}}}},
        multi("channels", CHANNEL_NAMES), multi("categories", CATEGORIES)]
    return [p for p in every if p["keyword"] in used]


def dataset_json(dataset: dict) -> dict:
    used = {f["param"] for f in FILTERS if uses_parameter(dataset, f["param"])}
    return {"name": dataset_id(dataset["name"]), "displayName": dataset["name"],
            "queryLines": [dataset_query(dataset)], "parameters": parameters(used)}


def option_dataset_json(option: dict) -> dict:
    return {"name": dataset_id(option["name"]), "displayName": option["name"], "queryLines": [option["query"]]}


def filter_widget(index: int, flt: dict) -> dict:
    """전역 필터 위젯 하나. 모든 지표 데이터셋의 같은 매개변수에 묶는다."""
    param = flt["param"]
    queries = [{"name": f"param_{short_id(d['name'])}_{param}",
                "query": {"datasetName": dataset_id(d["name"]), "parameters": [{"name": param, "keyword": param}],
                          "disaggregated": False}} for d in DATASETS if uses_parameter(d, param)]
    fields = [{"parameterName": param, "queryName": q["name"]} for q in queries]
    if flt.get("options"):
        options = f"options_{flt['options']}"
        column = flt["dimension"]
        queries.insert(0, {"name": options, "query": {"datasetName": dataset_id(flt["options"]), "fields": [field(column)],
                                                       "disaggregated": False}})
        fields.insert(0, {"fieldName": column, "displayName": label(column), "queryName": options})
    kind = {"date_range": "filter-date-range-picker", "multi_select": "filter-multi-select"}[flt["kind"]]
    return {"widget": {"name": short_id(f"filter-{param}"), "queries": queries,
                       "spec": {"version": 2, "widgetType": kind, "encodings": {"fields": fields},
                                "frame": {"showTitle": True, "title": flt["title"]}}},
            "position": {"x": 2 * index, "y": 0, "width": 2, "height": 2}}


def filter_page() -> dict:
    """모든 페이지 맨 위에 보이는 전역 필터 페이지."""
    return {"name": short_id("page-filters"), "displayName": "필터", "pageType": "PAGE_TYPE_GLOBAL_FILTERS",
            "layout": [filter_widget(i, f) for i, f in enumerate(FILTERS)]}


def field(name: str, aggregate: str = "") -> dict:
    """쿼리 필드. aggregate가 있으면 그 함수로 감싼 이름(예: sum(total_sales))을 쓴다."""
    if aggregate:
        return {"name": f"{aggregate}({name})", "expression": f"{aggregate.upper()}(`{name}`)"}
    return {"name": name, "expression": f"`{name}`"}


def widget_spec(widget: dict) -> tuple:
    """(쿼리 필드 목록, disaggregated 여부, spec dict)."""
    frame = {"showTitle": bool(widget["title"]), "title": widget["title"]}
    if widget.get("note"):   # 제목 아래 작은 설명 (와이어프레임의 카드·차트 아래 글)
        frame.update({"showDescription": True, "description": widget["note"]})
    kind = widget["kind"]
    if kind == "counter":
        encodings = {"value": {"fieldName": widget["value"], "displayName": label(widget["value"])}}
        return [field(widget["value"])], True, _spec(kind, encodings, frame)
    if kind == "table":
        columns = [{"fieldName": c, "displayName": label(c)} for c in widget["columns"]]
        return [field(c) for c in widget["columns"]], True, _spec(kind, {"columns": columns}, frame)
    # bar·line: 이미 기준별로 집계된 행이라 SUM은 값을 그대로 돌려준다 (기준 행마다 값이 하나일 때만 비가산 지표를 그려도 된다).
    x = widget["x_field"]
    ys = widget["y_fields"]
    y_fields = [{"fieldName": f"sum({y})", "displayName": label(y)} for y in ys]
    value_axis = ({"fieldName": y_fields[0]["fieldName"], "displayName": y_fields[0]["displayName"]} if len(ys) == 1
                  else {"fields": y_fields})
    value_axis["scale"] = {"type": "quantitative"}
    if widget.get("horizontal"):   # 가로 막대: 값이 가로축, 기준이 세로축. 큰 순서로 위에서부터 보이게 정렬한다.
        encodings = {"x": value_axis,
                     "y": {"fieldName": x, "displayName": label(x), "scale": {"type": "categorical", "sort": {"by": "x-reversed"}}}}
    else:
        x_type = "temporal" if kind == "line" else "categorical"
        encodings = {"x": {"fieldName": x, "displayName": label(x), "scale": {"type": x_type}}, "y": value_axis}
    queried = [field(x), *(field(y, "sum") for y in ys)]
    color = widget.get("color_field")
    if color:   # 채널 계열: 색을 채널마다 고정한다
        encodings["color"] = {"fieldName": color, "displayName": label(color),
                              "scale": {"type": "categorical",
                                        "mappings": [{"value": n, "color": c} for n, c in CHANNEL_COLORS.items()]}}
        if color != x:
            queried.append(field(color))
    return queried, False, _spec(kind, encodings, frame)


def _spec(kind: str, encodings: dict, frame: dict) -> dict:
    return {"version": SPEC_VERSION[kind], "widgetType": kind, "encodings": encodings, "frame": frame}


def layout_item(page: str, index: int, widget: dict) -> dict:
    fields, disaggregated, spec = widget_spec(widget)
    return {
        "widget": {
            "name": short_id(f"widget-{page}-{index}"),
            "queries": [{"name": "main_query",
                         "query": {"datasetName": dataset_id(widget["dataset"]), "fields": fields,
                                   "disaggregated": disaggregated}}],
            "spec": spec,
        },
        "position": {key: widget[key] for key in ("x", "y", "width", "height")},
    }


def build_dashboard() -> dict:
    """지표 데이터셋, 필터 선택지 데이터셋, 판매·환불 페이지, 전역 필터 페이지를 담은 대시보드 dict."""
    pages = [{"name": short_id(f"page-{page['name']}"), "displayName": page["title"], "pageType": "PAGE_TYPE_CANVAS",
              "layout": [layout_item(page["name"], i, w) for i, w in enumerate(page["widgets"])]} for page in PAGES]
    datasets = [dataset_json(d) for d in DATASETS] + [option_dataset_json(o) for o in OPTION_DATASETS]
    return {"datasets": datasets, "pages": [*pages, filter_page()]}


def write_dashboard(path=DEFAULT_PATH) -> Path:
    """JSON을 UTF-8, CRLF(저장소 규칙)로 쓴다."""
    text = json.dumps(build_dashboard(), ensure_ascii=False, indent=2).replace("\n", "\r\n")
    Path(path).write_bytes(text.encode("utf-8"))
    return Path(path)


if __name__ == "__main__":
    print(write_dashboard())
