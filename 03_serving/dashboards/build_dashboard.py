"""dashboard_spec의 정의를 Lakeview 대시보드 JSON(.lvdash.json)으로 만든다.

  build_dashboard(filters)   정의 -> 대시보드 dict (datasets, pages). filters=False는 필터 없는 첫 대시보드, True는 기간·채널 필터 버전
  write_dashboard()          dict -> 파일. 기본 위치는 이 폴더의 commerce_poc.lvdash.json (필터 없는 버전)
실행: python 03_serving/dashboards/build_dashboard.py  (파일을 다시 만든다)
Databricks에서 이 파일을 Git folder로 동기화하면 대시보드로 열린다. 필터 없는 버전의 위젯 모양은 Databricks에서 열려 확인했다(사용자 보고, 이 파일의 데이터셋·페이지는 그때 내려받은 파일과 같다). 필터 버전은 열었을 때 끊긴다는 사용자 보고가 있어 저장소 파일로 쓰지 않는다. 쿼리와 열 이름의 일치, 위젯 위치 겹침 여부는 로컬 테스트로 확인한다.
"""
import hashlib
import json
from pathlib import Path

from dashboard_spec import (CHANNEL_NAMES, DATASETS, DEFAULT_DATE_RANGE, FILTERS, LABELS, OPTION_DATASETS, PAGES,
                            dataset_columns, dataset_query)

DEFAULT_PATH = Path(__file__).resolve().parent / "commerce_poc.lvdash.json"
SPEC_VERSION = {"counter": 2, "table": 2, "bar": 3}   # 위젯 종류별 spec 버전


def short_id(text: str) -> str:
    """같은 이름이면 항상 같은 8자 ID (다시 만들어도 파일이 바뀌지 않게 한다)."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:8]


def label(column: str) -> str:
    return LABELS.get(column, column)


def dataset_id(name: str) -> str:
    return short_id(f"dataset-{name}")


def parameters() -> list:
    """데이터셋 매개변수 정의: 기간(날짜 범위)과 채널(다중 선택). 기본값은 1년 전~오늘, 채널 전체."""
    low, high = DEFAULT_DATE_RANGE
    return [
        {"displayName": "date_range", "keyword": "date_range", "dataType": "DATE", "complexType": "RANGE",
         "defaultSelection": {"range": {"dataType": "DATE", "min": {"value": low}, "max": {"value": high}}}},
        {"displayName": "channels", "keyword": "channels", "dataType": "STRING", "complexType": "MULTI",
         "defaultSelection": {"values": {"dataType": "STRING", "values": [{"value": n} for n in CHANNEL_NAMES]}}},
    ]


def dataset_json(dataset: dict, filters: bool = True) -> dict:
    result = {"name": dataset_id(dataset["name"]), "displayName": dataset["name"],
              "queryLines": [dataset_query(dataset, filters=filters)]}
    if filters:
        result["parameters"] = parameters()
    return result


def option_dataset_json(option: dict) -> dict:
    return {"name": dataset_id(option["name"]), "displayName": option["name"], "queryLines": [option["query"]]}


def filter_widget(index: int, flt: dict) -> dict:
    """전역 필터 위젯 하나. 모든 지표 데이터셋의 같은 매개변수에 묶는다."""
    param = flt["param"]
    queries = [{"name": f"param_{short_id(d['name'])}_{param}",
                "query": {"datasetName": dataset_id(d["name"]), "parameters": [{"name": param, "keyword": param}],
                          "disaggregated": False}} for d in DATASETS]
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
            "position": {"x": 3 * index, "y": 0, "width": 3, "height": 2}}


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
    frame = {"showTitle": True, "title": widget["title"]}
    kind = widget["kind"]
    if kind == "counter":
        encodings = {"value": {"fieldName": widget["value"], "displayName": label(widget["value"])}}
        return [field(widget["value"])], True, _spec(kind, encodings, frame)
    if kind == "table":
        columns = [{"fieldName": c, "displayName": label(c)} for c in widget["columns"]]
        return [field(c) for c in widget["columns"]], True, _spec(kind, {"columns": columns}, frame)
    # bar: 이미 기준별로 집계된 행이라 SUM은 값을 그대로 돌려준다. 비가산 지표(비율 등)는 막대에 쓰지 않는다.
    x = widget["x_field"]
    ys = widget["y_fields"]
    encodings = {"x": {"fieldName": x, "scale": {"type": "categorical"}, "displayName": label(x)},
                 "y": {"scale": {"type": "quantitative"},
                       "fields": [{"fieldName": f"sum({y})", "displayName": label(y)} for y in ys]}}
    if len(ys) == 1:
        encodings["y"] = {"fieldName": f"sum({ys[0]})", "scale": {"type": "quantitative"}, "displayName": label(ys[0])}
    return [field(x), *(field(y, "sum") for y in ys)], False, _spec(kind, encodings, frame)


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


def build_dashboard(filters: bool = True) -> dict:
    """지표 데이터셋, 필터 선택지 데이터셋, 판매·환불 페이지, 전역 필터 페이지를 담은 대시보드 dict.

    filters=False면 필터 없는 첫 대시보드(데이터셋 9개, 페이지 2개)를 만든다."""
    pages = [{"name": short_id(f"page-{page['name']}"), "displayName": page["title"], "pageType": "PAGE_TYPE_CANVAS",
              "layout": [layout_item(page["name"], i, w) for i, w in enumerate(page["widgets"])]} for page in PAGES]
    if not filters:
        return {"datasets": [dataset_json(d, filters=False) for d in DATASETS], "pages": pages}
    datasets = [dataset_json(d) for d in DATASETS] + [option_dataset_json(o) for o in OPTION_DATASETS]
    return {"datasets": datasets, "pages": [*pages, filter_page()]}


def write_dashboard(path=DEFAULT_PATH, filters: bool = False) -> Path:
    """JSON을 UTF-8, CRLF(저장소 규칙)로 쓴다. 저장소의 파일은 필터 없는 첫 대시보드다 (필터 버전은 filters=True)."""
    text = json.dumps(build_dashboard(filters), ensure_ascii=False, indent=2).replace("\n", "\r\n")
    Path(path).write_bytes(text.encode("utf-8"))
    return Path(path)


if __name__ == "__main__":
    print(write_dashboard())
