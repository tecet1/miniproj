"""dashboard_spec의 정의를 Lakeview 대시보드 JSON(.lvdash.json)으로 만든다.

  build_dashboard()   정의 -> 대시보드 dict (datasets, pages)
  write_dashboard()   dict -> 파일. 기본 위치는 이 폴더의 commerce_poc.lvdash.json
실행: python 03_serving/dashboards/build_dashboard.py  (파일을 다시 만든다)
Databricks에서 이 파일을 Git folder로 동기화하면 대시보드로 열린다. JSON 모양(위젯 spec 버전 등)을 Databricks가 받아들이는지는
확인하지 않았다 (미검증). 쿼리와 열 이름의 일치, 위젯 위치 겹침 여부는 로컬 테스트로 확인한다.
"""
import hashlib
import json
from pathlib import Path

from dashboard_spec import DATASETS, LABELS, PAGES, dataset_columns, dataset_query

DEFAULT_PATH = Path(__file__).resolve().parent / "commerce_poc.lvdash.json"
SPEC_VERSION = {"counter": 2, "table": 2, "bar": 3}   # 위젯 종류별 spec 버전


def short_id(text: str) -> str:
    """같은 이름이면 항상 같은 8자 ID (다시 만들어도 파일이 바뀌지 않게 한다)."""
    return hashlib.md5(text.encode("utf-8")).hexdigest()[:8]


def label(column: str) -> str:
    return LABELS.get(column, column)


def dataset_json(dataset: dict) -> dict:
    return {"name": short_id(f"dataset-{dataset['name']}"), "displayName": dataset["name"],
            "queryLines": [dataset_query(dataset)]}


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
                         "query": {"datasetName": short_id(f"dataset-{widget['dataset']}"), "fields": fields,
                                   "disaggregated": disaggregated}}],
            "spec": spec,
        },
        "position": {key: widget[key] for key in ("x", "y", "width", "height")},
    }


def build_dashboard() -> dict:
    """사용하는 데이터셋만 담은 대시보드 dict."""
    pages = [{"name": short_id(f"page-{page['name']}"), "displayName": page["title"], "pageType": "PAGE_TYPE_CANVAS",
              "layout": [layout_item(page["name"], i, w) for i, w in enumerate(page["widgets"])]} for page in PAGES]
    return {"datasets": [dataset_json(d) for d in DATASETS], "pages": pages}


def write_dashboard(path=DEFAULT_PATH) -> Path:
    """JSON을 UTF-8, CRLF(저장소 규칙)로 쓴다."""
    text = json.dumps(build_dashboard(), ensure_ascii=False, indent=2).replace("\n", "\r\n")
    Path(path).write_bytes(text.encode("utf-8"))
    return Path(path)


if __name__ == "__main__":
    print(write_dashboard())
