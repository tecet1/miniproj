"""대시보드 정의와 생성된 Lakeview JSON을 검사한다.

[검증 범위] 데이터셋이 존재하는 지표·기준만 조회하는지, 같은 조회를 로컬(sqlite)에서 돌렸을 때 열과 값이 기대값과 맞는지,
위젯이 데이터셋의 열만 쓰는지, 위젯 위치가 격자 안에서 겹치지 않는지, 저장소의 JSON 파일이 정의와 같은지.
[검증하지 못하는 것] Databricks가 이 JSON(위젯 spec 버전·encodings)을 대시보드로 열고 MEASURE() 쿼리가 실행되는지. 로컬 통과 != Databricks 성공.
"""
import json
import re
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _gold_sqlite as g  # noqa: E402  (경로 설정과 sys.path 추가를 맡는다)
import _silver_rows as silver  # noqa: E402
import build_dashboard  # noqa: E402
import dashboard_spec as spec  # noqa: E402
import metric_defs  # noqa: E402

DATASETS = {d["name"]: d for d in spec.DATASETS}
DASHBOARD = build_dashboard.build_dashboard()
GRID = 6


def local_rows(con, dataset: dict) -> list:
    """데이터셋을 일반 SQL(정의의 식)로 실행한 결과. 구성비·정렬은 실제 쿼리와 같은 wrap_query를 쓴다."""
    inner = g.plain_sql(metric_defs.DEFINITIONS[dataset["view"]], dataset["dims"], dataset["measures"])
    return silver.run(con, spec.wrap_query(dataset, inner))


class TestSpec(unittest.TestCase):
    def test_datasets_use_only_defined_dimensions_and_measures(self):
        for dataset in spec.DATASETS:
            definition = metric_defs.DEFINITIONS[dataset["view"]]
            with self.subTest(dataset=dataset["name"]):
                self.assertTrue(set(dataset["dims"]) <= {n for n, _ in definition["dimensions"]})
                self.assertTrue(set(dataset["measures"]) <= {n for n, _ in definition["measures"]})

    def test_query_text(self):
        sql = spec.dataset_query(DATASETS["sales_by_channel"])
        self.assertTrue(sql.startswith("SELECT channel_name, MEASURE(total_sales) AS total_sales,"))
        self.assertTrue(sql.endswith("FROM commerce_poc.serving.sales_metrics GROUP BY ALL ORDER BY channel_name"))
        self.assertNotIn("GROUP BY", spec.dataset_query(DATASETS["sales_kpi"]))
        reason = spec.dataset_query(DATASETS["refund_by_reason"])
        self.assertIn("refund_amount_share", reason)
        self.assertIn("OVER ()", reason)

    def test_every_measure_in_a_query_is_wrapped_in_measure(self):
        for dataset in spec.DATASETS:
            sql = spec.dataset_query(dataset)
            for measure in dataset["measures"]:
                with self.subTest(dataset=dataset["name"], measure=measure):
                    self.assertIn(f"MEASURE({measure}) AS {measure}", sql)

    def test_query_columns_match_local_result_columns(self):
        con = g.gold_connection(silver.fixture_rows())
        for dataset in spec.DATASETS:
            with self.subTest(dataset=dataset["name"]):
                rows = local_rows(con, dataset)
                self.assertTrue(rows)
                self.assertEqual(spec.dataset_columns(dataset), list(rows[0]))


class TestValues(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con = g.gold_connection(silver.fixture_rows())

    def test_kpi_values_match_expected(self):
        sales = local_rows(self.con, DATASETS["sales_kpi"])[0]
        want = json.loads((silver.FIXTURES / "expected" / "expected.json").read_text(encoding="utf-8"))["total"]
        self.assertEqual((want["total_sales"], want["refund_amount"], want["net_sales"], want["paid_item_count"],
                          want["paying_customers"]), (sales["total_sales"], sales["refund_amount"], sales["net_sales"],
                                                      sales["item_count"], sales["paying_customers"]))
        refund = local_rows(self.con, DATASETS["refund_kpi"])[0]
        self.assertEqual((want["refund_amount"], want["refund_quantity"]), (refund["refund_amount"], refund["refund_quantity"]))

    def test_refund_share_sums_to_one(self):
        rows = local_rows(self.con, DATASETS["refund_by_reason"])
        self.assertAlmostEqual(1.0, sum(r["refund_amount_share"] for r in rows))
        self.assertEqual([("DEFECT", 105000)], [(r["refund_reason"], r["refund_amount"]) for r in rows])

    def test_refund_share_splits_between_reasons(self):
        rows = silver.rows_of(
            orders=[silver.order("O1")], order_items=[silver.item("O1", quantity=4, price=1000)],
            payments=[silver.payment("O1", amount=4000)],
            refunds=[silver.refund("O1", no=1, quantity=1, amount=1000, reason="DEFECT"),
                     silver.refund("O1", no=2, quantity=1, amount=3000, reason="CHANGE_OF_MIND")])
        con = g.gold_connection(rows)
        result = {r["refund_reason"]: r["refund_amount_share"] for r in local_rows(con, DATASETS["refund_by_reason"])}
        self.assertEqual({"DEFECT": 0.25, "CHANGE_OF_MIND": 0.75}, result)

    def test_sales_by_channel_has_three_rows_in_order(self):
        rows = local_rows(self.con, DATASETS["sales_by_channel"])
        self.assertEqual(3, len(rows))
        self.assertEqual([r["channel_name"] for r in rows], sorted(r["channel_name"] for r in rows))
        self.assertEqual(1266000, sum(r["total_sales"] for r in rows))


def widgets(dashboard: dict) -> list:
    return [(page["name"], item) for page in dashboard["pages"] for item in page["layout"]]


class TestJson(unittest.TestCase):
    def test_names_are_unique(self):
        dataset_names = [d["name"] for d in DASHBOARD["datasets"]]
        page_names = [p["name"] for p in DASHBOARD["pages"]]
        widget_names = [i["widget"]["name"] for _, i in widgets(DASHBOARD)]
        for names in (dataset_names, page_names, widget_names):
            self.assertEqual(len(names), len(set(names)))
            self.assertTrue(all(re.fullmatch(r"[0-9a-f]{8}", n) for n in names))

    def test_widget_queries_point_to_existing_dataset_columns(self):
        by_id = {d["name"]: d for d in DASHBOARD["datasets"]}
        columns = {build_dashboard.short_id(f"dataset-{name}"): spec.dataset_columns(d) for name, d in DATASETS.items()}
        self.assertEqual(set(by_id), set(columns))
        for page, item in widgets(DASHBOARD):
            query = item["widget"]["queries"][0]["query"]
            for field in query["fields"]:
                column = re.sub(r"^\w+\((\w+)\)$", r"\1", field["name"])
                with self.subTest(page=page, widget=item["widget"]["name"], field=field["name"]):
                    self.assertIn(column, columns[query["datasetName"]])

    def test_widget_encodings_use_fields_of_their_query(self):
        for page, item in widgets(DASHBOARD):
            widget = item["widget"]
            fields = {f["name"] for f in widget["queries"][0]["query"]["fields"]}
            encodings = widget["spec"]["encodings"]
            used = set()
            for key, value in encodings.items():
                if key == "columns":
                    used |= {c["fieldName"] for c in value}
                elif "fieldName" in value:
                    used.add(value["fieldName"])
                else:
                    used |= {f["fieldName"] for f in value["fields"]}
            with self.subTest(page=page, widget=widget["name"]):
                self.assertTrue(used and used <= fields, (used, fields))

    def test_bars_never_plot_non_additive_measures(self):
        non_additive = {"refund_ratio", "unit_price_per_item", "paying_customers", "average_order_value", "refund_amount_share"}
        for page in spec.PAGES:
            for widget in page["widgets"]:
                if widget["kind"] == "bar":
                    self.assertFalse(non_additive & set(widget["y_fields"]), widget["title"])

    def test_layout_fits_grid_without_overlap(self):
        for page in DASHBOARD["pages"]:
            boxes = [i["position"] for i in page["layout"]]
            for box in boxes:
                self.assertTrue(0 <= box["x"] and box["x"] + box["width"] <= GRID and box["width"] > 0 and box["height"] > 0)
            for i, a in enumerate(boxes):
                for b in boxes[i + 1:]:
                    apart = (a["x"] + a["width"] <= b["x"] or b["x"] + b["width"] <= a["x"]
                             or a["y"] + a["height"] <= b["y"] or b["y"] + b["height"] <= a["y"])
                    self.assertTrue(apart, (page["displayName"], a, b))

    def test_all_spec_6_5_sales_measures_are_shown_as_kpi(self):
        shown = {w["value"] for p in spec.PAGES for w in p["widgets"] if w["kind"] == "counter"}
        self.assertEqual({n for n, _ in metric_defs.SALES_METRICS["measures"]} | {"refund_quantity"}, shown)

    def test_checked_in_file_matches_definition(self):
        raw = build_dashboard.DEFAULT_PATH.read_bytes()
        self.assertFalse(raw.startswith(b"\xef\xbb\xbf"))
        self.assertEqual(DASHBOARD, json.loads(raw.decode("utf-8")))
        self.assertIn(b"\r\n", raw)


if __name__ == "__main__":
    unittest.main()
