"""Metric View 정의를 고정 입력의 Gold 위에서 일반 SQL로 실행해 확인한다.

[검증 범위] 지표 식(합계·건수·distinct·비율)이 기대값과 같은 값을 내는지, 비가산 지표가 조회 범위에서 다시 계산되는지,
YAML 문장이 정의와 같은지, metric_check가 그 값을 올바르게 읽고 비교하는지(가짜 spark).
[검증하지 못하는 것] Databricks가 YAML을 받아들이는지, MEASURE() 조회와 `source.` 접두어가 의도대로 풀리는지. 로컬 통과 != Databricks 성공.
"""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _gold_sqlite as g  # noqa: E402  (경로 설정과 sys.path 추가를 맡는다)
import _silver_rows as silver  # noqa: E402
import create_metric_views  # noqa: E402
import metric_check  # noqa: E402
import metric_defs  # noqa: E402

try:
    import yaml
except ImportError:   # PyYAML이 없으면 YAML 왕복 테스트만 건너뛴다
    yaml = None

SALES, REFUND = metric_defs.SALES_METRICS, metric_defs.REFUND_METRICS
ALL_SALES = [name for name, _ in SALES["measures"]]


def query(con, definition, dims, measures):
    return silver.run(con, g.plain_sql(definition, dims, measures))


class TestFixtureMetrics(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con = g.gold_connection(silver.fixture_rows())

    def test_refund_view_matches_expected_and_sales_view(self):
        total = query(self.con, REFUND, [], ["refund_amount", "refund_quantity"])[0]
        self.assertEqual((105000, 3), (total["refund_amount"], total["refund_quantity"]))
        self.assertEqual(query(self.con, SALES, [], ["refund_amount"])[0]["refund_amount"], total["refund_amount"])
        reasons = query(self.con, REFUND, ["refund_type", "refund_reason"], ["refund_amount"])
        self.assertEqual([("RETURN", "DEFECT", 105000)],
                         [(r["refund_type"], r["refund_reason"], r["refund_amount"]) for r in reasons])

    def test_additive_measures_add_up_over_any_dimension(self):
        """어느 기준으로 나눠도 합이 전체 값과 같다 (전체 값은 기준 없이 따로 조회한다)."""
        total = query(self.con, SALES, [], ["total_sales", "refund_amount", "item_count"])[0]
        for dim in ("payment_date", "channel_name", "method_name", "category", "product_id", "payment_hour"):
            rows = query(self.con, SALES, [dim], ["total_sales", "refund_amount", "item_count"])
            with self.subTest(dimension=dim):
                self.assertEqual((total["total_sales"], total["refund_amount"], total["item_count"]),
                                 (sum(r["total_sales"] for r in rows), sum(r["refund_amount"] for r in rows),
                                  sum(r["item_count"] for r in rows)))
                self.assertGreater(total["total_sales"], 0)

    def test_non_additive_customers_are_recounted_not_summed(self):
        """상품 단위로 나누면 한 고객이 여러 상품에 걸쳐 중복되므로 합이 전체 결제고객수(18)와 달라진다."""
        total = query(self.con, SALES, [], ["paying_customers"])[0]["paying_customers"]
        by_product = query(self.con, SALES, ["product_id"], ["paying_customers"])
        self.assertEqual(18, total)
        self.assertGreater(sum(r["paying_customers"] for r in by_product), total)

    def test_no_date_dimension_is_joined(self):
        """일자 차원은 두지 않는다 (event_spec.md 6.2절). 일자 기준은 Fact의 DATE 컬럼 payment_date·refund_date다."""
        for definition in metric_defs.DEFINITIONS.values():
            with self.subTest(view=definition["name"]):
                self.assertNotIn("dim_date", [name for name, _ in definition["joins"]])
                self.assertFalse({"month", "week_start", "weekday", "holiday"} & {n for n, _ in definition["dimensions"]})


class TestEdgeCases(unittest.TestCase):
    def test_empty_gold_gives_null_ratios_not_errors(self):
        con = g.gold_connection(silver.rows_of())
        row = query(con, SALES, [], ALL_SALES)[0]
        self.assertEqual((None, 0, 0), (row["total_sales"], row["item_count"], row["paying_customers"]))
        for ratio in ("refund_ratio", "unit_price_per_item", "average_order_value"):
            self.assertIsNone(row[ratio], ratio)

    def test_anonymous_user_is_not_counted_as_customer(self):
        rows = silver.rows_of(orders=[silver.order("O1", user=None)], order_items=[silver.item("O1")],
                              payments=[silver.payment("O1", user=None)])
        row = query(g.gold_connection(rows), SALES, [], ALL_SALES)[0]
        self.assertEqual((2000, 1, 0), (row["total_sales"], row["item_count"], row["paying_customers"]))
        self.assertIsNone(row["average_order_value"])

    def test_unit_price_keeps_decimals(self):
        """나누어떨어지지 않는 단가(3,001원 / 3건)가 정수로 잘리지 않는다 (고정 입력은 42,200원으로 딱 떨어져 놓치기 쉽다)."""
        rows = silver.rows_of(orders=[silver.order("O1")],
                              order_items=[silver.item("O1", no=1, quantity=1, price=1000),
                                           silver.item("O1", no=2, quantity=1, price=1000),
                                           silver.item("O1", no=3, quantity=1, price=1001)],
                              payments=[silver.payment("O1", amount=3001)])
        row = query(g.gold_connection(rows), SALES, [], ["unit_price_per_item"])[0]
        self.assertAlmostEqual(3001 / 3, row["unit_price_per_item"], places=9)

    def test_same_user_id_in_two_channels_is_two_customers(self):
        rows = silver.rows_of(
            orders=[silver.order("O1", user="U1", channel="A"), silver.order("O2", user="U1", channel="B")],
            order_items=[silver.item("O1", channel="A"), silver.item("O2", channel="B")],
            payments=[silver.payment("O1", user="U1", channel="A"), silver.payment("O2", user="U1", channel="B")])
        row = query(g.gold_connection(rows), SALES, [], ["paying_customers"])[0]
        self.assertEqual(2, row["paying_customers"])


class TestYaml(unittest.TestCase):
    def test_create_sql_wraps_yaml_for_metric_view(self):
        sql = metric_defs.create_view_sql(SALES, "commerce_poc.gold", "commerce_poc.serving")
        self.assertTrue(sql.startswith("CREATE OR REPLACE VIEW commerce_poc.serving.sales_metrics WITH METRICS LANGUAGE YAML AS $$"))
        self.assertTrue(sql.endswith("\n$$"))
        self.assertIn("source: commerce_poc.gold.fact_sales_item", sql)
        self.assertEqual(1, sql.count("$$\nversion"))

    @unittest.skipIf(yaml is None, "PyYAML 없음")
    def test_yaml_round_trips_to_definition(self):
        for definition in metric_defs.DEFINITIONS.values():
            parsed = yaml.safe_load(metric_defs.render_yaml(definition, "commerce_poc.gold"))
            with self.subTest(view=definition["name"]):
                self.assertEqual("commerce_poc.gold." + definition["source"], parsed["source"])
                # PyYAML(YAML 1.1)은 키 `on`을 True로 읽는다. Metric View 문서도 `on:`을 쓰므로 정의는 그대로 두고 테스트만 맞춘다.
                self.assertEqual([(j["name"], j.get("on", j.get(True))) for j in parsed["joins"]],
                                 list(definition["joins"]))
                self.assertEqual([(d["name"], d["expr"]) for d in parsed["dimensions"]], list(definition["dimensions"]))
                self.assertEqual([(m["name"], m["expr"]) for m in parsed["measures"]], list(definition["measures"]))
                self.assertTrue(all(j["source"] == "commerce_poc.gold." + j["name"] for j in parsed["joins"]))

    def test_names_are_unique_and_joins_are_gold_dimensions(self):
        for definition in metric_defs.DEFINITIONS.values():
            for key in ("dimensions", "measures", "joins"):
                names = [name for name, _ in definition[key]]
                self.assertEqual(len(names), len(set(names)), f"{definition['name']} {key}")
            self.assertTrue(all(name.startswith("dim_") for name, _ in definition["joins"]))

    def test_every_measure_in_spec_6_5_exists(self):
        self.assertEqual({"total_sales", "refund_amount", "net_sales", "refund_ratio", "item_count",
                          "unit_price_per_item", "paying_customers", "average_order_value"}, set(ALL_SALES))
        self.assertEqual({"refund_amount", "refund_quantity"}, {n for n, _ in REFUND["measures"]})


class FakeCreateSpark:
    def __init__(self):
        self.statements = []

    def sql(self, text):
        self.statements.append(text)


class TestCreate(unittest.TestCase):
    def test_creates_schema_then_each_view(self):
        spark = FakeCreateSpark()
        names = create_metric_views.create_metric_views(spark, "c.gold", "c.serving")
        self.assertEqual(["c.serving.sales_metrics", "c.serving.refund_metrics"], names)
        self.assertEqual("CREATE SCHEMA IF NOT EXISTS c.serving", spark.statements[0])
        self.assertEqual(3, len(spark.statements))
        self.assertTrue(all("WITH METRICS" in s for s in spark.statements[1:]))


class TestMetricCheck(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con = g.gold_connection(silver.fixture_rows())

    def test_fixture_gold_passes_check(self):
        spark = g.FakeSpark(self.con)
        self.assertEqual([], metric_check.check_metric_views(spark, "c.serving"))
        self.assertTrue(all("c.serving." in q for q in spark.queries))

    def test_refund_view_and_sales_view_disagreement_is_reported(self):
        con = g.gold_connection(silver.fixture_rows())
        con.execute("UPDATE gold.fact_refund SET refund_amount = refund_amount + 1 WHERE rowid = 1")
        problems = metric_check.check_metric_views(g.FakeSpark(con), "c.serving")
        self.assertTrue(any("sales_metrics vs refund_metrics" in p for p in problems), problems)

    def test_measure_sql_format(self):
        self.assertEqual("SELECT MEASURE(a) AS x FROM v", metric_check.measure_sql("v", {"x": "a"}))
        self.assertEqual("SELECT c, MEASURE(a) AS x FROM v GROUP BY c", metric_check.measure_sql("v", {"x": "a"}, "c"))



class TestPaymentHour(unittest.TestCase):
    """결제 시간대(payment_hour): 결제 시각을 KST로 바꾼 시 0~23 (event_spec.md 6.5절). sales_metrics에만 있다."""

    @classmethod
    def setUpClass(cls):
        cls.con = g.gold_connection(silver.fixture_rows())

    def test_only_sales_metrics_has_the_dimension(self):
        self.assertIn("payment_hour", [name for name, _ in metric_defs.SALES_METRICS["dimensions"]])
        self.assertNotIn("payment_hour", [name for name, _ in metric_defs.REFUND_METRICS["dimensions"]])

    def test_expression_converts_utc_to_kst_hour(self):
        self.assertEqual("hour(from_utc_timestamp(payment_at, 'Asia/Seoul'))", dict(metric_defs.SALES_METRICS["dimensions"])["payment_hour"])

    def test_hours_are_kst_not_utc(self):
        by_hour = {r["payment_hour"]: r["total_sales"] for r in query(self.con, SALES, ["payment_hour"], ["total_sales"])}
        self.assertIn(0, by_hour)         # S23: 결제 UTC 15:30 = KST 00:30
        self.assertIn(10, by_hour)        # S01: 결제 UTC 01:00:30 = KST 10:00
        self.assertTrue(set(by_hour) <= set(range(24)))

    def test_hourly_sales_add_up_to_the_total(self):
        by_hour = query(self.con, SALES, ["payment_hour"], ["total_sales"])
        total = query(self.con, SALES, [], ["total_sales"])[0]["total_sales"]
        self.assertEqual(total, sum(r["total_sales"] for r in by_hour))


if __name__ == "__main__":
    unittest.main()
