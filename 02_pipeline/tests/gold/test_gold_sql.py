"""Gold Fact SQL을 로컬(sqlite)에서 실행해 규칙을 확인한다.

[검증 범위] SQL의 조인·집계·제외 규칙. 같은 문장을 Spark가 같은 결과로 실행하는지, KST 일자 변환(from_utc_timestamp)이
sqlite 식(+9시간)과 같은 날짜를 내는지는 Databricks에서 gold_check로 따로 확인한다 (로컬 테스트 통과 != Databricks 성공).
"""
import json
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "02_pipeline" / "gold"))

import _silver_rows as silver  # noqa: E402
import dimensions  # noqa: E402
import gold_check  # noqa: E402
import gold_sql  # noqa: E402
import gold_tables  # noqa: E402



def build_facts(rows: dict) -> tuple:
    """Silver 행에서 (fact_sales_item 행 목록, fact_refund 행 목록)을 sqlite로 계산한다."""
    con = silver.connect(rows)
    sales = silver.run(con, gold_sql.sales_item_sql("silver", silver.sqlite_date))
    refunds = silver.run(con, gold_sql.refund_sql("silver", silver.sqlite_date))
    con.close()
    return sales, refunds


def raw_totals(sales: list) -> dict:
    """채널별 합계 5개 (gold_check.RAW_KEYS). 고객은 channel+user_id 기준이라 채널별로 센다."""
    result = {}
    for channel in "ABC":
        mine = [r for r in sales if r["channel"] == channel]
        result[channel] = {"total_sales": sum(r["item_paid_amount"] for r in mine),
                           "refund_amount": sum(r["refund_amount"] for r in mine),
                           "refund_quantity": sum(r["refund_quantity"] for r in mine),
                           "paid_item_count": len(mine),
                           "paying_customers": len({r["user_id"] for r in mine if r["user_id"]})}
    return result


class TestFixtureGold(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows = silver.fixture_rows()
        cls.sales, cls.refunds = build_facts(cls.rows)

    def test_columns_match_gold_tables(self):
        self.assertEqual(gold_tables.column_names("fact_sales_item"), list(self.sales[0]))
        self.assertEqual(gold_tables.column_names("fact_refund"), list(self.refunds[0]))

    def test_primary_keys_are_unique(self):
        for name, rows in (("fact_sales_item", self.sales), ("fact_refund", self.refunds)):
            keys = [tuple(r[k] for k in gold_tables.TABLES[name]["pk"]) for r in rows]
            self.assertEqual(len(keys), len(set(keys)), name)

    def test_refund_totals_reconcile_between_facts(self):
        self.assertEqual(sum(r["refund_amount"] for r in self.refunds), sum(r["refund_amount"] for r in self.sales))
        self.assertEqual(sum(r["refund_quantity"] for r in self.refunds), sum(r["refund_quantity"] for r in self.sales))

    def test_success_payment_equals_items_plus_shipping_and_shipping_rule(self):
        shipping = {(o["channel"], o["order_id"]): o["shipping_fee"] for o in self.rows["orders"]}
        paid = {(p["channel"], p["order_id"]): p["payment_amount"] for p in self.rows["payments"]
                if p["status"] == "SUCCEEDED"}
        totals = {}
        for r in self.sales:
            key = (r["channel"], r["order_id"])
            totals[key] = totals.get(key, 0) + r["item_paid_amount"]
        for key, items_total in totals.items():
            with self.subTest(order=key):
                self.assertEqual(items_total + shipping[key], paid[key])
                self.assertEqual(0 if items_total >= 20000 else 4000, shipping[key])

    def test_dimension_keys_exist(self):
        """Fact가 참조하는 상품·결제수단·환불 프로필·채널이 Dimension에 있다."""
        products = {row[0] for row in dimensions.product_rows(silver.ROOT / "01_contracts" / "contracts" / "skus.json")}
        methods = {row[0] for row in dimensions.payment_method_rows()}
        profiles = {row[0] for row in dimensions.refund_profile_rows()}
        channels = {row[0] for row in dimensions.channel_rows()}
        for r in self.sales:
            self.assertIn(r["product_id"], products)
            self.assertIn(r["payment_method"], methods)
            self.assertIn(r["channel"], channels)
        for r in self.refunds:
            self.assertIn(r["refund_profile_key"], profiles)
            self.assertIn(r["product_id"], products)

    def test_s06_uses_the_successful_payment_not_the_failed_one(self):
        row = next(r for r in self.sales if r["order_id"] == "A-S06-1")
        self.assertEqual(("EASY_PAY", "A-S06-1-P2"), (row["payment_method"], row["payment_id"]))
        self.assertEqual(1, sum(1 for r in self.sales if r["order_id"] == "A-S06-1"))

    def test_s23_payment_date_is_kst(self):
        row = next(r for r in self.sales if r["order_id"] == "A-S23-1")
        self.assertEqual("2026-10-11T15:30:00.000Z", row["payment_at"])    # UTC로는 10-11
        self.assertEqual("2026-10-12", row["payment_date"])                # KST로는 10-12

    def test_s09_refund_rolls_up_into_sales_item(self):
        row = next(r for r in self.sales if r["order_item_id"] == "A-S09-1-1")
        self.assertEqual((2, 1, 35000), (row["quantity"], row["refund_quantity"], row["refund_amount"]))
        self.assertEqual("2026-10-12", row["last_refund_date"])
        refund = next(r for r in self.refunds if r["refund_id"] == "A-S09-1-R1")
        self.assertEqual(("RETURN-DEFECT", "2026-10-12", "2026-10-12", "A-S09-1-P1"),
                         (refund["refund_profile_key"], refund["payment_date"], refund["refund_date"],
                          refund["payment_id"]))

    def test_unpaid_and_cancelled_orders_have_no_fact(self):
        """S08은 결제 전 취소, S21의 주문은 격리되어 Silver에 없다. 결제 성공 주문은 채널당 11건이다
        (기존 8건 + 오류 사례 S26 1건 + S27 2건. S24·S25는 업무 키 위반, S28은 격리라 빠진다)."""
        ids = {r["order_id"] for r in self.sales if r["channel"] == "A"}
        self.assertNotIn("A-S08-1", ids)
        self.assertEqual(11, len(ids))


class TestRules(unittest.TestCase):
    """합성 Silver 행으로 제외·지연 도착·부분 환불 규칙을 확인한다 (현재 픽스처에 없는 경우)."""

    def test_cancelled_and_paid_order_is_excluded_with_its_refunds(self):
        o = "A-X1"
        rows = silver.rows_of(orders=[silver.order(o)], order_items=[silver.item(o)],
                              payments=[silver.payment(o)], order_cancellations=[silver.cancel(o)],
                              refunds=[silver.refund(o)])
        self.assertEqual(([], []), build_facts(rows))

    def test_payment_before_order_waits_for_the_order(self):
        o = "A-X2"
        early = silver.rows_of(payments=[silver.payment(o)])
        self.assertEqual(([], []), build_facts(early))
        arrived = silver.rows_of(orders=[silver.order(o)], order_items=[silver.item(o)],
                                 payments=[silver.payment(o)])
        sales, _ = build_facts(arrived)
        self.assertEqual(["A-X2-1"], [r["order_item_id"] for r in sales])

    def test_refund_before_payment_is_not_in_either_fact(self):
        o = "A-X3"
        rows = silver.rows_of(orders=[silver.order(o)], order_items=[silver.item(o)], refunds=[silver.refund(o)])
        self.assertEqual(([], []), build_facts(rows))

    def test_refund_pointing_at_a_failed_payment_is_not_in_fact_refund(self):
        """환불은 payment_id가 가리키는 결제 성공 행으로만 원결제일에 귀속한다 (event_spec.md 1.3·6.1절)."""
        o = "A-X10"
        rows = silver.rows_of(orders=[silver.order(o)], order_items=[silver.item(o)],
                              payments=[silver.payment(o, 1, status="FAILED"), silver.payment(o, 2)],
                              refunds=[silver.refund(o, payment_no=1)])
        sales, refunds = build_facts(rows)
        self.assertEqual((1, 0), (len(sales), len(refunds)))

    def test_missing_order_row_keeps_both_facts_empty(self):
        """orders 행이 아직 없으면(주문 이벤트 지연) 결제·환불 모두 다음 실행에서 반영한다. 두 Fact가 같이 움직인다."""
        o = "A-X11"
        rows = silver.rows_of(order_items=[silver.item(o)], payments=[silver.payment(o)], refunds=[silver.refund(o)])
        self.assertEqual(([], []), build_facts(rows))

    def test_failed_only_order_has_no_fact(self):
        o = "A-X4"
        rows = silver.rows_of(orders=[silver.order(o)], order_items=[silver.item(o)],
                              payments=[silver.payment(o, status="FAILED")])
        self.assertEqual(([], []), build_facts(rows))

    def test_every_item_of_a_paid_order_gets_a_row_with_amounts(self):
        o = "A-X5"
        rows = silver.rows_of(orders=[silver.order(o)],
                              order_items=[silver.item(o, 1, quantity=2, price=1500, discount=500),
                                           silver.item(o, 2, product="SKU-002", quantity=1, price=3000)],
                              payments=[silver.payment(o)])
        sales, _ = build_facts(rows)
        self.assertEqual([(3000, 500, 2500), (3000, 0, 3000)],
                         [(r["item_gross_amount"], r["item_discount"], r["item_paid_amount"]) for r in sales])
        self.assertEqual([(0, 0, None)] * 2, [(r["refund_quantity"], r["refund_amount"], r["last_refund_date"])
                                              for r in sales])

    def test_partial_refunds_are_summed_and_last_refund_date_is_the_latest_kst_date(self):
        o = "A-X6"
        rows = silver.rows_of(
            orders=[silver.order(o)], order_items=[silver.item(o, quantity=3)], payments=[silver.payment(o)],
            refunds=[silver.refund(o, 1, quantity=1, amount=1000, at="2026-10-12T02:00:00.000Z"),
                     silver.refund(o, 2, quantity=1, amount=1000, at="2026-10-12T15:00:00.000Z")])  # KST 10-13 00:00
        sales, refunds = build_facts(rows)
        self.assertEqual((2, 2000, "2026-10-13"), (sales[0]["refund_quantity"], sales[0]["refund_amount"],
                                                  sales[0]["last_refund_date"]))
        self.assertEqual(["2026-10-12", "2026-10-13"], sorted(r["refund_date"] for r in refunds))
        self.assertEqual({"2026-10-12"}, {r["payment_date"] for r in refunds})       # 원결제일로 귀속

    def test_refund_without_reason_is_profiled_as_other(self):
        o = "A-X7"
        rows = silver.rows_of(orders=[silver.order(o)], order_items=[silver.item(o)], payments=[silver.payment(o)],
                              refunds=[silver.refund(o, reason=None, kind="CANCEL")])
        _, refunds = build_facts(rows)
        self.assertEqual("CANCEL-OTHER", refunds[0]["refund_profile_key"])

    def test_anonymous_customer_is_kept_with_null_user(self):
        o = "A-X8"
        rows = silver.rows_of(orders=[silver.order(o, user=None)], order_items=[silver.item(o)],
                              payments=[silver.payment(o, user=None)])
        sales, _ = build_facts(rows)
        self.assertEqual([None], [r["user_id"] for r in sales])

    def test_same_order_id_in_two_channels_stays_separate(self):
        a = silver.rows_of(orders=[silver.order("X9")], order_items=[silver.item("X9")], payments=[silver.payment("X9")])
        b = silver.rows_of(orders=[silver.order("X9", channel="B")], order_items=[silver.item("X9", channel="B")],
                           payments=[silver.payment("X9", channel="B")])
        merged = {t: a[t] + b[t] for t in a}
        sales, _ = build_facts(merged)
        self.assertEqual(["A", "B"], sorted(r["channel"] for r in sales))


class TestSqlText(unittest.TestCase):
    def test_kst_date_expression_uses_seoul_time_zone(self):
        self.assertIn("from_utc_timestamp(p.payment_created_at, 'Asia/Seoul')", gold_sql.kst_date("p.payment_created_at"))

    def test_default_sql_uses_the_spark_expression(self):
        self.assertIn("from_utc_timestamp", gold_sql.sales_item_sql("s"))
        self.assertIn("from_utc_timestamp", gold_sql.refund_sql("s"))



class TestErrorCaseFixtures(unittest.TestCase):
    """오류 사례 시나리오 S24~S28이 명세대로 Gold에서 제외되는지 본다 (event_spec.md 6.1절). 정답 파일 비교가 아니라 시나리오별 동작 확인이다."""

    @classmethod
    def setUpClass(cls):
        cls.rows = silver.fixture_rows()
        cls.sales, cls.refunds = build_facts(cls.rows)
        cls.con = silver.connect(cls.rows)

    @classmethod
    def tearDownClass(cls):
        cls.con.close()

    def sales_of(self, order_id, channel="A"):
        return [r for r in self.sales if r["channel"] == channel and r["order_id"] == order_id]

    def test_double_payment_and_duplicate_order_created_are_excluded(self):
        for channel in "ABC":
            self.assertEqual([], self.sales_of(f"{channel}-S24-1", channel), channel)
            self.assertEqual([], self.sales_of(f"{channel}-S25-1", channel), channel)

    def test_refund_to_a_failed_payment_is_dropped_but_the_sale_stays(self):
        for channel in "ABC":
            rows = self.sales_of(f"{channel}-S26-1", channel)
            self.assertEqual(1, len(rows))
            self.assertEqual((0, 0), (rows[0]["refund_quantity"], rows[0]["refund_amount"]))
            self.assertFalse([r for r in self.refunds if r["channel"] == channel and r["order_id"] == f"{channel}-S26-1"])

    def test_refund_with_a_different_order_id_is_dropped_and_both_orders_stay(self):
        for channel in "ABC":
            self.assertEqual(1, len(self.sales_of(f"{channel}-S27-1", channel)))
            self.assertEqual(1, len(self.sales_of(f"{channel}-S27-2", channel)))
            self.assertFalse([r for r in self.refunds if r["order_id"].startswith(f"{channel}-S27")])
            self.assertEqual(0, sum(r["refund_amount"] for r in self.sales if r["order_id"].startswith(f"{channel}-S27")))

    def test_duplicate_item_ids_are_quarantined_so_there_is_no_order_and_no_fact(self):
        for channel in "ABC":
            self.assertEqual([], self.sales_of(f"{channel}-S28-1", channel))
            self.assertFalse([r for r in self.rows["orders"] if r["order_id"] == f"{channel}-S28-1"])
            self.assertTrue([r for r in self.rows["payments"] if r["order_id"] == f"{channel}-S28-1"])   # 결제 이벤트는 Silver에 남는다

    def test_exclusion_counts_per_channel(self):
        def count(sql):
            return silver.run(self.con, sql)[0]["n"]
        self.assertEqual(6, count(gold_sql.violation_order_count_sql("silver")))     # S24·S25 x 3채널
        self.assertEqual(6, count(gold_sql.invalid_refund_count_sql("silver")))      # S26·S27 x 3채널

    def test_the_normal_refund_is_still_valid(self):
        refund_orders = {r["order_id"] for r in self.refunds}
        self.assertEqual({"A-S09-1", "B-S09-1", "C-S09-1"}, refund_orders)


class TestExclusionRules(unittest.TestCase):
    """합성 Silver 행으로 위반 주문·무효 환불·보류 환불의 경계를 확인한다."""

    def facts(self, rows):
        return build_facts(rows)

    def counts(self, rows):
        con = silver.connect(rows)
        result = (silver.run(con, gold_sql.violation_order_count_sql("silver"))[0]["n"],
                  silver.run(con, gold_sql.invalid_refund_count_sql("silver"))[0]["n"])
        con.close()
        return result

    def base(self, **extra):
        o = "A-X1"
        tables = dict(orders=[silver.order(o)], order_items=[silver.item(o)], payments=[silver.payment(o)])
        tables.update(extra)
        return silver.rows_of(**tables)

    def test_a_valid_refund_reaches_both_facts_and_adds_up(self):
        sales, refunds = self.facts(self.base(refunds=[silver.refund("A-X1")]))
        self.assertEqual(1, len(refunds))
        self.assertEqual((1, 1000), (sales[0]["refund_quantity"], sales[0]["refund_amount"]))
        self.assertEqual((0, 0), self.counts(self.base(refunds=[silver.refund("A-X1")])))

    def test_refund_to_a_payment_that_failed_is_invalid(self):
        rows = self.base(refunds=[{**silver.refund("A-X1"), "payment_id": "A-X1-P2"}])
        rows["payments"].append(silver.payment("A-X1", no=2, status="FAILED"))
        sales, refunds = self.facts(rows)
        self.assertEqual(([], 1), ([r for r in refunds], len(sales)))
        self.assertEqual(0, sales[0]["refund_amount"])
        self.assertEqual((0, 1), self.counts(rows))

    def test_refund_with_another_order_id_is_invalid(self):
        rows = self.base(refunds=[{**silver.refund("A-X1"), "order_id": "A-X2"}])
        rows["orders"].append(silver.order("A-X2"))
        sales, refunds = self.facts(rows)
        self.assertEqual(([], 0), (refunds, sales[0]["refund_amount"]))
        self.assertEqual((0, 1), self.counts(rows))

    def test_refund_whose_item_belongs_to_another_order_is_invalid_even_with_a_matching_payment(self):
        rows = self.base(refunds=[{**silver.refund("A-X1"), "order_item_id": "A-X2-1"}])
        rows["orders"].append(silver.order("A-X2"))
        rows["order_items"].append(silver.item("A-X2"))
        sales, refunds = self.facts(rows)
        self.assertEqual(([], 0), (refunds, sum(r["refund_amount"] for r in sales)))
        self.assertEqual((0, 1), self.counts(rows))

    def test_an_invalid_refund_of_a_violation_order_is_counted_only_as_a_violation(self):
        rows = self.base(refunds=[{**silver.refund("A-X1"), "payment_id": "A-X1-P2"}])
        rows["payments"].append(silver.payment("A-X1", no=2, status="FAILED"))
        rows["orders"].append({**silver.order("A-X1"), "event_id": "A-X1-order2"})      # 주문 생성 2건
        self.assertEqual((1, 0), self.counts(rows))

    def test_refund_before_its_payment_arrives_is_pending_not_invalid(self):
        rows = silver.rows_of(orders=[silver.order("A-X1")], order_items=[silver.item("A-X1")], refunds=[silver.refund("A-X1")])
        sales, refunds = self.facts(rows)
        self.assertEqual(([], []), (sales, refunds))
        self.assertEqual((0, 0), self.counts(rows))
        rows["payments"].append(silver.payment("A-X1"))      # 결제가 도착하면 들어온다
        sales, refunds = self.facts(rows)
        self.assertEqual((1, 1), (len(sales), len(refunds)))

    def test_refund_of_a_violation_order_is_excluded_but_not_counted_as_invalid(self):
        rows = self.base(refunds=[silver.refund("A-X1")])
        rows["orders"].append({**silver.order("A-X1"), "event_id": "A-X1-order2"})      # 주문 생성 2건
        sales, refunds = self.facts(rows)
        self.assertEqual(([], []), (sales, refunds))
        self.assertEqual((1, 0), self.counts(rows))

    def test_two_successful_payments_make_a_violation_but_failed_retries_do_not(self):
        rows = self.base()
        rows["payments"].append(silver.payment("A-X1", no=2, status="FAILED"))
        self.assertEqual(0, self.counts(rows)[0])
        rows["payments"].append(silver.payment("A-X1", no=3))
        self.assertEqual(1, self.counts(rows)[0])
        self.assertEqual([], self.facts(rows)[0])

    def test_violation_in_one_channel_does_not_touch_the_same_order_id_in_another(self):
        rows = self.base()
        rows["orders"].append({**silver.order("A-X1", channel="B")})
        rows["order_items"].append(silver.item("A-X1", channel="B"))
        rows["payments"].append(silver.payment("A-X1", channel="B"))
        rows["payments"].append(silver.payment("A-X1", no=2, channel="B"))      # B 채널만 결제 성공 2건
        sales, _ = self.facts(rows)
        self.assertEqual(["A"], [r["channel"] for r in sales])


if __name__ == "__main__":
    unittest.main()
