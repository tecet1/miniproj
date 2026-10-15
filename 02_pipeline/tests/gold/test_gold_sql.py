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
