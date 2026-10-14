"""대시보드 정의와 생성된 Lakeview JSON을 검사한다. (plans/05 카드 5-F·5-G)

[검증 범위] 데이터셋이 존재하는 지표·기준만 조회하는지, 같은 조회를 로컬(sqlite)에서 돌렸을 때 열과 값이 손으로 계산한 기대값과 맞는지
(결제주 경계, 비가산 지표의 주별 재계산, 이전 기간·전년 동기·증감 문장, 채널·카테고리·기간 필터, 큰 순 정렬, 구성비 합),
위젯이 데이터셋의 열만 쓰는지, 위젯 위치가 격자 안에서 겹치지 않는지, 저장소의 JSON 파일이 정의와 같은지.
[검증하지 못하는 것] Databricks가 이 JSON(위젯 spec 버전·encodings, 선 차트·색 고정·정렬)을 대시보드로 열고 MEASURE() 쿼리와 CTE가 실행되는지,
sqlite에 등록한 Spark 함수(date_sub·datediff·add_months·weekday·format_number)가 Spark와 같게 동작하는지. 로컬 통과 != Databricks 성공.
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
import dimensions  # noqa: E402
import metric_defs  # noqa: E402

EXPECTED = json.loads((silver.FIXTURES / "expected" / "expected.json").read_text(encoding="utf-8"))
DATASETS = {d["name"]: d for d in spec.DATASETS}
DASHBOARD = build_dashboard.build_dashboard()
GRID = 6

ALL_CHANNELS = tuple(spec.CHANNEL_NAMES)
ALL_CATEGORIES = tuple(spec.CATEGORIES)
WIDE = ("2000-01-01", "2100-01-01")
CATEGORY_OF = {pid: category for pid, _, category in dimensions.product_rows(g.SKUS)}
CLOTHES, SHOES = CATEGORY_OF["SKU-001"], CATEGORY_OF["SKU-004"]
A, B = ALL_CHANNELS[0], ALL_CHANNELS[1]   # 채널 이름: A 자사몰, B 종합 오픈마켓


def local_text(text: str, date_range=WIDE, channels=ALL_CHANNELS, categories=ALL_CATEGORIES, compare_range=WIDE) -> str:
    """쿼리 문장의 매개변수에 값을 넣는다 (sqlite 실행용)."""
    text = text.replace(":date_range.min", f"'{date_range[0]}'").replace(":date_range.max", f"'{date_range[1]}'")
    text = text.replace(":compare_range.min", f"'{compare_range[0]}'").replace(":compare_range.max", f"'{compare_range[1]}'")
    # 다중 선택 값은 배열로 전달된다: 로컬에서는 JSON 배열 문자열로 넣고 sqlite에 등록한 array_contains가 읽는다
    text = text.replace(":channels", "'" + json.dumps(list(channels), ensure_ascii=False) + "'")
    return text.replace(":categories", "'" + json.dumps(list(categories), ensure_ascii=False) + "'")


def local_where(definition: dict, where: str, **filters) -> str:
    """where 조건의 매개변수에 값을 넣고 분석 기준 이름을 정의의 식으로 바꾼 조건. 한 번에 바꿔 식 안의 이름이 다시 바뀌지 않게 한다."""
    exprs = dict(definition["dimensions"])
    text = local_text(where, **filters)
    return re.sub(r"\b(" + "|".join(sorted(exprs, key=len, reverse=True)) + r")\b", lambda m: exprs[m.group(1)], text)


def local_select(**filters):
    """spec.compare_query에 넘기는 select 함수: 같은 정의를 일반 SQL로 조회한다."""
    def select(view, measures, where):
        definition = metric_defs.DEFINITIONS[view]
        return g.plain_sql(definition, [], measures, local_where(definition, where, **filters))
    return select


def local_rows(con, dataset: dict, **filters) -> list:
    """데이터셋을 로컬에서 실행한 결과. 필터·구성비·정렬·KPI 조립은 실제 쿼리와 같은 함수를 쓴다."""
    if dataset.get("compare"):
        return silver.run(con, spec.compare_query(dataset, local_select(**filters)))
    definition = metric_defs.DEFINITIONS[dataset["view"]]
    inner = g.plain_sql(definition, dataset["dims"], dataset["measures"], local_where(definition, spec.where_clause(), **filters))
    return silver.run(con, spec.wrap_query(dataset, inner))


# ── 손으로 계산하는 합성 입력 ──────────────────────────────────────────────────────────
def sale(order_id, paid_at, user, quantity, sku="SKU-001", channel="A", price=1000):
    """결제 성공 주문 하나(상품 1개). 금액 = quantity x price. paid_at은 UTC이고 KST는 +9시간이다."""
    return {"orders": [silver.order(order_id, user=user, at=paid_at, channel=channel)],
            "order_items": [{**silver.item(order_id, product=sku, quantity=quantity, price=price, channel=channel)}],
            "payments": [silver.payment(order_id, amount=quantity * price, at=paid_at, user=user, channel=channel)]}


def refund_of(order_id, amount, at, channel="A", user="U1"):
    return {"refunds": [silver.refund(order_id, quantity=1, amount=amount, at=at, channel=channel, user=user)]}


def combine(*parts) -> dict:
    rows = {table: [] for table in silver.COLUMNS}
    for part in parts:
        for table, items in part.items():
            rows[table] += items
    return rows


def weekly_rows() -> dict:
    """결제주 경계 시나리오 (자사몰 A, 금액은 quantity x 1,000원). 2026-10-12는 월요일이다.

    O1  10-11 23:59 KST(일)   1,000  U1  -> 주 10-05
    O2  10-12 00:00 KST(월)   2,000  U1  -> 주 10-12   (UTC 날짜로는 10-11이라 UTC로 자르면 주가 틀린다)
    O3  10-14 12:00 KST(수)   3,000  U1  -> 주 10-12
    O4  10-14 12:00 KST(수)   4,000  U2  -> 주 10-12
    O5  2025-12-31 12:00 KST(수)  500  U3 -> 주 2025-12-29 (연말)
    O6  2026-01-01 12:00 KST(목)  700  U4 -> 주 2025-12-29 (연초, 같은 주)
    R1  O1을 10-19(다음 주)에 400원 환불 -> 원결제일 귀속이라 주 10-05의 환불금액
    """
    return combine(
        sale("O1", "2026-10-11T14:59:00.000Z", "U1", 1), sale("O2", "2026-10-11T15:00:00.000Z", "U1", 2),
        sale("O3", "2026-10-14T03:00:00.000Z", "U1", 3), sale("O4", "2026-10-14T03:00:00.000Z", "U2", 4),
        sale("O5", "2025-12-31T03:00:00.000Z", "U3", 1, price=500), sale("O6", "2026-01-01T03:00:00.000Z", "U4", 1, price=700),
        refund_of("O1", 400, "2026-10-19T03:00:00.000Z"))


def compare_rows() -> dict:
    """비교 기간 시나리오. 조회 기간 2026-10-08 ~ 2026-10-14 (7일), 이전 기간 10-01 ~ 10-07, 전년 동기 2025-10-08 ~ 2025-10-14.

    현재  C1  10-07 15:30Z = 10-08 00:30 KST  A 의류  U1  2,000   (UTC 날짜로는 10-07이라 UTC로 자르면 이전 기간이 된다)
          C2  10-14 12:00 KST            A 의류  U1  3,000
          C3  10-14 12:00 KST            B 신발  U2  5,000
          C4  10-15 12:00 KST            A 의류  U9  9,999   (기간 밖: 끝 다음 날)
          R1  C1을 10-20에 1,000원 환불 -> 원결제일 10-08에 귀속
    이전  P1  10-01 12:00 KST            A 의류  U1  4,000
          P2  10-07 14:59Z = 10-07 23:59 KST A 의류 U3  1,000
          P0  09-30 12:00 KST            A 의류  U8  7,777   (기간 밖: 시작 전날)
    전년  Y1  2025-10-10 12:00 KST       A 의류  U1  20,000
    """
    return combine(
        sale("C1", "2026-10-07T15:30:00.000Z", "U1", 2), sale("C2", "2026-10-14T03:00:00.000Z", "U1", 3),
        sale("C3", "2026-10-14T03:00:00.000Z", "U2", 5, sku="SKU-004", channel="B"),
        sale("C4", "2026-10-15T03:00:00.000Z", "U9", 1, price=9999),
        refund_of("C1", 1000, "2026-10-20T03:00:00.000Z"),
        sale("P1", "2026-10-01T03:00:00.000Z", "U1", 4), sale("P2", "2026-10-07T14:59:00.000Z", "U3", 1),
        sale("P0", "2026-09-30T03:00:00.000Z", "U8", 1, price=7777),
        sale("Y1", "2025-10-10T03:00:00.000Z", "U1", 20))


RANGE = ("2026-10-08", "2026-10-14")
PREV = ("2026-10-01", "2026-10-07")        # 사용자가 고른 비교 기간: 바로 앞 7일
LAST_YEAR = ("2025-10-08", "2025-10-14")   # 사용자가 고른 비교 기간: 1년 전 같은 7일


class TestSpec(unittest.TestCase):
    def test_datasets_use_only_defined_dimensions_and_measures(self):
        for dataset in spec.DATASETS:
            with self.subTest(dataset=dataset["name"]):
                if dataset.get("compare"):
                    for measure, shape in dataset["compare"].items():
                        self.assertIn(measure, {n for n, _ in metric_defs.DEFINITIONS[shape["view"]]["measures"]})
                    continue
                definition = metric_defs.DEFINITIONS[dataset["view"]]
                self.assertTrue(set(dataset["dims"]) <= {n for n, _ in definition["dimensions"]})
                self.assertTrue(set(dataset["measures"]) <= {n for n, _ in definition["measures"]})

    def test_query_text(self):
        sql = spec.dataset_query(DATASETS["sales_by_channel"])
        self.assertTrue(sql.startswith("SELECT channel_name, MEASURE(total_sales) AS total_sales,"))
        self.assertTrue(sql.endswith("FROM commerce_poc.serving.sales_metrics WHERE payment_date BETWEEN :date_range.min "
                                     "AND :date_range.max AND array_contains(:channels, channel_name) AND array_contains(:categories, category) "
                                     "GROUP BY ALL ORDER BY total_sales DESC, channel_name"))
        reason = spec.dataset_query(DATASETS["refund_by_reason"])
        self.assertIn("refund_amount_share", reason)
        self.assertIn("OVER ()", reason)

    def test_every_measure_in_a_query_is_wrapped_in_measure(self):
        for dataset in spec.DATASETS:
            sql = spec.dataset_query(dataset)
            measures = dataset["compare"] if dataset.get("compare") else dataset["measures"]
            for measure in measures:
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
        want = EXPECTED["total"]
        self.assertEqual((want["total_sales"], want["refund_amount"], want["net_sales"], want["paid_item_count"],
                          want["paying_customers"]), (sales["total_sales"], sales["refund_amount"], sales["net_sales"],
                                                      sales["item_count"], sales["paying_customers"]))
        self.assertAlmostEqual(want["refund_ratio"], sales["refund_ratio"], places=12)
        refund = local_rows(self.con, DATASETS["refund_kpi"])[0]
        self.assertEqual((want["refund_amount"], want["refund_quantity"]), (refund["refund_amount"], refund["refund_quantity"]))

    def test_refund_share_sums_to_one(self):
        rows = local_rows(self.con, DATASETS["refund_by_reason"])
        self.assertAlmostEqual(1.0, sum(r["refund_amount_share"] for r in rows))
        self.assertEqual([("DEFECT", 105000)], [(r["refund_reason"], r["refund_amount"]) for r in rows])

    def test_refund_share_splits_between_reasons_largest_first(self):
        rows = silver.rows_of(
            orders=[silver.order("O1")], order_items=[silver.item("O1", quantity=4, price=1000)],
            payments=[silver.payment("O1", amount=4000)],
            refunds=[silver.refund("O1", no=1, quantity=1, amount=1000, reason="DEFECT"),
                     silver.refund("O1", no=2, quantity=1, amount=3000, reason="CHANGE_OF_MIND")])
        con = g.gold_connection(rows)
        result = local_rows(con, DATASETS["refund_by_reason"])
        self.assertEqual([("CHANGE_OF_MIND", 0.75), ("DEFECT", 0.25)], [(r["refund_reason"], r["refund_amount_share"]) for r in result])

    def test_channel_net_share_sums_to_one_and_is_sorted_by_net_sales(self):
        rows = local_rows(self.con, DATASETS["channel_net_share"])
        self.assertEqual(3, len(rows))
        self.assertAlmostEqual(1.0, sum(r["net_sales_share"] for r in rows), places=12)
        want = {name: EXPECTED["per_channel"][ch]["net_sales"] for ch, name in dimensions.channel_rows()}
        self.assertEqual(want, {r["channel_name"]: r["net_sales"] for r in rows})
        self.assertEqual(sorted((r["net_sales"] for r in rows), reverse=True), [r["net_sales"] for r in rows])

    def test_sorting_is_largest_first_and_ties_break_by_name(self):
        rows = combine(sale("T1", "2026-10-12T03:00:00.000Z", "U1", 3, channel="A"),
                       sale("T2", "2026-10-12T03:00:00.000Z", "U2", 3, channel="B"),
                       sale("T3", "2026-10-12T03:00:00.000Z", "U3", 5, channel="C"))
        con = g.gold_connection(rows)
        for name in ("channel_net_share", "sales_by_channel"):
            with self.subTest(dataset=name):
                got = [r["channel_name"] for r in local_rows(con, DATASETS[name])]
                self.assertEqual([ALL_CHANNELS[2], *sorted(ALL_CHANNELS[:2])], got)   # 5,000원이 먼저, 3,000원 동률은 이름순

    def test_ratio_by_channel_is_sorted_by_ratio_not_by_amount(self):
        """채널별 환불금액비율은 비율이 큰 순이다 (규모가 큰 채널이 위로 오지 않는다)."""
        rows = combine(sale("T1", "2026-10-12T03:00:00.000Z", "U1", 10, channel="A"),      # 10,000원, 환불 1,000 -> 0.1
                       refund_of("T1", 1000, "2026-10-13T03:00:00.000Z", channel="A"),
                       sale("T2", "2026-10-12T03:00:00.000Z", "U2", 2, channel="B"),        # 2,000원, 환불 1,000 -> 0.5
                       refund_of("T2", 1000, "2026-10-13T03:00:00.000Z", channel="B", user="U2"))
        got = [(r["channel_name"], r["refund_ratio"]) for r in local_rows(g.gold_connection(rows), DATASETS["refund_ratio_by_channel"])]
        self.assertEqual([(ALL_CHANNELS[1], 0.5), (ALL_CHANNELS[0], 0.1)], got)

    def test_sales_by_method_and_category_are_sorted_largest_first(self):
        for name, column in (("sales_by_method", "total_sales"), ("sales_by_category", "total_sales")):
            rows = local_rows(self.con, DATASETS[name])
            with self.subTest(dataset=name):
                self.assertEqual(sorted((r[column] for r in rows), reverse=True), [r[column] for r in rows])
                self.assertEqual(1266000, sum(r[column] for r in rows))

    def test_sales_by_channel_table_has_five_measures(self):
        rows = local_rows(self.con, DATASETS["sales_by_channel"])
        self.assertEqual(["channel_name", "total_sales", "net_sales", "refund_ratio", "paying_customers", "average_order_value"],
                         list(rows[0]))
        self.assertEqual(1266000, sum(r["total_sales"] for r in rows))


class TestWeeks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con = g.gold_connection(weekly_rows())

    def by_week(self, name="refund_ratio_by_week"):
        return {r["payment_week"]: r for r in local_rows(self.con, DATASETS[name])}

    def test_week_boundaries_start_on_monday_in_kst(self):
        """일요일 23:59 KST는 앞 주, 월요일 00:00 KST는 새 주. 연말·연초는 같은 주(2025-12-29 시작)."""
        weeks = {r["payment_week"]: r["total_sales"] for r in local_rows(self.con, DATASETS["sales_trend_by_channel_week"])}
        self.assertEqual({"2025-12-29": 1200, "2026-10-05": 1000, "2026-10-12": 9000}, weeks)

    def test_weeks_are_sorted_ascending(self):
        weeks = [r["payment_week"] for r in local_rows(self.con, DATASETS["sales_trend_by_channel_week"])]
        self.assertEqual(sorted(weeks), weeks)

    def test_additive_measures_add_up_over_weeks(self):
        rows = local_rows(self.con, DATASETS["sales_trend_by_channel_week"])
        self.assertEqual(11200, sum(r["total_sales"] for r in rows))
        self.assertEqual(11200, local_rows(self.con, DATASETS["sales_kpi"], date_range=WIDE)[0]["total_sales"])

    def test_refund_ratio_is_recomputed_per_week_with_original_payment_week(self):
        """환불은 원결제일의 주에 귀속된다: 환불이 10-19(다음 주)에 일어나도 주 10-05의 분자다. 주 10-05 = 400/1,000."""
        weeks = self.by_week()
        self.assertAlmostEqual(0.4, weeks["2026-10-05"]["refund_ratio"], places=12)
        self.assertEqual(0.0, weeks["2026-10-12"]["refund_ratio"])
        self.assertEqual(0.0, weeks["2025-12-29"]["refund_ratio"])
        refunds = {r["payment_week"]: r["refund_amount"] for r in local_rows(self.con, DATASETS["refund_trend_by_week"])}
        self.assertEqual({"2026-10-05": 400}, refunds)

    def test_customers_and_order_value_are_recounted_per_week_not_summed_from_days(self):
        """주 10-12: U1이 이틀(10-12, 10-14), U2가 하루 결제 -> 일별 고객 합은 3명이지만 주의 결제고객수는 2명, 객단가 9,000/2."""
        con = self.con
        daily = {r["payment_date"]: r["paying_customers"] for r in
                 silver.run(con, g.plain_sql(metric_defs.SALES_METRICS, ["payment_date"], ["paying_customers"]))}
        self.assertEqual(3, daily["2026-10-12"] + daily["2026-10-14"])
        week = silver.run(con, g.plain_sql(metric_defs.SALES_METRICS, ["payment_week"], ["paying_customers", "average_order_value"],
                                           "payment_week = '2026-10-12'"))[0]
        self.assertEqual((2, 4500.0), (week["paying_customers"], week["average_order_value"]))

    def test_weekly_trend_follows_channel_and_category_filters(self):
        rows = combine(weekly_rows(), sale("S1", "2026-10-14T03:00:00.000Z", "U5", 1, sku="SKU-004", channel="B", price=800))
        con = g.gold_connection(rows)
        only_b = local_rows(con, DATASETS["sales_trend_by_channel_week"], channels=(B,))
        self.assertEqual([("2026-10-12", B, 800)], [(r["payment_week"], r["channel_name"], r["total_sales"]) for r in only_b])
        shoes = local_rows(con, DATASETS["sales_trend_by_channel_week"], categories=(SHOES,))
        self.assertEqual([("2026-10-12", 800)], [(r["payment_week"], r["total_sales"]) for r in shoes])
        clothes_only_week = local_rows(con, DATASETS["refund_ratio_by_week"], categories=(CLOTHES,), date_range=("2026-10-12", "2026-10-18"))
        self.assertEqual([("2026-10-12", 0.0)], [(r["payment_week"], r["refund_ratio"]) for r in clothes_only_week])

    def test_partial_first_week_is_cut_by_the_selected_range(self):
        """조회 기간이 주 중간(수요일)에서 시작하면 첫 주는 그날 이후 값만 갖는다 (부분 주). 주 10-12 = O3+O4 = 7,000."""
        weeks = {r["payment_week"]: r["total_sales"] for r in
                 local_rows(self.con, DATASETS["sales_trend_by_channel_week"], date_range=("2026-10-14", "2026-10-31"))}
        self.assertEqual({"2026-10-12": 7000}, weeks)


class TestCompare(unittest.TestCase):
    """비교 표: 지표마다 한 행(지표, 조회 기간 값, 비교 기간 값, 증감). 조회 기간 2026-10-08 ~ 14, 비교 기간은 사용자가 고른다.
    PREV(10-01~07): 총 5,000, 순 5,000, 고객 2명(U1,U3), 건수 2, 환불 0. LAST_YEAR(2025-10-08~14): 총 20,000(Y1, U1), 건수 1."""

    @classmethod
    def setUpClass(cls):
        cls.con = g.gold_connection(compare_rows())

    def table(self, name="sales_compare", con=None, date_range=RANGE, compare_range=PREV, **filters):
        rows = local_rows(con or self.con, DATASETS[name], date_range=date_range, compare_range=compare_range, **filters)
        return {r["metric"]: (r["current_value"], r["compare_value"], r["change"]) for r in rows}

    def test_one_row_per_metric_in_card_order(self):
        rows = local_rows(self.con, DATASETS["sales_compare"], date_range=RANGE, compare_range=PREV)
        self.assertEqual([spec.LABELS[m] for m in spec.SALES_COMPARE], [r["metric"] for r in rows])
        self.assertEqual(list(range(1, 9)), [r["sort_order"] for r in rows])

    def test_against_the_previous_seven_days(self):
        self.assertEqual({
            "총판매금액": ("10,000원", "5,000원", "▲ 100.0%"),
            "순판매금액": ("9,000원", "5,000원", "▲ 80.0%"),
            "환불금액비율": ("10.0%", "0.0%", "▲ 10.0%p"),        # 비율의 차(%p), 비교값이 0이어도 계산된다
            "결제고객수": ("2명", "2명", "0.0%"),                   # 같으면 방향 기호 없음
            "객단가": ("5,000원", "2,500원", "▲ 100.0%"),           # 기간마다 다시 계산한 값
            "상품결제건수": ("3건", "2건", "▲ 50.0%"),
            "상품결제단가": ("3,333원", "2,500원", "▲ 33.3%"),      # 3,333.33 vs 2,500
            "환불금액": ("1,000원", "0원", "-"),                    # 비교값이 0이라 증감률을 낼 수 없다
        }, self.table())

    def test_against_the_same_week_last_year(self):
        table = self.table(compare_range=LAST_YEAR)
        self.assertEqual(("10,000원", "20,000원", "▼ 50.0%"), table["총판매금액"])
        self.assertEqual(("9,000원", "20,000원", "▼ 55.0%"), table["순판매금액"])
        self.assertEqual(("2명", "1명", "▲ 100.0%"), table["결제고객수"])
        self.assertEqual(("3,333원", "20,000원", "▼ 83.3%"), table["상품결제단가"])

    def test_compare_period_can_have_a_different_length(self):
        """비교 기간은 조회 기간과 길이가 달라도 된다: 2025-10 한 달(Y1 20,000)."""
        self.assertEqual(("10,000원", "20,000원", "▼ 50.0%"),
                         self.table(compare_range=("2025-10-01", "2025-10-31"))["총판매금액"])

    def test_empty_comparison_period_says_no_results(self):
        for filters in ({"channels": (B,)}, {"categories": (SHOES,)}):
            table = self.table(**filters)
            with self.subTest(filters=filters):
                self.assertEqual(("5,000원", "-", "실적이 없습니다."), table["총판매금액"])
                self.assertEqual(("1명", "0명", "실적이 없습니다."), table["결제고객수"])
                self.assertEqual({"실적이 없습니다."}, {row[2] for row in table.values()})

    def test_customers_are_recounted_inside_each_period_for_the_selection(self):
        """자사몰만: 현재 고객 1명(U1)이 비교 기간 2명 대비 50% 감소, 총판매금액은 5,000 vs 5,000."""
        table = self.table(channels=(A,))
        self.assertEqual(("5,000원", "5,000원", "0.0%"), table["총판매금액"])
        self.assertEqual(("1명", "2명", "▼ 50.0%"), table["결제고객수"])
        self.assertEqual(("5,000원", "2,500원", "▲ 100.0%"), table["객단가"])

    def test_refund_table_attributes_refunds_to_the_original_payment_date(self):
        """C1(10-08 결제)의 환불은 10-20에 일어났지만 조회 기간의 환불이다. 환불금액비율은 sales_metrics 값(1,000/10,000)이다."""
        self.assertEqual({"환불금액": ("1,000원", "-", "-"), "환불수량": ("1개", "-", "-"),
                          "환불금액비율": ("10.0%", "0.0%", "▲ 10.0%p")}, self.table("refund_compare"))

    def test_refund_table_without_sales_in_comparison_period_says_no_results(self):
        """오픈마켓만: 현재 환불이 없어 "-", 비교 기간에 결제가 없어 "실적이 없습니다."."""
        self.assertEqual(("-", "-", "실적이 없습니다."), self.table("refund_compare", channels=(B,))["환불금액"])

    def test_empty_current_period_shows_dash(self):
        table = self.table(date_range=("2026-10-09", "2026-10-13"))
        self.assertEqual("-", table["총판매금액"][0])

    def test_range_edges_include_both_end_days(self):
        one_day = local_rows(self.con, DATASETS["sales_kpi"], date_range=("2026-10-08", "2026-10-08"))[0]
        self.assertEqual(2000, one_day["total_sales"])            # C1만 (KST 10-08 00:30)
        none = local_rows(self.con, DATASETS["sales_kpi"], date_range=("2026-10-09", "2026-10-13"))[0]
        self.assertEqual((None, 0), (none["total_sales"], none["item_count"]))
        edge = self.table(compare_range=("2026-10-07", "2026-10-07"))["총판매금액"]   # P2만 (KST 10-07 23:59)
        self.assertEqual("1,000원", edge[1])


class TestFilters(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.con = g.gold_connection(silver.fixture_rows())

    def test_every_dataset_query_applies_the_selections(self):
        """모든 지표 데이터셋이 채널·카테고리·조회 기간을 쓰고, 비교 표만 비교 기간을 더 쓴다."""
        for dataset in spec.DATASETS:
            sql = spec.dataset_query(dataset)
            want = {"date_range", "channels", "categories"} | ({"compare_range"} if dataset.get("compare") else set())
            with self.subTest(dataset=dataset["name"]):
                self.assertEqual(want, {f["param"] for f in spec.FILTERS if spec.uses_parameter(dataset, f["param"])})
                self.assertEqual(sql.count("WHERE"), sql.count(":channels"))   # 지표를 조회하는 곳마다 같은 WHERE가 걸린다

    def test_filter_dimensions_exist_in_both_views(self):
        for definition in metric_defs.DEFINITIONS.values():
            names = {n for n, _ in definition["dimensions"]}
            self.assertTrue({f["dimension"] for f in spec.FILTERS} <= names, definition["name"])

    def test_filter_options_match_dimension_tables(self):
        self.assertEqual([name for _, name in dimensions.channel_rows()], spec.CHANNEL_NAMES)
        self.assertEqual(sorted({category for _, _, category in dimensions.product_rows(g.SKUS)}), sorted(spec.CATEGORIES))
        self.assertEqual(set(spec.CHANNEL_NAMES), set(spec.CHANNEL_COLORS))

    def test_single_channel_filter_gives_that_channels_expected_values(self):
        for channel, name in dimensions.channel_rows():
            row = local_rows(self.con, DATASETS["sales_kpi"], channels=(name,))[0]
            want = EXPECTED["per_channel"][channel]
            with self.subTest(channel=channel):
                self.assertEqual((want["total_sales"], want["refund_amount"], want["paid_item_count"], want["paying_customers"]),
                                 (row["total_sales"], row["refund_amount"], row["item_count"], row["paying_customers"]))
                self.assertAlmostEqual(want["average_order_value"], row["average_order_value"], places=9)

    def test_two_channels_recount_customers_and_ratios_over_the_selection(self):
        names = ALL_CHANNELS[:2]
        row = local_rows(self.con, DATASETS["sales_kpi"], channels=names)[0]
        a, b = (EXPECTED["per_channel"][c] for c in "AB")
        sales, customers = a["total_sales"] + b["total_sales"], a["paying_customers"] + b["paying_customers"]
        self.assertEqual((sales, customers), (row["total_sales"], row["paying_customers"]))
        self.assertAlmostEqual(sales / customers, row["average_order_value"], places=9)
        self.assertAlmostEqual((a["refund_amount"] + b["refund_amount"]) / sales, row["refund_ratio"], places=12)

    def test_category_filter_recomputes_customer_metrics(self):
        """카테고리 하나만 고르면 결제고객수·객단가가 그 카테고리 안에서 다시 계산된다 (카테고리별 값의 합과 다르다)."""
        total = local_rows(self.con, DATASETS["sales_kpi"])[0]
        parts = [local_rows(self.con, DATASETS["sales_kpi"], categories=(c,))[0] for c in ALL_CATEGORIES]
        self.assertEqual(total["total_sales"], sum(p["total_sales"] or 0 for p in parts))
        self.assertGreater(sum(p["paying_customers"] for p in parts), total["paying_customers"])
        for part in parts:
            if part["paying_customers"]:
                self.assertAlmostEqual(part["total_sales"] / part["paying_customers"], part["average_order_value"], places=9)

    def test_date_range_includes_the_edge_days_and_excludes_others(self):
        day = local_rows(self.con, DATASETS["sales_kpi"], date_range=("2026-10-12", "2026-10-12"))[0]
        self.assertEqual(1266000, day["total_sales"])
        for rng in (("2026-10-13", "2026-10-20"), ("2026-10-01", "2026-10-11")):
            row = local_rows(self.con, DATASETS["sales_kpi"], date_range=rng)[0]
            with self.subTest(range=rng):
                self.assertEqual((None, 0), (row["total_sales"], row["item_count"]))

    def test_refund_datasets_follow_the_same_filters(self):
        self.assertEqual(105000, local_rows(self.con, DATASETS["refund_kpi"], date_range=("2026-10-12", "2026-10-12"))[0]["refund_amount"])
        self.assertEqual([], local_rows(self.con, DATASETS["refund_by_reason"], date_range=("2026-10-13", "2026-10-13")))
        only_a = local_rows(self.con, DATASETS["refund_ratio_by_channel"], channels=(ALL_CHANNELS[0],))
        self.assertEqual([ALL_CHANNELS[0]], [r["channel_name"] for r in only_a])
        self.assertAlmostEqual(EXPECTED["per_channel"]["A"]["refund_ratio"], only_a[0]["refund_ratio"], places=12)

    def test_refund_share_is_recomputed_over_the_filtered_rows(self):
        rows = silver.rows_of(
            orders=[silver.order("O1"), silver.order("O2", channel="B")],
            order_items=[silver.item("O1", quantity=4, price=1000), silver.item("O2", quantity=4, price=1000, channel="B")],
            payments=[silver.payment("O1", amount=4000), silver.payment("O2", amount=4000, channel="B")],
            refunds=[silver.refund("O1", quantity=1, amount=1000, reason="DEFECT"),
                     silver.refund("O2", quantity=1, amount=3000, reason="OTHER", channel="B")])
        con = g.gold_connection(rows)
        both = {r["refund_reason"]: r["refund_amount_share"] for r in local_rows(con, DATASETS["refund_by_reason"])}
        only_a = {r["refund_reason"]: r["refund_amount_share"] for r in
                  local_rows(con, DATASETS["refund_by_reason"], channels=(ALL_CHANNELS[0],))}
        self.assertEqual({"DEFECT": 0.25, "OTHER": 0.75}, both)
        self.assertEqual({"DEFECT": 1.0}, only_a)


def is_selector(item: dict) -> bool:
    return item["widget"].get("spec", {}).get("widgetType", "").startswith("filter-")


def selectors(page: dict) -> list:
    return [i for i in page["layout"] if is_selector(i)]


def widgets(dashboard: dict) -> list:
    """페이지의 데이터 위젯 (맨 위 선택기와 쿼리 없는 텍스트 위젯 제외)."""
    return [(page["name"], item) for page in dashboard["pages"] for item in page["layout"]
            if "queries" in item["widget"] and not is_selector(item)]


# plans/05 카드 5-G "최종 시안 기준 구현 명세"의 배치표를 그대로 옮긴 독립 기대값: (종류, 데이터셋.열 또는 섹션 제목, x, y, w, h).
# y는 시안 기준(선택기 없이 0부터)이고, 실제 위치는 맨 위 선택기 줄(높이 2) 아래로 2만큼 내려간다 (2026-10-14 사용자 결정).
SELECTOR_ROW = [("filter", "channels", 0, 0, 1, 2), ("filter", "categories", 1, 0, 1, 2),
                ("filter", "date_range", 2, 0, 2, 2), ("filter", "compare_range", 4, 0, 2, 2)]
SPEC_LAYOUT = {
    "sales": [
        ("text", "핵심 지표", 0, 0, 6, 1),
        ("counter", "sales_kpi.total_sales", 0, 1, 2, 3), ("counter", "sales_kpi.net_sales", 2, 1, 2, 3),
        ("counter", "sales_kpi.refund_ratio", 4, 1, 2, 3),
        ("counter", "sales_kpi.paying_customers", 0, 4, 1, 2), ("counter", "sales_kpi.average_order_value", 1, 4, 1, 2),
        ("counter", "sales_kpi.item_count", 2, 4, 1, 2), ("counter", "sales_kpi.unit_price_per_item", 3, 4, 1, 2),
        ("counter", "sales_kpi.refund_amount", 4, 4, 1, 2), ("counter", "refund_kpi.refund_quantity", 5, 4, 1, 2),
        ("table", "sales_compare", 0, 6, 6, 6),
        ("text", "추이", 0, 12, 6, 1),
        ("line", "sales_trend_by_channel_week", 0, 13, 6, 6),
        ("bar", "channel_net_share", 0, 19, 3, 6), ("line", "refund_ratio_by_week", 3, 19, 3, 6),
        ("text", "구성", 0, 25, 6, 1),
        ("bar", "sales_by_method", 0, 26, 3, 6), ("bar", "sales_by_category", 3, 26, 3, 6),
        ("text", "상세", 0, 32, 6, 1),
        ("table", "sales_by_channel", 0, 33, 6, 5),
    ],
    "refund": [
        ("text", "핵심 지표", 0, 0, 6, 1),
        ("counter", "refund_kpi.refund_amount", 0, 1, 2, 3), ("counter", "refund_kpi.refund_quantity", 2, 1, 2, 3),
        ("counter", "sales_kpi.refund_ratio", 4, 1, 2, 3),
        ("table", "refund_compare", 0, 4, 6, 3),
        ("text", "구성", 0, 7, 6, 1),
        ("bar", "refund_by_reason", 0, 8, 3, 6), ("bar", "refund_ratio_by_channel", 3, 8, 3, 6),
        ("text", "추이", 0, 14, 6, 1),
        ("line", "refund_trend_by_week", 0, 15, 6, 6),
        ("text", "상세", 0, 21, 6, 1),
        ("table", "refund_by_reason", 0, 22, 6, 4),
    ],
}


class TestJson(unittest.TestCase):
    def test_names_are_unique(self):
        dataset_names = [d["name"] for d in DASHBOARD["datasets"]]
        page_names = [p["name"] for p in DASHBOARD["pages"]]
        widget_names = [i["widget"]["name"] for p in DASHBOARD["pages"] for i in p["layout"]]
        for names in (dataset_names, page_names, widget_names):
            self.assertEqual(len(names), len(set(names)))
            self.assertTrue(all(re.fullmatch(r"[0-9a-f]{8}", n) for n in names))

    def test_widget_queries_point_to_existing_dataset_columns(self):
        by_id = {d["name"]: d for d in DASHBOARD["datasets"]}
        columns = {build_dashboard.dataset_id(name): spec.dataset_columns(d) for name, d in DATASETS.items()}
        columns.update({build_dashboard.dataset_id(o["name"]): o["columns"] for o in spec.OPTION_DATASETS})
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

    def test_no_global_filter_panel(self):
        self.assertEqual(["PAGE_TYPE_CANVAS"] * 2, [p["pageType"] for p in DASHBOARD["pages"]])

    def test_each_page_has_four_selectors_bound_to_its_own_datasets(self):
        kinds = {"date_range": "filter-date-range-picker", "multi_select": "filter-multi-select"}
        for page, page_spec in zip(DASHBOARD["pages"], spec.PAGES):
            used = {w["dataset"] for w in page_spec["widgets"] if w.get("dataset")}
            by_param = {}
            for item in selectors(page):
                fields = item["widget"]["spec"]["encodings"]["fields"]
                queries = {q["name"]: q["query"] for q in item["widget"]["queries"]}
                bound = [f for f in fields if "parameterName" in f]
                self.assertEqual(1, len({f["parameterName"] for f in bound}))
                for f in bound:
                    self.assertEqual(f["parameterName"], queries[f["queryName"]]["parameters"][0]["keyword"])
                by_param[bound[0]["parameterName"]] = ({queries[f["queryName"]]["datasetName"] for f in bound},
                                                       item["widget"]["spec"]["widgetType"])
            want = {f["param"]: ({build_dashboard.dataset_id(n) for n in used if spec.uses_parameter(DATASETS[n], f["param"])},
                                 kinds[f["kind"]]) for f in spec.FILTERS}
            with self.subTest(page=page["displayName"]):
                self.assertEqual(want, by_param)
                self.assertEqual({"channels", "categories", "date_range", "compare_range"}, set(by_param))
                self.assertTrue(by_param["compare_range"][0])   # 비교 표 데이터셋에 묶여 있다

    def test_option_datasets_offer_the_dimension_values(self):
        for page in DASHBOARD["pages"]:
            items = [i for i in selectors(page) if i["widget"]["spec"]["widgetType"] == "filter-multi-select"]
            self.assertEqual({"channel_name", "category"},
                             {f["fieldName"] for i in items for f in i["widget"]["spec"]["encodings"]["fields"] if "fieldName" in f})
            for item in items:
                options = [f for f in item["widget"]["spec"]["encodings"]["fields"] if "fieldName" in f][0]
                query = next(q for q in item["widget"]["queries"] if q["name"] == options["queryName"])["query"]
                flt = next(f for f in spec.FILTERS if f["dimension"] == options["fieldName"])
                self.assertEqual(build_dashboard.dataset_id(flt["options"]), query["datasetName"])

    def test_datasets_declare_only_the_parameters_they_use_with_defaults(self):
        options = {o["name"] for o in spec.OPTION_DATASETS}
        for dataset in DASHBOARD["datasets"]:
            if dataset["displayName"] in options:
                self.assertNotIn("parameters", dataset)
                continue
            by_key = {p["keyword"]: p for p in dataset["parameters"]}
            used = {f["param"] for f in spec.FILTERS if spec.uses_parameter(DATASETS[dataset["displayName"]], f["param"])}
            with self.subTest(dataset=dataset["displayName"]):
                self.assertEqual(used, set(by_key))
                for key, want in (("date_range", ("now-30d/d", "now/d")), ("compare_range", ("now-61d/d", "now-31d/d"))):
                    if key in by_key:
                        rng = by_key[key]["defaultSelection"]["range"]
                        self.assertEqual(("RANGE", want), (by_key[key]["complexType"], (rng["min"]["value"], rng["max"]["value"])))
                self.assertEqual(spec.CHANNEL_NAMES, [v["value"] for v in by_key["channels"]["defaultSelection"]["values"]["values"]])
                self.assertEqual(spec.CATEGORIES, [v["value"] for v in by_key["categories"]["defaultSelection"]["values"]["values"]])

    def test_non_additive_measures_are_plotted_only_one_row_per_mark(self):
        """비가산 지표(비율 등)는 막대·선의 SUM이 행들을 더하지 않도록 데이터셋이 막대·선 하나당 한 행을 낼 때만 쓴다."""
        non_additive = {"refund_ratio", "unit_price_per_item", "paying_customers", "average_order_value", "refund_amount_share"}
        for page in spec.PAGES:
            for widget in page["widgets"]:
                if widget["kind"] in ("bar", "line") and non_additive & set(widget["y_fields"]):
                    dims = set(DATASETS[widget["dataset"]]["dims"])
                    self.assertEqual({widget["x_field"], *([widget["color_field"]] if widget.get("color_field") else [])},
                                     dims, widget["title"])

    def test_horizontal_bars_are_sorted_largest_first(self):
        for page in spec.PAGES:
            for widget in page["widgets"]:
                if widget["kind"] == "bar":
                    with self.subTest(widget=widget["title"]):
                        self.assertTrue(widget["horizontal"])
                        order = DATASETS[widget["dataset"]]["order"]
                        measure = re.sub(r"_share$", "", widget["y_fields"][0])   # 비중 막대도 금액이 큰 순이다
                        self.assertRegex(order, rf"^{measure} DESC, {widget['x_field']}$")

    def test_trends_are_weekly_lines(self):
        lines = [w for p in spec.PAGES for w in p["widgets"] if w["kind"] == "line"]
        self.assertEqual(3, len(lines))
        for widget in lines:
            self.assertEqual("payment_week", widget["x_field"])
            self.assertEqual(f"payment_week", DATASETS[widget["dataset"]]["order"].split(",")[0])

    def test_channel_colors_are_fixed_for_channel_series(self):
        for page, item in widgets(DASHBOARD):
            encodings = item["widget"]["spec"]["encodings"]
            if "color" in encodings:
                mappings = {m["value"]: m["color"] for m in encodings["color"]["scale"]["mappings"]}
                self.assertEqual({"자사몰": "#2a78d6", "종합 오픈마켓": "#eb6834", "스포츠 전문몰": "#1baf7a"}, mappings)
                self.assertEqual("channel_name", encodings["color"]["fieldName"])

    def test_deltas_use_symbols_not_status_colors(self):
        """증감은 기호와 숫자만 쓴다: 증감 문장 SQL에 색 지정이 없고 위젯 spec에도 색 지정이 없다."""
        text = spec.delta_text("a", "b", "c")
        self.assertIn("▲", text)
        self.assertIn("▼", text)
        for _, item in widgets(DASHBOARD):
            if "compare_value" in json.dumps(item["widget"]["queries"]):
                self.assertNotIn("color", json.dumps(item["widget"]["spec"]))

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

    def test_kpi_cards_are_one_counter_each(self):
        """판매: 큰 카드 3개(총판매금액·순판매금액·환불금액비율) + 작은 카드 6개(환불수량 포함), 환불: 큰 카드 3개. 카드 하나 = counter 위젯 하나."""
        def cards(page, small):
            return [w["value"] for w in next(p for p in spec.PAGES if p["name"] == page)["widgets"]
                    if w["kind"] == "counter" and (w["height"] == 2) == small]
        self.assertEqual(["total_sales", "net_sales", "refund_ratio"], cards("sales", False))
        self.assertEqual(["paying_customers", "average_order_value", "item_count", "unit_price_per_item", "refund_amount",
                          "refund_quantity"], cards("sales", True))
        self.assertEqual(["refund_amount", "refund_quantity", "refund_ratio"], cards("refund", False))
        self.assertEqual(9, sum(w["kind"] == "counter" for w in spec.PAGES[0]["widgets"]))
        self.assertEqual(3, sum(w["kind"] == "counter" for w in spec.PAGES[1]["widgets"]))

    def test_changes_are_collected_in_one_table_per_page(self):
        for page, name in (("sales", "sales_compare"), ("refund", "refund_compare")):
            tables = [w for w in next(p for p in spec.PAGES if p["name"] == page)["widgets"] if w.get("dataset") == name]
            with self.subTest(page=page):
                self.assertEqual(1, len(tables))
                self.assertEqual(["metric", "current_value", "compare_value", "change"], tables[0]["columns"])

    def test_cards_do_not_depend_on_the_comparison_query(self):
        """비교 표 쿼리(CTE)가 실패해도 숫자 카드는 뜬다: 카드 데이터셋은 CTE가 없는 단순 조회다."""
        for page in spec.PAGES:
            for widget in page["widgets"]:
                if widget["kind"] == "counter":
                    self.assertNotIn("WITH", spec.dataset_query(DATASETS[widget["dataset"]]))

    def test_layout_matches_the_approved_design_spec(self):
        """위젯 종류·데이터셋(카드는 열까지)·위치가 명세 배치표와 같다 (시안 https://claude.ai/artifact/CzuDV4PbqLs78GMWGNQKmE)."""
        for page in spec.PAGES:
            got = []
            for w in page["widgets"]:
                if w["kind"] == "filter":
                    key = w["param"]
                elif w["kind"] == "text":
                    key = w["title"]
                elif w["kind"] == "counter":
                    key = f"{w['dataset']}.{w['value']}"
                else:
                    key = w["dataset"]
                got.append((w["kind"], key, w["x"], w["y"], w["width"], w["height"]))
            want = SELECTOR_ROW + [(k, key, x, y + 2, w, h) for k, key, x, y, w, h in SPEC_LAYOUT[page["name"]]]
            with self.subTest(page=page["name"]):
                self.assertEqual(want, got)

    def test_section_titles_are_text_widgets_without_queries(self):
        for page in DASHBOARD["pages"]:
            texts = [i["widget"] for i in page["layout"] if "multilineTextboxSpec" in i["widget"]]
            with self.subTest(page=page["displayName"]):
                self.assertEqual(4, len(texts))
                for widget in texts:
                    self.assertNotIn("queries", widget)
                    self.assertTrue(widget["multilineTextboxSpec"]["lines"][0].startswith("### "))
        first = next(i for i in DASHBOARD["pages"][0]["layout"] if "multilineTextboxSpec" in i["widget"])
        self.assertEqual(["### 핵심 지표", spec.NOTE_COMPARE], first["widget"]["multilineTextboxSpec"]["lines"])

    def test_design_notes_are_shown_under_titles(self):
        notes = [(w["title"], w["note"]) for p in spec.PAGES for w in p["widgets"] if w.get("note")]
        for pair in (("총판매금액", "조회 기간 합계"), ("순판매금액", "총판매금액 − 환불금액"),
                     ("환불금액비율", "낮을수록 좋은 지표입니다."), ("환불금액", "원결제일이 조회 기간에 든 환불"),
                     ("환불수량", "환불된 상품 수량 합계"),
                     ("채널별 총판매금액 추이 · 주별", "첫 주와 마지막 주는 부분 기간일 수 있습니다."),
                     ("결제일별 환불금액 추이 · 원결제일 귀속, 주별", "최근 주는 아직 환불이 접수 중일 수 있어 낮게 보일 수 있습니다.")):
            self.assertIn(pair, notes)
        item = next(i for _, i in widgets(DASHBOARD) if i["widget"]["spec"]["frame"].get("title") == "순판매금액")
        self.assertEqual("총판매금액 − 환불금액", item["widget"]["spec"]["frame"]["description"])

    def test_all_spec_6_5_sales_measures_are_shown_as_kpi(self):
        shown = {w["value"] for p in spec.PAGES for w in p["widgets"] if w["kind"] == "counter"}
        self.assertEqual({n for n, _ in metric_defs.SALES_METRICS["measures"]} | {"refund_quantity"}, shown)

    def test_checked_in_file_matches_definition(self):
        raw = build_dashboard.DEFAULT_PATH.read_bytes()
        self.assertFalse(raw.startswith(b"\xef\xbb\xbf"))
        self.assertEqual(build_dashboard.build_dashboard(), json.loads(raw.decode("utf-8")))
        self.assertIn(b"\r\n", raw)


    def test_screen_guide_contains_every_dataset_query(self):
        """화면 구성 가이드의 SQL이 정의와 같다. 정의를 바꾸면 가이드를 다시 만들어야 한다 (scratch 생성 스크립트가 아니라 이 테스트가 어긋남을 알린다)."""
        guide = (build_dashboard.DEFAULT_PATH.parent / "화면_구성_가이드.md").read_text(encoding="utf-8").replace("\r\n", "\n")
        for dataset in spec.DATASETS:
            with self.subTest(dataset=dataset["name"]):
                self.assertIn(spec.dataset_query(dataset), guide)
        for option in spec.OPTION_DATASETS:
            self.assertIn(option["query"], guide)

if __name__ == "__main__":
    unittest.main()
