"""Gold 테이블이 독립 기대값(expected.json)과 맞는지 확인한다. 노트북에서 호출만 한다.

[입력] Silver·Gold 스키마 이름(예: "commerce_poc.silver", "commerce_poc.gold"), expected.json 경로
[출력] 문제 문장 목록. 빈 목록이면 모두 일치한다.
[확인 항목] (plans/04 완료 조건)
  1. 테이블별 건수 = expected.json (fact_sales_item, fact_refund total, Dimension 행 수는 "dimensions")
  2. 지표 = expected.json 채널별·total: 총판매금액, 환불금액, 환불수량, 순판매금액, 상품결제건수, 결제고객수,
     환불금액비율, 상품결제단가, 객단가 (정의는 event_spec.md 6.5절)
  3. 분포 = 결제일(KST)·결제수단·환불 프로필·환불일별 건수
  4. 대조: fact_refund 합계 = fact_sales_item의 환불 합계 (금액·수량), 성공 결제금액 = 상품 결제금액 합 + 배송비,
     배송비 규칙 (2만 원 이상 0원, 미만 4,000원)
  5. PK 중복 0건, Dimension 키 누락 0건
비교 함수는 pyspark 없이 로컬에서 테스트한다. Spark 조회 함수는 Databricks에서만 돈다 (미검증).
"""
import json
import math
from pathlib import Path

from gold_tables import TABLES

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_EXPECTED = ROOT / "01_contracts" / "data_fixtures" / "expected" / "expected.json"
CHANNELS = ("A", "B", "C")
RAW_KEYS = ("total_sales", "refund_amount", "refund_quantity", "paid_item_count", "paying_customers")
# 분포 항목 -> (Gold 테이블, 키 컬럼). 이름은 expected.json의 키와 같다.
DISTRIBUTIONS = {
    "sales_item_by_payment_date": ("fact_sales_item", "payment_date"),
    "sales_item_by_payment_method": ("fact_sales_item", "payment_method"),
    "refund_by_payment_date": ("fact_refund", "payment_date"),
    "refund_by_refund_date": ("fact_refund", "refund_date"),
    "refund_by_profile": ("fact_refund", "refund_profile_key"),
}
# Fact의 Dimension 참조: (Fact, Fact 컬럼, Dimension, Dimension 키)
REFERENCES = [
    ("fact_sales_item", "channel", "dim_channel", "channel"),
    ("fact_sales_item", "product_id", "dim_product", "product_id"),
    ("fact_sales_item", "payment_method", "dim_payment_method", "payment_method"),
    ("fact_sales_item", "payment_date", "dim_date", "date"),
    ("fact_sales_item", "last_refund_date", "dim_date", "date"),
    ("fact_refund", "channel", "dim_channel", "channel"),
    ("fact_refund", "product_id", "dim_product", "product_id"),
    ("fact_refund", "payment_date", "dim_date", "date"),
    ("fact_refund", "refund_date", "dim_date", "date"),
    ("fact_refund", "refund_profile_key", "dim_refund_profile", "refund_profile_key"),
]


# ── 계산·비교 (pyspark 없음) ────────────────────────────────────────────────────────
def expected_row_counts(expected: dict) -> dict:
    """expected.json 전체에서 테이블별 기대 건수를 만든다."""
    return {"fact_sales_item": expected["total"]["fact_sales_item"],
            "fact_refund": expected["total"]["fact_refund"], **expected["dimensions"]}


def derive_metrics(raw: dict) -> dict:
    """합계 5개(RAW_KEYS)에서 지표 9개를 만든다 (event_spec.md 6.5절). 분모가 0이면 None."""
    sales, count, customers = raw["total_sales"], raw["paid_item_count"], raw["paying_customers"]
    return {**raw, "net_sales": sales - raw["refund_amount"],
            "refund_ratio": raw["refund_amount"] / sales if sales else None,
            "unit_price_per_item": sales / count if count else None,
            "average_order_value": sales / customers if customers else None}


def with_total(per_channel: dict) -> dict:
    """채널별 합계에 "total"을 더한다. 결제고객수는 채널+user_id로 구분하므로 채널별 값의 합이다."""
    return {**per_channel, "total": {key: sum(raw[key] for raw in per_channel.values()) for key in RAW_KEYS}}


def compare_equal(checks: dict) -> list[str]:
    """checks = {이름: (실제, 기대)}. 다른 것만 문제로 돌려준다."""
    return [f"{name}: 기대 {expected}, 실제 {actual}" for name, (actual, expected) in checks.items()
            if actual != expected]


def compare_zero(label: str, counts: dict) -> list[str]:
    """{이름: 건수} 중 0이 아닌 것을 문제로 돌려준다 (PK 중복, 키 누락, 대조 불일치)."""
    return [f"{label} {name}: {n}건" for name, n in counts.items() if n != 0]


def _same(actual, expected) -> bool:
    if actual is None or expected is None:
        return actual is expected
    return math.isclose(actual, expected, rel_tol=1e-9, abs_tol=0.0)


def compare_metrics(actual: dict, expected: dict) -> list[str]:
    """actual = {채널 또는 "total": 지표 dict}를 expected.json의 per_channel·total과 비교한다."""
    problems = []
    for scope, metrics in actual.items():
        want = expected["total"] if scope == "total" else expected["per_channel"][scope]
        for key in (*RAW_KEYS, "net_sales", "refund_ratio", "unit_price_per_item", "average_order_value"):
            if not _same(metrics[key], want[key]):
                problems.append(f"지표 {scope} {key}: 기대 {want[key]}, 실제 {metrics[key]}")
    return problems


def compare_distribution(name: str, rows: dict, expected: dict) -> list[str]:
    """rows = {(채널, 키): 건수}를 채널별·total로 접어 expected.json의 분포와 비교한다."""
    per_channel = {channel: {} for channel in CHANNELS}
    total = {}
    for (channel, key), n in rows.items():
        per_channel.setdefault(channel, {})[key] = n
        total[key] = total.get(key, 0) + n
    problems = []
    for channel in CHANNELS:
        if per_channel[channel] != expected["per_channel"][channel][name]:
            problems.append(f"{name} {channel}: 기대 {expected['per_channel'][channel][name]}, 실제 {per_channel[channel]}")
    if total != expected["total"][name]:
        problems.append(f"{name} total: 기대 {expected['total'][name]}, 실제 {total}")
    return problems


# ── Spark 조회 (Databricks에서만 실행) ───────────────────────────────────────────────
def count_tables(spark, gold: str) -> dict:
    return {table: spark.table(f"{gold}.{table}").count() for table in TABLES}


def sales_item_raw(spark, gold: str) -> dict:
    """fact_sales_item의 채널별 합계 5개. 행이 없는 채널은 0으로 채운다."""
    rows = spark.sql(
        f"SELECT channel, SUM(item_paid_amount) AS total_sales, SUM(refund_amount) AS refund_amount, "
        f"SUM(refund_quantity) AS refund_quantity, COUNT(*) AS paid_item_count, "
        f"COUNT(DISTINCT user_id) AS paying_customers FROM {gold}.fact_sales_item GROUP BY channel").collect()
    found = {row["channel"]: {key: row[key] for key in RAW_KEYS} for row in rows}
    return {channel: found.get(channel, dict.fromkeys(RAW_KEYS, 0)) for channel in CHANNELS}


def refund_totals(spark, gold: str) -> dict:
    """fact_refund의 환불금액·환불수량 합계 (행이 없으면 0)."""
    row = spark.sql(f"SELECT COALESCE(SUM(refund_amount), 0) AS refund_amount, "
                    f"COALESCE(SUM(refund_quantity), 0) AS refund_quantity FROM {gold}.fact_refund").first()
    return {"refund_amount": row["refund_amount"], "refund_quantity": row["refund_quantity"]}


def count_distribution(spark, gold: str, table: str, column: str) -> dict:
    """{(채널, 키): 건수}. 일자는 yyyy-MM-dd 문자열이다."""
    rows = spark.sql(f"SELECT channel, CAST({column} AS STRING) AS k, COUNT(*) AS n "
                     f"FROM {gold}.{table} GROUP BY channel, {column}").collect()
    return {(row["channel"], row["k"]): row["n"] for row in rows}


def count_pk_duplicates(spark, gold: str) -> dict:
    """PK가 겹치는 키 수. 테이블마다 0이어야 한다."""
    result = {}
    for table, definition in TABLES.items():
        keys = ", ".join(definition["pk"])
        result[table] = spark.sql(f"SELECT COUNT(*) AS n FROM (SELECT {keys} FROM {gold}.{table} "
                                  f"GROUP BY {keys} HAVING COUNT(*) > 1)").first()["n"]
    return result


def count_missing_dimension_keys(spark, gold: str) -> dict:
    """Dimension에 없는 키를 가진 Fact 행 수 (NULL은 제외)."""
    result = {}
    for fact, column, dim, key in REFERENCES:
        result[f"{fact}.{column}->{dim}"] = spark.sql(
            f"SELECT COUNT(*) AS n FROM {gold}.{fact} f LEFT ANTI JOIN {gold}.{dim} d ON f.{column} = d.{key} "
            f"WHERE f.{column} IS NOT NULL").first()["n"]
    return result


def count_payment_mismatches(spark, silver: str, gold: str) -> dict:
    """Fact에 있는 주문 중 성공 결제금액 != 상품 결제금액 합 + 배송비인 주문 수, 배송비 규칙을 어긴 주문 수."""
    base = (f"SELECT COUNT(*) AS n FROM (SELECT channel, order_id, SUM(item_paid_amount) AS items_total "
            f"FROM {gold}.fact_sales_item GROUP BY channel, order_id) g "
            f"JOIN {silver}.orders o ON o.channel = g.channel AND o.order_id = g.order_id "
            f"JOIN {silver}.payments p ON p.channel = g.channel AND p.order_id = g.order_id AND p.status = 'SUCCEEDED' ")
    return {
        "결제금액 != 상품합+배송비": spark.sql(base + "WHERE p.payment_amount <> g.items_total + o.shipping_fee").first()["n"],
        "배송비 규칙 위반": spark.sql(
            base + "WHERE o.shipping_fee <> CASE WHEN g.items_total >= 20000 THEN 0 ELSE 4000 END").first()["n"],
    }


def check_gold(spark, silver_schema: str, gold_schema: str, expected_path=DEFAULT_EXPECTED) -> list[str]:
    """Gold 전체를 기대값과 비교해 문제 문장 목록을 돌려준다. 빈 목록이면 일치한다."""
    expected = json.loads(Path(expected_path).read_text(encoding="utf-8"))
    counts, want = count_tables(spark, gold_schema), expected_row_counts(expected)
    raw = with_total(sales_item_raw(spark, gold_schema))
    refund = refund_totals(spark, gold_schema)
    reconcile = {"환불금액 fact_refund vs fact_sales_item": (refund["refund_amount"], raw["total"]["refund_amount"]),
                 "환불수량 fact_refund vs fact_sales_item": (refund["refund_quantity"], raw["total"]["refund_quantity"])}
    problems = (compare_equal({f"{table} 건수": (counts[table], n) for table, n in want.items()})
                + compare_metrics({scope: derive_metrics(r) for scope, r in raw.items()}, expected)
                + compare_equal(reconcile))
    for name, (table, column) in DISTRIBUTIONS.items():
        problems += compare_distribution(name, count_distribution(spark, gold_schema, table, column), expected)
    return (problems
            + compare_zero("PK 중복", count_pk_duplicates(spark, gold_schema))
            + compare_zero("Dimension 키 누락", count_missing_dimension_keys(spark, gold_schema))
            + compare_zero("대조 불일치", count_payment_mismatches(spark, silver_schema, gold_schema)))
