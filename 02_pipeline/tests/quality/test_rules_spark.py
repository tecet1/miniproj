"""선언형 규칙을 로컬 Spark에서 실제로 돌려 본다 (pyspark가 없으면 건너뛴다).

행 규칙: 기존 파이썬 검사(validate_values, validate_skus)를 정답으로 삼아 같은 이벤트를 같게 가르는지 차분 비교한다.
집합 규칙: Silver·Gold·Bronze 테이블을 만들고, 위반을 넣었을 때만 건수가 잡히는지, 증분 규칙이 옛 행을 보지 않는지 확인한다.
"""
import itertools
import json
import re
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "02_pipeline" / "quality"), str(ROOT / "02_pipeline" / "silver"),
                str(ROOT / "02_pipeline" / "gold"), str(ROOT / "02_pipeline" / "tests")]

import gold_tables  # noqa: E402
import rules as Q  # noqa: E402
import silver_tables  # noqa: E402
import validate_skus  # noqa: E402
import validate_values  # noqa: E402
from conversion_error import ConversionError  # noqa: E402
from spark_support import local_spark  # noqa: E402

SPARK = local_spark()
RULES = Q.load_rules()
ROW = [r for r in RULES if r["kind"] == "row"]
SET = {r["id"]: r for r in RULES if r["kind"] == "set"}
KNOWN = {"SKU-001", "SKU-002"}
SINCE = "TIMESTAMP '2026-10-14 00:00:00'"
NEW, OLD = "2026-10-14 06:00:00", "2026-10-13 06:00:00"
SCHEMAS = {"bronze": "qb", "silver": "qs", "gold": "qg"}


def python_says_violation(event: dict) -> bool:
    """기존 파이썬 검사가 이 이벤트를 격리하는가."""
    try:
        validate_values.validate_event_values(event)
        validate_skus.validate_known_skus(event, KNOWN)
        return False
    except ConversionError:
        return True


def sample_events():
    """경계값을 조합한 이벤트들 (공통 이벤트 dict)."""
    events = []
    for fee, qty, price, discount, sku in itertools.product((-1, 0, 4000), (-1, 0, 2), (-1, 0, 1000),
                                                            (-500, 0, 1000, 2000, 2500), ("SKU-001", "SKU-999")):
        events.append({"event_type": "ORDER_CREATED", "shipping_fee": fee,
                       "items": [{"order_item_id": "I1", "quantity": qty, "unit_price": price, "item_discount": discount,
                                  "product_id": sku}]})
    events.append({"event_type": "ORDER_CREATED", "shipping_fee": 0, "items": [          # 여러 행 중 한 행만 나쁨
        {"order_item_id": "I1", "quantity": 1, "unit_price": 1000, "item_discount": 0, "product_id": "SKU-001"},
        {"order_item_id": "I2", "quantity": 0, "unit_price": 1000, "item_discount": 0, "product_id": "SKU-002"}]})
    events.append({"event_type": "ORDER_CREATED", "shipping_fee": 0, "items": [
        {"order_item_id": "I1", "quantity": 1, "unit_price": 1000, "item_discount": 0, "product_id": "SKU-001"},
        {"order_item_id": "I2", "quantity": 1, "unit_price": 1000, "item_discount": 0, "product_id": "SKU-002"}]})
    events.append({"event_type": "ORDER_CREATED", "shipping_fee": 0, "items": [          # 같은 order_item_id 두 행
        {"order_item_id": "I1", "quantity": 1, "unit_price": 1000, "item_discount": 0, "product_id": "SKU-001"},
        {"order_item_id": "I1", "quantity": 1, "unit_price": 1000, "item_discount": 0, "product_id": "SKU-002"}]})
    for event_type, amount in itertools.product(("PAYMENT_SUCCEEDED", "PAYMENT_FAILED"), (-1, 0, 5)):
        events.append({"event_type": event_type, "payment_amount": amount})
    for qty, amount in itertools.product((-1, 0, 1), (-1, 0, 5)):
        events.append({"event_type": "REFUND_SUCCEEDED", "refund_quantity": qty, "refund_amount": amount})
    events.append({"event_type": "ORDER_CANCELLED"})
    return events


@unittest.skipUnless(SPARK, "pyspark/Java가 없으면 로컬 Spark 검증을 건너뛴다")
class TestRowRulesAgainstPython(unittest.TestCase):
    def test_spark_rules_agree_with_the_python_checks_on_every_sample_event(self):
        events = sample_events()
        rows = [(i, json.dumps(e)) for i, e in enumerate(events)]
        SPARK.createDataFrame(rows, "id INT, j STRING").createOrReplaceTempView("sample_json")
        SPARK.sql(f"SELECT id, from_json(j, '{silver_tables.EVENT_SCHEMA}') AS e FROM sample_json") \
            .select("id", "e.*").createOrReplaceTempView("sample_events")
        condition = " OR ".join(f"({Q.row_violation_condition(r, KNOWN)})" for r in ROW)
        flagged = {row["id"] for row in SPARK.sql(f"SELECT id FROM sample_events WHERE {condition}").collect()}
        expected = {i for i, e in enumerate(events) if python_says_violation(e)}
        self.assertEqual(expected, flagged)
        self.assertGreater(len(expected), 100)                    # 위반 사례가 충분히 섞여 있다
        self.assertGreater(len(events) - len(expected), 10)       # 정상 사례도 있다

    def test_each_rule_catches_something_the_others_do_not_for_its_own_field(self):
        events = sample_events()
        SPARK.createDataFrame([(i, json.dumps(e)) for i, e in enumerate(events)], "id INT, j STRING") \
            .createOrReplaceTempView("sample_json2")
        SPARK.sql(f"SELECT id, from_json(j, '{silver_tables.EVENT_SCHEMA}') AS e FROM sample_json2") \
            .select("id", "e.*").createOrReplaceTempView("sample_events2")
        for rule in ROW:
            hits = SPARK.sql(f"SELECT COUNT(*) AS n FROM sample_events2 WHERE {Q.row_violation_condition(rule, KNOWN)}").first()["n"]
            self.assertGreater(hits, 0, rule["id"])


BRONZE_EVENTS = {"events": {"columns": [("channel", "STRING"), ("received_at", "TIMESTAMP"), ("payload", "STRING"),
                                        ("eh_partition", "INT"), ("eh_offset", "BIGINT"), ("eh_enqueued_at", "TIMESTAMP"),
                                        ("ingested_at", "TIMESTAMP")]}}
DEFINITIONS = {"qb": BRONZE_EVENTS, "qs": silver_tables.TABLES, "qg": gold_tables.TABLES}
STORE = {}


def stamp(text):
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)   # 세션 시간대(UTC)와 맞춘다


def refresh(schema, table):
    """테이블 행 목록을 임시 뷰 {schema}_{table}로 등록한다. (로컬 Windows Spark는 파일 테이블을 쓸 수 없어 뷰로 흉내 낸다)"""
    columns = DEFINITIONS[schema][table]["columns"]
    ddl = ", ".join(f"{name} {sql_type}" for name, sql_type in columns)
    rows = [tuple(row.get(name) for name, _ in columns) for row in STORE[(schema, table)]]
    SPARK.createDataFrame(rows, ddl).createOrReplaceTempView(f"{schema}_{table}")


def insert(schema, table, **values):
    STORE[(schema, table)].append(values)
    refresh(schema, table)


def violations(rule_id):
    sql = Q.render_set_sql(SET[rule_id], SCHEMAS, SINCE)
    sql = re.sub(r"\b(qb|qs|qg)\.(\w+)", r"\1_\2", sql)       # 스키마.테이블 -> 뷰 이름
    return SPARK.sql(sql).first()["violations"]


@unittest.skipUnless(SPARK, "pyspark/Java가 없으면 로컬 Spark 검증을 건너뛴다")
class TestSetRules(unittest.TestCase):
    def setUp(self):
        STORE.clear()
        for schema, tables in DEFINITIONS.items():
            for table in tables:
                STORE[(schema, table)] = []
                refresh(schema, table)

    def test_every_shipped_set_rule_runs_and_is_zero_on_empty_tables(self):
        for rule_id in SET:
            self.assertEqual(0, violations(rule_id), rule_id)

    def test_duplicate_event_keys_are_caught_only_when_the_rows_are_new(self):
        for table, rule_id in (("orders", "silver_orders_pk_unique"), ("payments", "silver_payments_pk_unique"),
                               ("refunds", "silver_refunds_pk_unique")):
            for received_at, expected in ((OLD, 0), (NEW, 1)):
                STORE[("qs", table)] = []
                for _ in range(2):
                    insert("qs", table, channel="A", event_id="E1", order_id="O1", received_at=stamp(received_at))
                self.assertEqual(expected, violations(rule_id), f"{rule_id} {received_at}")

    def test_distinct_event_ids_are_not_duplicates(self):
        insert("qs", "orders", channel="A", event_id="E1", order_id="O1", received_at=stamp(NEW))
        insert("qs", "orders", channel="A", event_id="E2", order_id="O2", received_at=stamp(NEW))
        insert("qs", "orders", channel="B", event_id="E1", order_id="O3", received_at=stamp(NEW))
        self.assertEqual(0, violations("silver_orders_pk_unique"))

    def test_refund_pointing_to_a_missing_item_is_flagged_and_a_present_one_is_not(self):
        insert("qs", "refunds", channel="A", event_id="R1", refund_id="RF1", order_item_id="I1", received_at=stamp(NEW))
        self.assertEqual(1, violations("silver_refund_points_to_item"))
        insert("qs", "order_items", channel="A", order_item_id="I1", order_id="O1")
        self.assertEqual(0, violations("silver_refund_points_to_item"))
        insert("qs", "refunds", channel="A", event_id="R2", refund_id="RF2", order_item_id="I9", received_at=stamp(OLD))
        self.assertEqual(0, violations("silver_refund_points_to_item"))        # 옛 행은 증분에서 보지 않는다
        insert("qs", "order_items", channel="B", order_item_id="I2", order_id="O2")
        insert("qs", "refunds", channel="A", event_id="R3", refund_id="RF3", order_item_id="I2", received_at=stamp(NEW))
        self.assertEqual(1, violations("silver_refund_points_to_item"))        # 다른 채널의 같은 ID는 참조가 아니다

    def test_quarantine_must_keep_the_bronze_original(self):
        insert("qb", "events", channel="A", payload="{x}", eh_partition=0, eh_offset=1)
        insert("qs", "quarantine", channel="A", payload="{x}", eh_partition=0, eh_offset=1, ingested_at=stamp(NEW))
        self.assertEqual(0, violations("silver_quarantine_keeps_original"))
        insert("qs", "quarantine", channel="A", payload="{changed}", eh_partition=0, eh_offset=1, ingested_at=stamp(NEW))
        self.assertEqual(1, violations("silver_quarantine_keeps_original"))

    def test_full_scope_rules_see_old_rows_too(self):
        for _ in range(2):
            insert("qs", "order_items", channel="A", order_item_id="I1", order_id="O1")
            insert("qg", "fact_sales_item", channel="A", order_item_id="I1", product_id="P1")
            insert("qg", "fact_refund", channel="A", refund_id="RF1")
        self.assertEqual(1, violations("silver_order_items_pk_unique"))
        self.assertEqual(1, violations("gold_fact_sales_item_pk_unique"))
        self.assertEqual(1, violations("gold_fact_refund_pk_unique"))

    def test_fact_product_must_exist_in_the_dimension(self):
        insert("qg", "fact_sales_item", channel="A", order_item_id="I1", product_id="P1")
        insert("qg", "fact_sales_item", channel="A", order_item_id="I2", product_id=None)
        self.assertEqual(1, violations("gold_sales_item_product_exists"))
        insert("qg", "dim_product", product_id="P1", product_name="n", category="c")
        self.assertEqual(0, violations("gold_sales_item_product_exists"))


@unittest.skipUnless(SPARK, "pyspark/Java가 없으면 로컬 Spark 검증을 건너뛴다")
class TestRulesMovedFromTheOldChecks(unittest.TestCase):
    """check_silver·check_gold에서 규칙 목록으로 옮긴 검사(계획 07 카드 7-C)가 위반을 잡고 정상은 통과시키는지 본다."""

    def setUp(self):
        STORE.clear()
        for schema, tables in DEFINITIONS.items():
            for table in tables:
                STORE[(schema, table)] = []
                refresh(schema, table)

    def order_with_payment(self, shipping=4000, items_total=2000, amount=6000, order_id="O1"):
        insert("qs", "orders", channel="A", event_id=f"{order_id}-e", order_id=order_id, user_id="U", shipping_fee=shipping, received_at=stamp(NEW))
        insert("qs", "order_items", channel="A", order_item_id=f"{order_id}-1", order_id=order_id, quantity=2, unit_price=items_total // 2, item_discount=0)
        insert("qs", "payments", channel="A", event_id=f"{order_id}-p", order_id=order_id, status="SUCCEEDED", payment_amount=amount, received_at=stamp(NEW))
        insert("qg", "fact_sales_item", channel="A", order_item_id=f"{order_id}-1", order_id=order_id, item_paid_amount=items_total)

    def test_every_moved_rule_exists_and_is_clean_on_empty_tables(self):
        for rule_id in ("silver_order_cancellations_pk_unique", "silver_payment_amount_matches_items", "gold_payment_amount_matches_items",
                        "gold_shipping_fee_rule", "gold_refund_amount_matches_between_facts", "gold_refund_quantity_matches_between_facts",
                        "gold_fact_sales_item_channel_in_dim_channel", "gold_fact_refund_refund_profile_key_in_dim_refund_profile"):
            self.assertEqual(0, violations(rule_id), rule_id)

    def test_cancellation_event_id_duplicate_is_caught_in_the_window_only(self):
        for received_at, expected in ((OLD, 0), (NEW, 1)):
            STORE[("qs", "order_cancellations")] = []
            for _ in range(2):
                insert("qs", "order_cancellations", channel="A", event_id="C1", order_id="O1", received_at=stamp(received_at))
            self.assertEqual(expected, violations("silver_order_cancellations_pk_unique"), received_at)

    def test_silver_payment_amount_must_equal_items_plus_shipping(self):
        self.order_with_payment(amount=6000)
        self.assertEqual(0, violations("silver_payment_amount_matches_items"))
        self.order_with_payment(amount=5999, order_id="O2")
        self.assertEqual(1, violations("silver_payment_amount_matches_items"))

    def test_failed_payments_are_not_compared_with_the_order_amount(self):
        self.order_with_payment()
        insert("qs", "payments", channel="A", event_id="O1-pf", order_id="O1", status="FAILED", payment_amount=1, received_at=stamp(NEW))
        self.assertEqual(0, violations("silver_payment_amount_matches_items"))

    def test_gold_payment_amount_and_shipping_rules(self):
        self.order_with_payment(shipping=4000, items_total=10000, amount=14000)
        self.assertEqual((0, 0), (violations("gold_payment_amount_matches_items"), violations("gold_shipping_fee_rule")))
        self.order_with_payment(shipping=0, items_total=25000, amount=25000, order_id="O2")
        self.assertEqual((0, 0), (violations("gold_payment_amount_matches_items"), violations("gold_shipping_fee_rule")))
        self.order_with_payment(shipping=4000, items_total=25000, amount=29000, order_id="O3")      # 금액은 맞지만 배송비 규칙 위반
        self.assertEqual((0, 1), (violations("gold_payment_amount_matches_items"), violations("gold_shipping_fee_rule")))
        self.order_with_payment(shipping=4000, items_total=10000, amount=13000, order_id="O4")      # 배송비 규칙은 맞지만 금액 불일치
        self.assertEqual((1, 1), (violations("gold_payment_amount_matches_items"), violations("gold_shipping_fee_rule")))

    def test_refund_totals_must_match_between_the_two_facts(self):
        insert("qg", "fact_sales_item", channel="A", order_item_id="I1", refund_amount=500, refund_quantity=1)
        insert("qg", "fact_refund", channel="A", refund_id="R1", refund_amount=500, refund_quantity=1)
        self.assertEqual((0, 0), (violations("gold_refund_amount_matches_between_facts"), violations("gold_refund_quantity_matches_between_facts")))
        insert("qg", "fact_refund", channel="A", refund_id="R2", refund_amount=100, refund_quantity=0)
        self.assertEqual((1, 0), (violations("gold_refund_amount_matches_between_facts"), violations("gold_refund_quantity_matches_between_facts")))
        insert("qg", "fact_refund", channel="A", refund_id="R3", refund_amount=0, refund_quantity=2)
        self.assertEqual((1, 1), (violations("gold_refund_amount_matches_between_facts"), violations("gold_refund_quantity_matches_between_facts")))

    def test_fact_keys_must_exist_in_the_dimensions_and_null_is_not_a_missing_key(self):
        insert("qg", "dim_channel", channel="A", channel_name="x")
        insert("qg", "dim_product", product_id="P1")
        insert("qg", "fact_sales_item", channel="A", order_item_id="I1", product_id="P1", payment_method=None)
        self.assertEqual(0, violations("gold_fact_sales_item_channel_in_dim_channel"))
        self.assertEqual(0, violations("gold_fact_sales_item_product_id_in_dim_product"))
        self.assertEqual(0, violations("gold_fact_sales_item_payment_method_in_dim_payment_method"))      # NULL은 누락이 아니다
        insert("qg", "fact_sales_item", channel="Z", order_item_id="I2", product_id="P9")
        self.assertEqual(1, violations("gold_fact_sales_item_channel_in_dim_channel"))
        self.assertEqual(1, violations("gold_fact_sales_item_product_id_in_dim_product"))
        insert("qg", "fact_refund", channel="A", refund_id="R1", product_id="P1", refund_profile_key="RETURN-NOPE")
        self.assertEqual(1, violations("gold_fact_refund_refund_profile_key_in_dim_refund_profile"))
        self.assertEqual(0, violations("gold_fact_refund_channel_in_dim_channel"))


if __name__ == "__main__":
    unittest.main()
