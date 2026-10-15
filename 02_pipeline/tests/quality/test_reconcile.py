"""Bronze 재계산 대조(quality/reconcile_sql.py, reconcile_rules.json)를 로컬 Spark에서 확인한다 (pyspark가 없으면 건너뛴다).

[방법] 시나리오 전체(오류 사례 S24~S28, 격리 대상 오류 포함, 생성기로 만든 줄)와 오류를 높은 비율로 주입한 합성 데이터를 Bronze 행으로 만들고,
  (1) 같은 행을 파이썬 파이프라인(convert_row + Silver 행 만들기 + Gold SQL)으로 처리한 결과를 "실제" Silver·Gold 테이블로 등록한다.
  (2) reconcile_sql이 만든 SQL이 Bronze payload만 보고 다시 계산한 결과(recon_* 뷰)와 비교한 차이 건수가 모든 규칙에서 0인지 본다.
  (3) 실제 테이블을 일부러 틀리게 만들면(행 삭제, 금액 변경, 행 추가) 해당 규칙이 차이를 잡는지 본다 (변이 확인).
[확인하지 못하는 것] Databricks(Delta, ANSI 모드)에서의 실행. 로컬 Spark와 파이썬 파이프라인이 같은 결과를 내는 것까지만 본다.
"""
import json
import re
import sys
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "02_pipeline" / "quality"), str(ROOT / "02_pipeline" / "silver"), str(ROOT / "02_pipeline" / "gold"),
                str(ROOT / "02_pipeline" / "tests" / "gold"), str(ROOT / "02_pipeline" / "tests"),
                str(ROOT / "04_experiments" / "src"), str(ROOT / "01_contracts" / "src")]

import _silver_rows as sr  # noqa: E402
import generate  # noqa: E402
import gold_sql  # noqa: E402
import gold_tables  # noqa: E402
import reconcile_sql as RS  # noqa: E402
import rules as Q  # noqa: E402
import scenario_loader  # noqa: E402
import silver_tables  # noqa: E402
import synth_scenarios as synth  # noqa: E402
from ids import NamedIds  # noqa: E402
from spark_support import local_spark  # noqa: E402

SPARK = local_spark()
UTC = timezone.utc
SCHEMAS = {"bronze": "qb", "silver": "qs", "gold": "qg"}
KNOWN = sr.KNOWN
RULES_PATH = ROOT / "02_pipeline" / "quality" / "reconcile_rules.json"
CONFIG = json.loads((ROOT / "04_experiments" / "configs" / "dump.json").read_text(encoding="utf-8"))
HIGH_RATES = {"fault": 0.03, "double_payment": 0.03, "duplicate_order_created": 0.03, "refund_to_failed_payment": 0.2,
              "refund_order_mismatch": 0.2, "duplicate_item_id": 0.05}


# 채널별 필드 이름 (이벤트 명세 2.2절). 손으로 만든 변형 이벤트를 만들 때 쓴다.
FIELDS = {
    "A": {"code": "event_type", "id": "event_id", "order_key": "order_id", "cancel_code": "order.cancelled", "order_code": "order.created", "pay_code": "payment.succeeded", "refund_code": "refund.succeeded",
          "items": "items", "sku": "sku", "qty": "quantity", "discount": (None, "discount_amount"), "shipping": (None, "shipping_fee"),
          "method": "method", "amount": "amount", "refund_type": "type", "refund_qty": "refund_quantity", "user": (None, "user_id")},
    "B": {"code": "eventName", "id": "eventId", "order_key": "orderNo", "cancel_code": "ORDER_CANCELED", "order_code": "ORDER_PLACED", "pay_code": "PAY_APPROVED", "refund_code": "REFUND_DONE",
          "items": "orderItems", "sku": "sellerSku", "qty": "qty", "discount": ("discount", "total"), "shipping": ("delivery", "fee"),
          "method": "payMethod", "amount": "paidAmount", "refund_type": "claimType", "refund_qty": "refundQty", "user": ("buyer", "memberId")},
    "C": {"code": "evt_cd", "id": "evt_id", "order_key": "ord_no", "cancel_code": "O09", "order_code": "O01", "pay_code": "P01", "refund_code": "R01",
          "items": "lines", "sku": "goods_cd", "qty": "qty", "discount": (None, "dc_amt"), "shipping": (None, "dlv_amt"),
          "method": "pay_type", "amount": "pay_amt", "refund_type": "clm_tp", "refund_qty": "rfd_qty", "user": (None, "mem_no")},
}


def _set(container, path, value):
    """path = (바깥 키 또는 None, 안쪽 키). 바깥 키가 있으면 그 객체 안의 안쪽 키를 바꾼다. value가 DELETE면 지운다."""
    outer, inner = path
    target = container if outer is None else container[outer]
    key = inner
    if value is DELETE:
        target.pop(key, None)
    else:
        target[key] = value


DELETE = object()


def scenario_lines(channel):
    """시나리오(scenarios.json)에서 생성기가 만든 채널 원본 줄. data_fixtures 파일을 읽지 않고 같은 생성기로 직접 만든다."""
    skus = scenario_loader.load_skus(ROOT / "01_contracts" / "contracts" / "skus.json")
    scenarios = scenario_loader.load_scenarios(ROOT / "01_contracts" / "contracts" / "scenarios.json")
    return generate.build_lines(channel, scenarios, skus)


def crafted_lines():
    """채널마다 정상 이벤트 하나씩을 골라 필드를 하나씩 망가뜨린 변형 줄을 만든다 (Silver가 격리하는지·받아들이는지 경계를 맞춰 본다)."""
    out = []
    base = {}
    for channel in "ABC":
        names = FIELDS[channel]
        for line in scenario_lines(channel):
            raw = json.loads(json.loads(line)["payload"])
            for kind, code in (("order", names["order_code"]), ("pay", names["pay_code"]), ("refund", names["refund_code"]), ("cancel", names["cancel_code"])):
                if raw[names["code"]] == code and ({"refund": "S09", "cancel": "S08"}.get(kind, "S04") in raw[names["id"]]):
                    base.setdefault((channel, kind), raw)
    counter = 0
    for channel in "ABC":
        n = FIELDS[channel]
        variants = [
            ("pay", "missing method", lambda r: _set(r, (None, n["method"]), DELETE)),
            ("pay", "unmapped method", lambda r: _set(r, (None, n["method"]), "bitcoin")),
            ("pay", "negative amount", lambda r: _set(r, (None, n["amount"]), -5)),
            ("pay", "zero amount", lambda r: _set(r, (None, n["amount"]), 0)),
            ("order", "unknown sku", lambda r: _set(r[n["items"]][0], (None, n["sku"]), "SKU-999")),
            ("order", "zero quantity", lambda r: _set(r[n["items"]][0], (None, n["qty"]), 0)),
            ("order", "text quantity", lambda r: _set(r[n["items"]][0], (None, n["qty"]), "N/A")),
            ("order", "huge discount", lambda r: _set(r[n["items"]][0], n["discount"], 10 ** 9)),
            ("order", "empty items", lambda r: r.__setitem__(n["items"], [])),
            ("order", "missing items", lambda r: r.pop(n["items"])),
            ("order", "negative shipping", lambda r: _set(r, n["shipping"], -1)),
            ("order", "missing event id", lambda r: r.pop(n["id"])),
            ("order", "empty user", lambda r: _set(r, n["user"], "")),
            ("order", "missing user", lambda r: _set(r, n["user"], DELETE)),
            ("cancel", "cancel of a paid order (S01, 다른 변형이 건드리지 않은 주문)", lambda r: r.__setitem__(n["order_key"], f"{channel}-S01-1")),
            ("refund", "unknown refund type", lambda r: _set(r, (None, n["refund_type"]), "weird")),
            ("refund", "zero refund quantity", lambda r: _set(r, (None, n["refund_qty"]), 0)),
        ]
        for kind, label, mutate in variants:
            raw = json.loads(json.dumps(base[(channel, kind)]))
            mutate(raw)
            counter += 1
            if n["id"] in raw:
                raw[n["id"]] = f"CRAFTED-{channel}-{counter}"
            out.append({"channel": channel, "payload": json.dumps(raw, ensure_ascii=False)})
    return out


class TestRuleFile(unittest.TestCase):
    def test_the_rule_file_is_what_reconcile_sql_generates(self):
        self.assertEqual(RS.rule_list(), Q.load_rules(RULES_PATH))

    def test_rules_are_full_scope_set_rules_with_error_severity(self):
        rules = Q.load_rules(RULES_PATH)
        self.assertEqual({"set"}, {r["kind"] for r in rules})
        self.assertEqual({"full"}, {r["scope"] for r in rules})
        self.assertEqual({"error"}, {r["severity"] for r in rules})
        self.assertEqual(0, max(r.get("max", 0) for r in rules))

    def test_every_silver_event_table_and_both_facts_and_quarantine_are_covered(self):
        ids = {r["id"] for r in Q.load_rules(RULES_PATH)}
        for table in ("orders", "order_items", "payments", "order_cancellations", "refunds", "quarantine"):
            self.assertIn(f"reconcile_silver_{table}", ids)
        for table in ("fact_sales_item", "fact_refund"):
            self.assertIn(f"reconcile_gold_{table}", ids)

    def test_the_sql_does_not_import_or_name_the_pipeline_modules(self):
        text = (ROOT / "02_pipeline" / "quality" / "reconcile_sql.py").read_text(encoding="utf-8")
        for name in ("channel_a", "channel_b", "channel_c", "convert_row", "silver_row_to_event", "validate_values", "gold_sql"):
            self.assertNotIn(f"import {name}", text)
            self.assertNotIn(f"from {name}", text)


def to_python(value, sql_type):
    """sqlite·문자열 값을 Spark 열 타입에 맞는 파이썬 값으로 바꾼다."""
    if value is None:
        return None
    if sql_type == "TIMESTAMP":
        return datetime.fromisoformat(str(value).replace("Z", "+00:00").replace(" ", "T")).replace(tzinfo=UTC) \
            if not str(value).endswith("+00:00") else datetime.fromisoformat(str(value))
    if sql_type == "DATE":
        return date.fromisoformat(str(value)[:10])
    return value


def register(prefix, table, columns, rows):
    """rows(dict 목록)를 임시 뷰 {prefix}_{table}로 등록한다. columns = [(이름, 타입)]."""
    ddl = ", ".join(f"{name} {sql_type}" for name, sql_type in columns)
    data = [tuple(to_python(row.get(name), sql_type) for name, sql_type in columns) for row in rows]
    SPARK.createDataFrame(data, ddl).createOrReplaceTempView(f"{prefix}_{table}")


def rewrite(sql):
    return re.sub(r"\b(qs|qg)\.(\w+)", r"\1_\2", sql)


def violations(rule):
    sql = rewrite(Q.render_set_sql(rule, SCHEMAS))
    return SPARK.sql(sql).first()["violations"]


@unittest.skipUnless(SPARK, "pyspark/Java가 없으면 로컬 Spark 검증을 건너뛴다")
class TestAgainstThePythonPipeline(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        skus = scenario_loader.load_skus(ROOT / "01_contracts" / "contracts" / "skus.json")
        lines = []
        for channel in "ABC":
            lines += [json.loads(l) for l in scenario_lines(channel)]
        sample = synth.build_scenarios(skus, dict(CONFIG, start="2025-11-03", end="2025-11-04", error_rates=HIGH_RATES, base_orders_per_day=40))
        for channel in "ABC":
            lines += [json.loads(l) for l in generate.build_lines(channel, sample, skus, NamedIds(channel))]
        lines += crafted_lines()
        lines += [{"channel": "A", "payload": "not json"}, {"channel": "X", "payload": "{}"}, {"channel": "B", "payload": "{\"eventName\": \"UNKNOWN\"}"}]
        base = datetime(2026, 10, 12, tzinfo=UTC)
        cls.bronze = [{"channel": l["channel"], "payload": l["payload"], "received_at": base + timedelta(seconds=i), "eh_partition": 0, "eh_offset": i}
                      for i, l in enumerate(lines)]
        SPARK.createDataFrame([(r["channel"], r["payload"], r["received_at"], r["eh_partition"], r["eh_offset"]) for r in cls.bronze],
                              "channel string, payload string, received_at timestamp, eh_partition int, eh_offset bigint") \
            .createOrReplaceTempView("qb_events")
        cls.build_reference()
        RS.create_views(SPARK, "qb_events", KNOWN)

    @classmethod
    def build_reference(cls):
        """같은 Bronze 행을 파이썬 파이프라인으로 처리해 Silver·Gold 행을 만든다 (convert_row, Silver 행 나누기, gold_sql)."""
        kept, seen, cls.rejected = [], set(), []
        for row in cls.bronze:
            result = sr.convert_row(row["channel"], row["payload"], KNOWN)
            if result["reason_code"]:
                cls.rejected.append(row["eh_offset"])
            elif (row["channel"], result["event_id"]) not in seen:
                seen.add((row["channel"], result["event_id"]))
                kept.append(json.loads(result["event_json"]))
        cls.silver = sr.to_silver(kept)
        con = sr.connect(cls.silver)
        cls.gold = {"fact_sales_item": sr.run(con, gold_sql.sales_item_sql("silver", sr.sqlite_date)),
                    "fact_refund": sr.run(con, gold_sql.refund_sql("silver", sr.sqlite_date))}
        con.close()

    def setUp(self):
        for table, definition in silver_tables.TABLES.items():
            if table != "quarantine":
                register("qs", table, definition["columns"], self.silver[table])
        register("qs", "quarantine", [("eh_partition", "INT"), ("eh_offset", "BIGINT")],
                 [{"eh_partition": 0, "eh_offset": offset} for offset in self.rejected])
        for table in ("fact_sales_item", "fact_refund"):
            register("qg", table, gold_tables.TABLES[table]["columns"], self.gold[table])

    def rule(self, rule_id):
        return next(r for r in Q.load_rules(RULES_PATH) if r["id"] == rule_id)

    def count(self, sql):
        return SPARK.sql(sql).first()["n"]

    # ── 데이터가 비어 있지 않고 오류 사례를 모두 담고 있다 ─────────────────────────────────
    def test_the_data_contains_every_kind_of_trouble(self):
        self.assertGreater(len(self.rejected), 10)
        self.assertGreater(self.count("SELECT COUNT(*) AS n FROM recon_order_stats WHERE n_created > 1"), 0)
        self.assertGreater(self.count("SELECT COUNT(*) AS n FROM recon_order_stats WHERE n_paid > 1"), 0)
        self.assertGreater(len(self.gold["fact_sales_item"]), 100)
        self.assertGreater(len(self.gold["fact_refund"]), 5)
        self.assertGreater(len(self.silver["refunds"]), len(self.gold["fact_refund"]))      # 무효 환불이 Gold에서 빠졌다

    def test_rejected_positions_match_the_python_pipeline(self):
        rejected = {r["eh_offset"] for r in SPARK.sql("SELECT eh_offset FROM recon_rejected").collect()}
        self.assertEqual(set(self.rejected), rejected)

    def test_error_scenarios_are_recomputed_as_designed(self):
        def facts(view, order_id):
            return self.count(f"SELECT COUNT(*) AS n FROM {view} WHERE order_id = '{order_id}'")
        for channel in "ABC":
            for scenario in ("S24", "S25", "S28"):
                self.assertEqual(0, facts("recon_fact_sales_item", f"{channel}-{scenario}-1"), (channel, scenario))
            self.assertEqual(1, facts("recon_fact_sales_item", f"{channel}-S26-1"))
            self.assertEqual(0, facts("recon_fact_refund", f"{channel}-S26-1"))
            self.assertEqual(0, self.count(f"SELECT COUNT(*) AS n FROM recon_fact_refund WHERE order_id LIKE '{channel}-S27-%'"))
            self.assertEqual(1, facts("recon_fact_refund", f"{channel}-S09-1"))

    # ── 정상 상태: 모든 규칙의 차이 0 ───────────────────────────────────────────────────
    def test_every_rule_finds_no_difference_when_silver_and_gold_are_right(self):
        for rule in Q.load_rules(RULES_PATH):
            with self.subTest(rule=rule["id"]):
                self.assertEqual(0, violations(rule))

    # ── 변이: 실제 테이블을 틀리게 만들면 잡힌다 ─────────────────────────────────────────
    def mutate(self, prefix, table, columns, rows):
        register(prefix, table, columns, rows)

    def silver_rows(self, table):
        return list(self.silver[table])

    def test_a_missing_silver_row_is_found(self):
        for table, rule_id in (("orders", "reconcile_silver_orders"), ("payments", "reconcile_silver_payments"),
                               ("refunds", "reconcile_silver_refunds"), ("order_cancellations", "reconcile_silver_order_cancellations"),
                               ("order_items", "reconcile_silver_order_items")):
            rows = self.silver_rows(table)
            self.mutate("qs", table, silver_tables.TABLES[table]["columns"], rows[1:])
            self.assertEqual(1, violations(self.rule(rule_id)), table)
            self.setUp()

    def test_a_changed_amount_is_found_as_one_missing_and_one_extra_row(self):
        rows = self.silver_rows("payments")
        rows[0] = {**rows[0], "payment_amount": rows[0]["payment_amount"] + 1}
        self.mutate("qs", "payments", silver_tables.TABLES["payments"]["columns"], rows)
        self.assertEqual(2, violations(self.rule("reconcile_silver_payments")))

    def test_an_extra_silver_row_is_found(self):
        rows = self.silver_rows("refunds") + [{**self.silver_rows("refunds")[0], "event_id": "EXTRA", "refund_id": "EXTRA"}]
        self.mutate("qs", "refunds", silver_tables.TABLES["refunds"]["columns"], rows)
        self.assertEqual(1, violations(self.rule("reconcile_silver_refunds")))

    def test_a_wrong_timestamp_is_found(self):
        rows = self.silver_rows("orders")
        rows[0] = {**rows[0], "order_created_at": "2026-01-01T00:00:00.000Z"}
        self.mutate("qs", "orders", silver_tables.TABLES["orders"]["columns"], rows)
        self.assertEqual(2, violations(self.rule("reconcile_silver_orders")))

    def test_quarantine_missing_or_extra_is_found(self):
        columns = [("eh_partition", "INT"), ("eh_offset", "BIGINT")]
        rows = [{"eh_partition": 0, "eh_offset": o} for o in self.rejected]
        self.mutate("qs", "quarantine", columns, rows[1:])
        self.assertEqual(1, violations(self.rule("reconcile_silver_quarantine")))
        self.mutate("qs", "quarantine", columns, rows + [{"eh_partition": 0, "eh_offset": 0}])
        self.assertEqual(1, violations(self.rule("reconcile_silver_quarantine")))

    def test_gold_fact_differences_are_found(self):
        sales = list(self.gold["fact_sales_item"])
        columns = gold_tables.TABLES["fact_sales_item"]["columns"]
        self.mutate("qg", "fact_sales_item", columns, sales[1:])
        self.assertEqual(1, violations(self.rule("reconcile_gold_fact_sales_item")))
        changed = [{**sales[0], "item_paid_amount": sales[0]["item_paid_amount"] + 1}] + sales[1:]
        self.mutate("qg", "fact_sales_item", columns, changed)
        self.assertEqual(2, violations(self.rule("reconcile_gold_fact_sales_item")))
        refunds = list(self.gold["fact_refund"])
        self.mutate("qg", "fact_refund", gold_tables.TABLES["fact_refund"]["columns"], refunds[1:])
        self.assertEqual(1, violations(self.rule("reconcile_gold_fact_refund")))

    def test_a_gold_fact_that_keeps_an_excluded_order_is_found(self):
        """Gold가 위반 주문(이중 결제)을 제외하지 않으면 잡힌다: S24 주문의 상품 행을 억지로 넣는다."""
        columns = gold_tables.TABLES["fact_sales_item"]["columns"]
        wrong = dict(self.gold["fact_sales_item"][0], order_id="A-S24-1", order_item_id="A-S24-1-1")
        self.mutate("qg", "fact_sales_item", columns, list(self.gold["fact_sales_item"]) + [wrong])
        self.assertEqual(1, violations(self.rule("reconcile_gold_fact_sales_item")))


if __name__ == "__main__":
    unittest.main()
