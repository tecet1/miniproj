"""선언형 품질 규칙(02_pipeline/quality/rules.py, rules.json) 테스트. pyspark 없이 규칙의 모양과 SQL 만들기를 확인한다.

행 규칙의 식이 실제로 어떤 이벤트를 격리하는지는 Spark에서 돌려야 알 수 있다 (카드 7-B에서 expected.json 회귀로 확인한다).
"""
import copy
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "02_pipeline" / "quality"), str(ROOT / "02_pipeline" / "silver")]

import rules as Q  # noqa: E402
import validate_values  # noqa: E402

RULES = Q.load_rules()
ROW = [r for r in RULES if r["kind"] == "row"]
SET = [r for r in RULES if r["kind"] == "set"]
SCHEMAS = {"bronze": "c.bronze", "silver": "c.silver", "gold": "c.gold"}
GOOD_ROW = {"id": "r1", "kind": "row", "applies_to": ["ORDER_CREATED"], "expect": "shipping_fee >= 0",
            "reason_code": "INVALID_VALUE", "detail": "d"}
GOOD_SET = {"id": "s1", "kind": "set", "layer": "silver", "severity": "error", "scope": "incremental", "description": "d",
            "sql": "SELECT COUNT(*) AS violations FROM {silver}.orders WHERE received_at >= {since}"}


def rejects(rule):
    with unittest.TestCase().assertRaises(Q.RuleError):
        Q.validate_rules([rule])


class TestShippedRules(unittest.TestCase):
    def test_the_shipped_rules_load_and_have_both_kinds(self):
        self.assertTrue(ROW)
        self.assertTrue(SET)

    def test_every_existing_value_check_has_a_row_rule(self):
        """validate_values.py의 값 검사가 빠짐없이 규칙으로 표현되어 있다 (필드별)."""
        for column in ("shipping_fee", "quantity", "unit_price", "item_discount", "payment_amount",
                       "refund_quantity", "refund_amount", "product_id"):
            self.assertTrue([r for r in ROW if column in r["expect"]], column)

    def test_row_rules_cover_every_event_type_that_validate_values_checks(self):
        covered = {t for r in ROW for t in r["applies_to"]}
        self.assertEqual(set(validate_values.CHECKS), covered)

    def test_row_rules_use_invalid_value_like_the_existing_checks(self):
        self.assertEqual({"INVALID_VALUE"}, {r["reason_code"] for r in ROW})

    def test_incremental_set_rules_filter_on_since_and_full_ones_do_not(self):
        self.assertTrue([r for r in SET if r["scope"] == "incremental"])
        self.assertTrue([r for r in SET if r["scope"] == "full"])
        for rule in SET:
            self.assertEqual(rule["scope"] == "incremental", "{since}" in rule["sql"], rule["id"])

    def test_set_rules_cover_silver_and_gold(self):
        self.assertEqual({"silver", "gold"}, {r["layer"] for r in SET})


class TestRejectsBadRules(unittest.TestCase):
    def test_missing_or_duplicate_ids(self):
        rejects({k: v for k, v in GOOD_ROW.items() if k != "id"})
        with self.assertRaises(Q.RuleError):
            Q.validate_rules([GOOD_ROW, dict(GOOD_ROW)])

    def test_unknown_kind(self):
        rejects(dict(GOOD_ROW, kind="other"))

    def test_row_rule_needs_known_event_types_reason_and_a_plain_expression(self):
        rejects(dict(GOOD_ROW, applies_to=[]))
        rejects(dict(GOOD_ROW, applies_to=["ORDER_SHIPPED"]))
        rejects(dict(GOOD_ROW, reason_code="OTHER"))
        rejects(dict(GOOD_ROW, expect=""))
        rejects(dict(GOOD_ROW, expect="x IN (SELECT y FROM t)"))
        rejects(dict(GOOD_ROW, expect="a >= 0; DROP TABLE x"))
        rejects(dict(GOOD_ROW, expect="a >= {unknown}"))

    def test_set_rule_needs_valid_fields(self):
        for key, bad in (("layer", "platinum"), ("severity", "fatal"), ("scope", "daily"), ("max", -1), ("max", True)):
            rejects(dict(GOOD_SET, **{key: bad}))

    def test_set_sql_must_be_a_read_only_select_returning_violations(self):
        rejects(dict(GOOD_SET, sql="DELETE FROM {silver}.orders WHERE received_at >= {since}"))
        rejects(dict(GOOD_SET, sql="SELECT COUNT(*) FROM {silver}.orders WHERE received_at >= {since}"))
        rejects(dict(GOOD_SET, sql="SELECT COUNT(*) AS violations FROM {silver}.orders; DROP TABLE x"))
        rejects(dict(GOOD_SET, sql="SELECT COUNT(*) AS violations FROM {silver}.orders WHERE a >= {since} AND b = {other}"))
        rejects(dict(GOOD_SET, sql="SELECT COUNT(*) AS violations FROM {silver}.orders; INSERT INTO t VALUES (1)"))

    def test_write_keywords_are_refused_even_inside_a_select(self):
        for word in ("insert", "update", "delete", "drop", "alter", "merge", "create", "truncate", "optimize", "vacuum"):
            rejects(dict(GOOD_SET, sql=f"SELECT COUNT(*) AS violations FROM {{silver}}.orders WHERE received_at >= {{since}} AND {word}"))
            rejects(dict(GOOD_ROW, expect=f"shipping_fee >= 0 OR {word}"))

    def test_scope_and_since_must_agree(self):
        rejects(dict(GOOD_SET, sql="SELECT COUNT(*) AS violations FROM {silver}.orders"))                  # incremental인데 since 없음
        rejects(dict(GOOD_SET, scope="full"))                                                              # full인데 since 있음

    def test_a_good_pair_is_accepted(self):
        self.assertEqual([GOOD_ROW, GOOD_SET], Q.validate_rules([GOOD_ROW, GOOD_SET]))

    def test_empty_list_is_rejected(self):
        with self.assertRaises(Q.RuleError):
            Q.validate_rules([])


class TestRendering(unittest.TestCase):
    def test_row_condition_is_true_for_violations(self):
        condition = Q.row_violation_condition(GOOD_ROW)
        self.assertEqual("event_type IN ('ORDER_CREATED') AND NOT COALESCE((shipping_fee >= 0), FALSE)", condition)

    def test_row_condition_lists_every_event_type_of_the_rule(self):
        rule = dict(GOOD_ROW, applies_to=["PAYMENT_SUCCEEDED", "PAYMENT_FAILED"])
        self.assertIn("event_type IN ('PAYMENT_SUCCEEDED', 'PAYMENT_FAILED')", Q.row_violation_condition(rule))

    def test_known_skus_become_a_quoted_sorted_list(self):
        rule = [r for r in ROW if r["id"] == "order_item_sku_known"][0]
        condition = Q.row_violation_condition(rule, {"SKU-002", "SKU-001"})
        self.assertIn("i.product_id IN ('SKU-001', 'SKU-002')", condition)
        self.assertNotIn("{", condition)

    def test_literal_list_escapes_quotes_and_handles_an_empty_set(self):
        self.assertEqual("'a''b'", Q.sql_literal_list({"a'b"}))
        self.assertEqual("NULL", Q.sql_literal_list(set()))

    def test_set_sql_fills_schemas_and_since(self):
        sql = Q.render_set_sql(GOOD_SET, SCHEMAS, "TIMESTAMP '2026-10-14 00:00:00'")
        self.assertEqual("SELECT COUNT(*) AS violations FROM c.silver.orders WHERE received_at >= TIMESTAMP '2026-10-14 00:00:00'", sql)

    def test_an_incremental_rule_without_since_is_refused(self):
        with self.assertRaises(Q.RuleError):
            Q.render_set_sql(GOOD_SET, SCHEMAS, None)

    def test_every_shipped_set_rule_renders_without_leftover_placeholders(self):
        for rule in SET:
            sql = Q.render_set_sql(rule, SCHEMAS, "TIMESTAMP '2026-01-01 00:00:00'")
            self.assertNotIn("{", sql, rule["id"])

    def test_judge_passes_up_to_the_allowance_and_fails_with_the_rule_severity(self):
        rule = dict(GOOD_SET, max=2)
        self.assertEqual("pass", Q.judge(rule, 2))
        self.assertEqual("error", Q.judge(rule, 3))
        self.assertEqual("warn", Q.judge(dict(rule, severity="warn"), 3))
        self.assertEqual("pass", Q.judge(dict(GOOD_SET), 0))


class TestReadmeExamples(unittest.TestCase):
    def test_the_json_examples_in_the_readme_are_valid_rules(self):
        import json
        import re
        text = (ROOT / "02_pipeline" / "quality" / "README.md").read_text(encoding="utf-8")
        examples = [json.loads(block) for block in re.findall(r"```json\n(.*?)\n```", text, re.S)]
        self.assertEqual(2, len(examples))
        Q.validate_rules(examples)


class TestLoading(unittest.TestCase):
    def test_disabled_rules_are_left_out(self):
        import json
        import tempfile
        rules = copy.deepcopy([GOOD_ROW, GOOD_SET])
        rules[1]["enabled"] = False
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "r.json"
            path.write_text(json.dumps({"rules": rules}), encoding="utf-8")
            self.assertEqual(["r1"], [r["id"] for r in Q.load_rules(path)])


if __name__ == "__main__":
    unittest.main()
