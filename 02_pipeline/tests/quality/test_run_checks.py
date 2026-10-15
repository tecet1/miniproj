"""run_checks(집합 규칙 증분 실행과 결과 기록)를 pyspark 없이, 그리고 로컬 Spark로 확인한다.

[확인하는 것] 규칙 고르기(모드별), 증분 시작 시각, 결과 행의 모양과 status, SQL 실패 처리, 로컬 Spark에서 실제 규칙 SQL의 증분 범위.
[확인하지 못하는 것] results 테이블 저장(Delta, insertInto), Unity Catalog 권한. Databricks에서 확인한다 (미검증).
"""
import re
import sys
import unittest
from unittest import mock
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "02_pipeline" / "quality"), str(ROOT / "02_pipeline" / "silver"),
                str(ROOT / "02_pipeline" / "gold"), str(ROOT / "02_pipeline" / "tests")]

import gold_tables  # noqa: E402
import rules as Q  # noqa: E402
import run_checks as R  # noqa: E402
import silver_tables  # noqa: E402
from spark_support import local_spark  # noqa: E402

RULES = Q.load_rules()
NOW = datetime(2026, 10, 15, 12, 0, tzinfo=timezone.utc)
SCHEMAS = {"bronze": "qb", "silver": "qs", "gold": "qg"}
SET_RULES = [r for r in RULES if r["kind"] == "set"]
INCREMENTAL = [r["id"] for r in SET_RULES if r["scope"] == "incremental"]


class TestSelectAndSince(unittest.TestCase):
    def test_incremental_mode_runs_only_incremental_rules(self):
        self.assertEqual(INCREMENTAL, [r["id"] for r in R.select_rules(RULES, "incremental")])

    def test_full_mode_runs_every_set_rule_and_no_row_rule(self):
        self.assertEqual([r["id"] for r in SET_RULES], [r["id"] for r in R.select_rules(RULES, "full")])

    def test_unknown_mode_is_rejected(self):
        with self.assertRaises(ValueError):
            R.select_rules(RULES, "daily")

    def test_since_is_previous_minus_overlap(self):
        self.assertEqual(NOW - timedelta(minutes=10), R.since_for("incremental", NOW))
        self.assertEqual(NOW - timedelta(minutes=1), R.since_for("incremental", NOW, timedelta(minutes=1)))

    def test_first_run_and_full_mode_look_at_everything(self):
        self.assertEqual(R.EPOCH, R.since_for("incremental", None))
        self.assertEqual(R.EPOCH, R.since_for("full", NOW))

    def test_sql_timestamp_is_utc_literal(self):
        kst = timezone(timedelta(hours=9))
        self.assertEqual("TIMESTAMP '2026-10-15 12:00:00+00:00'", R.sql_timestamp(NOW))
        self.assertEqual("TIMESTAMP '2026-10-15 12:00:00+00:00'", R.sql_timestamp(NOW.astimezone(kst)))


class TestResultRows(unittest.TestCase):
    def row(self, violations, severity="error", maximum=0):
        return R.make_result("r1", "silver", "incremental", "incremental", severity, maximum, violations, "run", NOW)

    def test_status_follows_judge(self):
        self.assertEqual("pass", self.row(0)["status"])
        self.assertEqual("error", self.row(1)["status"])
        self.assertEqual("warn", self.row(1, "warn")["status"])
        self.assertEqual("pass", self.row(5, maximum=5)["status"])

    def test_missing_violations_is_an_exception_not_a_pass(self):
        self.assertEqual("exception", self.row(None)["status"])

    def test_row_has_exactly_the_result_columns(self):
        self.assertEqual([name for name, _ in R.RESULT_COLUMNS], list(self.row(0)))

    def test_failed_keeps_error_and_exception_but_not_warn(self):
        rows = [self.row(0), self.row(1), self.row(1, "warn"), self.row(None)]
        self.assertEqual(["error", "exception"], [r["status"] for r in R.failed(rows)])

    def test_create_sql_lists_every_column(self):
        sql = R.create_results_sql("c.quality.results")
        self.assertTrue(sql.startswith("CREATE TABLE IF NOT EXISTS c.quality.results"))
        for name, sql_type in R.RESULT_COLUMNS:
            self.assertIn(f"{name} {sql_type}", sql)


class TestRunSetRules(unittest.TestCase):
    def run_rules(self, run_sql, mode="incremental", since=NOW):
        return R.run_set_rules(run_sql, RULES, SCHEMAS, mode, since, "run-1", NOW)

    def test_one_row_per_selected_rule_with_since_and_mode(self):
        rows = self.run_rules(lambda sql: 0)
        self.assertEqual(INCREMENTAL, [r["rule_id"] for r in rows])
        self.assertTrue(all(r["mode"] == "incremental" and r["since"] == NOW and r["status"] == "pass" for r in rows))

    def test_the_since_literal_and_schemas_reach_the_sql(self):
        seen = []
        self.run_rules(lambda sql: seen.append(sql) or 0)
        self.assertTrue(all("TIMESTAMP '2026-10-15 12:00:00+00:00'" in sql for sql in seen))
        self.assertFalse(any("{" in sql for sql in seen))

    def test_violation_count_is_recorded_with_rule_name_and_stage(self):
        rows = self.run_rules(lambda sql: 3 if "orders" in sql and "GROUP BY" in sql else 0)
        bad = [r for r in rows if r["violations"]]
        self.assertEqual([("silver_orders_pk_unique", "silver", 3, "error")],
                         [(r["rule_id"], r["stage"], r["violations"], r["status"]) for r in bad])

    def test_failing_sql_is_recorded_and_the_rest_still_run(self):
        def run_sql(sql):
            if "payments" in sql and "GROUP BY" in sql:
                raise RuntimeError("table not found")
            return 0
        rows = self.run_rules(run_sql)
        self.assertEqual(len(INCREMENTAL), len(rows))
        broken = [r for r in rows if r["status"] == "exception"]
        self.assertEqual(["silver_payments_pk_unique"], [r["rule_id"] for r in broken])
        self.assertIsNone(broken[0]["violations"])
        self.assertIn("table not found", broken[0]["detail"])

    def test_full_mode_runs_without_a_time_limit(self):
        seen = []
        rows = self.run_rules(lambda sql: seen.append(sql) or 0, "full", R.EPOCH)
        self.assertEqual(len(SET_RULES), len(rows))
        self.assertTrue(any("1970-01-01" in sql for sql in seen))


class TestSaveResults(unittest.TestCase):
    class FakeSpark:
        def __init__(self):
            self.statements = []

        def sql(self, statement):
            self.statements.append(statement)

    def test_empty_results_write_nothing(self):
        spark = self.FakeSpark()
        R.save_results(spark, [])
        self.assertEqual([], spark.statements)


class TestReconcileMode(unittest.TestCase):
    RECONCILE = Q.load_rules(R.RECONCILE_RULES)

    def test_reconcile_runs_every_rule_of_the_reconcile_file(self):
        self.assertEqual([r["id"] for r in self.RECONCILE], [r["id"] for r in R.select_rules(self.RECONCILE, "reconcile")])

    def test_reconcile_never_uses_a_time_window(self):
        self.assertEqual(R.EPOCH, R.since_for("reconcile", NOW))

    def run_reconcile(self, violations):
        spark = mock.Mock()
        with mock.patch.object(R.reconcile_sql, "create_views") as create_views,              mock.patch.object(R, "previous_check_time", return_value=NOW), mock.patch.object(R, "save_results") as save:
            spark.sql.side_effect = lambda sql: mock.Mock(first=lambda: {"violations": violations})
            rows = R.run_quality_checks(spark, SCHEMAS, mode="reconcile", now=NOW)
        return rows, create_views, save

    def test_views_are_built_from_the_bronze_events_table_and_the_known_skus_first(self):
        rows, create_views, save = self.run_reconcile(0)
        (spark, bronze, skus), _ = create_views.call_args
        self.assertEqual("qb.events", bronze)
        self.assertEqual(15, len(skus))
        self.assertIn("SKU-001", skus)
        save.assert_called_once()

    def test_every_reconcile_rule_is_recorded_with_the_reconcile_mode(self):
        rows, _, _ = self.run_reconcile(0)
        self.assertEqual([r["id"] for r in self.RECONCILE], [r["rule_id"] for r in rows])
        self.assertEqual({"reconcile"}, {r["mode"] for r in rows})
        self.assertEqual({"pass"}, {r["status"] for r in rows})
        self.assertEqual({R.EPOCH}, {r["since"] for r in rows})

    def test_a_difference_is_recorded_as_an_error_with_its_count(self):
        rows, _, _ = self.run_reconcile(3)
        self.assertEqual({"error"}, {r["status"] for r in rows})
        self.assertEqual({3}, {r["violations"] for r in rows})
        self.assertEqual(len(self.RECONCILE), len(R.failed(rows)))

    def test_the_other_modes_do_not_build_reconcile_views(self):
        spark = mock.Mock()
        with mock.patch.object(R.reconcile_sql, "create_views") as create_views,              mock.patch.object(R, "previous_check_time", return_value=None), mock.patch.object(R, "save_results"):
            spark.sql.side_effect = lambda sql: mock.Mock(first=lambda: {"violations": 0})
            R.run_quality_checks(spark, SCHEMAS, mode="incremental", now=NOW)
            R.run_quality_checks(spark, SCHEMAS, mode="full", now=NOW)
        create_views.assert_not_called()


# ── 로컬 Spark: 실제 규칙 SQL이 증분 범위만 보는지 ─────────────────────────────────────────────
SPARK = local_spark()
BRONZE_EVENTS = [("channel", "STRING"), ("payload", "STRING"), ("eh_partition", "INT"), ("eh_offset", "BIGINT")]
DEFINITIONS = {"qb": {"events": BRONZE_EVENTS},
               "qs": {t: d["columns"] for t, d in silver_tables.TABLES.items()},
               "qg": {t: d["columns"] for t, d in gold_tables.TABLES.items()}}


def stamp(text):
    return datetime.fromisoformat(text).replace(tzinfo=timezone.utc)


def register(schema, table, rows):
    columns = DEFINITIONS[schema][table]
    ddl = ", ".join(f"{name} {sql_type}" for name, sql_type in columns)
    SPARK.createDataFrame([tuple(r.get(name) for name, _ in columns) for r in rows], ddl) \
        .createOrReplaceTempView(f"{schema}_{table}")


def local_run_sql(sql):
    return SPARK.sql(re.sub(r"\b(qb|qs|qg)\.(\w+)", r"\1_\2", sql)).first()["violations"]


@unittest.skipUnless(SPARK, "pyspark/Java가 없으면 로컬 Spark 검증을 건너뛴다")
class TestOnLocalSpark(unittest.TestCase):
    def setUp(self):
        for schema, tables in DEFINITIONS.items():
            for table in tables:
                register(schema, table, [])

    def run_rules(self, mode, since):
        return {r["rule_id"]: r for r in R.run_set_rules(local_run_sql, RULES, SCHEMAS, mode, since, "run", NOW)}

    def duplicate_orders(self, received_at):
        register("qs", "orders", [{"channel": "A", "event_id": "E1", "received_at": stamp(received_at)}] * 2)

    def test_clean_tables_pass_every_rule(self):
        for mode in R.MODES:
            self.assertEqual({"pass"}, {r["status"] for r in self.run_rules(mode, R.EPOCH).values()}, mode)

    def test_new_duplicate_is_recorded_as_an_error(self):
        self.duplicate_orders("2026-10-15 11:55:00")
        row = self.run_rules("incremental", NOW - timedelta(minutes=10))["silver_orders_pk_unique"]
        self.assertEqual(("error", 1, "silver"), (row["status"], row["violations"], row["stage"]))

    def test_old_duplicate_is_outside_the_incremental_window_but_full_mode_sees_it(self):
        self.duplicate_orders("2026-10-01 00:00:00")
        self.assertEqual("pass", self.run_rules("incremental", NOW - timedelta(minutes=10))["silver_orders_pk_unique"]["status"])
        self.assertEqual("error", self.run_rules("full", R.EPOCH)["silver_orders_pk_unique"]["status"])

    def test_full_scope_rule_only_runs_in_full_mode(self):
        register("qs", "order_items", [{"channel": "A", "order_item_id": "I1"}] * 2)
        self.assertNotIn("silver_order_items_pk_unique", self.run_rules("incremental", R.EPOCH))
        self.assertEqual("error", self.run_rules("full", R.EPOCH)["silver_order_items_pk_unique"]["status"])


if __name__ == "__main__":
    unittest.main()
