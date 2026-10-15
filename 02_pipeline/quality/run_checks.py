"""집합 규칙을 증분(또는 전수)으로 실행하고 결과를 commerce_poc.quality.results에 쌓는다. 노트북은 run_quality_checks만 호출한다.

[흐름] 규칙 고르기(select_rules) -> 증분 시작 시각 정하기(since_for) -> 규칙마다 SQL 실행(run_set_rules) -> 결과 저장(save_results)
[모드]
  incremental  scope가 incremental인 규칙만, since 이후에 들어온 행만 본다. 운영 중에 자주 돈다.
  full         모든 집합 규칙을 since 없이(1970-01-01부터) 전체에 돌린다. 가끔 도는 점검이다.
  reconcile    reconcile_rules.json의 규칙(Bronze 원본에서 독립 경로로 다시 계산한 Silver·Gold와 실제 테이블의 행 단위 비교)을 전체에 돌린다.
               규칙을 돌리기 전에 reconcile_sql.create_views가 recon_* 임시 뷰를 만든다. 긴 주기 배치로 돌린다. 파이프라인이 Bronze를 다 따라잡은 뒤에 돌려야
               반영 중인 행 때문에 차이가 나지 않는다 (진행 중 데이터의 기준 시각 처리는 아직 없다).
[since] 직전 incremental 실행 시각에서 overlap(기본 10분)을 뺀 값이다. 직전 실행 직후에 Silver에 늦게 반영된 행을 놓치지 않으려는 겹침이고,
        규칙은 위반 건수를 세기만 하므로 겹쳐서 다시 봐도 결과가 달라지지 않는다. 기록이 없으면(첫 실행) 전수와 같다.
[한계] incremental 규칙은 received_at(또는 ingested_at)으로 범위를 자른다. 덤프처럼 received_at이 과거인 행은 증분에 걸리지 않으므로 full 모드로 본다.
[결과 행] RESULT_COLUMNS 한 줄 = 규칙 하나의 한 번 실행. status는 pass / warn / error / exception(SQL 실행 실패, violations는 NULL).
Spark를 쓰는 함수(previous_check_time, save_results, create_results_sql 실행)는 Databricks에서만 확인한다 (미검증). 나머지는 pyspark 없이 로컬에서 테스트한다.
"""
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import reconcile_sql
from rules import judge, load_rules, render_set_sql

RESULTS_TABLE = "commerce_poc.quality.results"
RESULT_COLUMNS = [("run_id", "STRING"), ("checked_at", "TIMESTAMP"), ("rule_id", "STRING"), ("stage", "STRING"),
                  ("scope", "STRING"), ("mode", "STRING"), ("severity", "STRING"), ("status", "STRING"),
                  ("violations", "BIGINT"), ("max_allowed", "INT"), ("since", "TIMESTAMP"), ("batch_id", "BIGINT"),
                  ("detail", "STRING")]
MODES = ("incremental", "full", "reconcile")
ROOT = Path(__file__).resolve().parents[2]
RECONCILE_RULES = Path(__file__).resolve().parent / "reconcile_rules.json"
SKUS_PATH = ROOT / "01_contracts" / "contracts" / "skus.json"
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
DEFAULT_OVERLAP = timedelta(minutes=10)
FAILED_STATUSES = ("error", "exception")


def results_ddl() -> str:
    """createDataFrame에 넘길 DDL 문자열 "이름 타입, ..."."""
    return ", ".join(f"{name} {sql_type}" for name, sql_type in RESULT_COLUMNS)


def create_results_sql(table: str = RESULTS_TABLE) -> str:
    return f"CREATE TABLE IF NOT EXISTS {table} ({results_ddl()})"


def make_result(rule_id: str, stage: str, scope: str, mode: str, severity: str, max_allowed: int, violations,
                run_id: str, checked_at: datetime, since=None, batch_id=None, detail: str = "") -> dict:
    """결과 한 줄. violations가 None이면 실행 실패(exception)로 기록한다."""
    if violations is None:
        status = "exception"
    else:
        status = judge({"severity": severity, "max": max_allowed}, violations)
    return {"run_id": run_id, "checked_at": checked_at, "rule_id": rule_id, "stage": stage, "scope": scope, "mode": mode,
            "severity": severity, "status": status, "violations": violations, "max_allowed": max_allowed,
            "since": since, "batch_id": batch_id, "detail": detail}


def sql_timestamp(moment: datetime) -> str:
    """UTC 시각을 규칙 SQL의 {since}에 들어갈 리터럴로 쓴다."""
    return "TIMESTAMP '" + moment.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S") + "+00:00'"


def select_rules(rules: list, mode: str) -> list:
    """집합 규칙 중 이 모드에서 돌릴 것. incremental은 증분 규칙만, full과 reconcile은 전부 (reconcile은 reconcile 규칙 목록을 받는다)."""
    if mode not in MODES:
        raise ValueError(f"mode는 {MODES} 중 하나여야 한다: {mode!r}")
    sets = [rule for rule in rules if rule["kind"] == "set"]
    return [rule for rule in sets if mode != "incremental" or rule["scope"] == "incremental"]


def since_for(mode: str, previous, overlap: timedelta = DEFAULT_OVERLAP) -> datetime:
    """증분 시작 시각. incremental이 아니거나 직전 기록이 없으면 EPOCH(전체), 아니면 직전 실행 시각 - overlap."""
    if mode != "incremental" or previous is None:
        return EPOCH
    return previous - overlap


def run_set_rules(run_sql, rules: list, schemas: dict, mode: str, since: datetime, run_id: str,
                  checked_at: datetime) -> list[dict]:
    """규칙마다 SQL을 실행해 결과 행 목록을 만든다. run_sql(sql) -> 위반 건수(int).

    SQL 실행이 실패하면 그 규칙은 exception으로 기록하고 다음 규칙으로 넘어간다 (한 규칙이 전체 검사를 막지 않는다).
    """
    rows = []
    literal = sql_timestamp(since)
    for rule in select_rules(rules, mode):
        try:
            violations, detail = int(run_sql(render_set_sql(rule, schemas, literal))), rule.get("description", "")
        except Exception as error:   # noqa: BLE001  어떤 SQL 오류든 결과에 남기고 계속한다
            violations, detail = None, f"{type(error).__name__}: {str(error)[:300]}"
        rows.append(make_result(rule["id"], rule["layer"], rule["scope"], mode, rule["severity"], rule.get("max", 0),
                                violations, run_id, checked_at, since, None, detail))
    return rows


def failed(rows: list[dict]) -> list[dict]:
    """error 또는 exception인 결과. warn은 다음 단계를 막지 않는다."""
    return [row for row in rows if row["status"] in FAILED_STATUSES]


# ── Spark (Databricks에서만 확인) ──────────────────────────────────────────────────────
def save_results(spark, rows: list[dict], table: str = RESULTS_TABLE) -> None:
    """결과 행을 테이블에 덧붙인다. 테이블이 없으면 먼저 만든다."""
    if not rows:
        return
    spark.sql(create_results_sql(table))
    columns = [name for name, _ in RESULT_COLUMNS]
    spark.createDataFrame([tuple(row[c] for c in columns) for row in rows], results_ddl()) \
        .write.mode("append").insertInto(table)


def previous_check_time(spark, table: str = RESULTS_TABLE):
    """직전 incremental 모드 실행 시각. 기록이 없으면 None."""
    spark.sql(create_results_sql(table))
    return spark.sql(f"SELECT MAX(checked_at) AS t FROM {table} WHERE mode = 'incremental'").first()["t"]


def run_quality_checks(spark, schemas: dict, mode: str = "incremental", table: str = RESULTS_TABLE,
                       overlap: timedelta = DEFAULT_OVERLAP, rules=None, now=None) -> list[dict]:
    """집합 규칙을 실행하고 결과를 저장한 뒤 결과 행을 돌려준다. schemas = {"bronze": ..., "silver": ..., "gold": ...}"""
    if rules is None:
        rules = load_rules(RECONCILE_RULES) if mode == "reconcile" else load_rules()
    now = now or datetime.now(timezone.utc)
    if mode == "reconcile":   # Bronze에서 다시 계산한 recon_* 뷰를 먼저 만든다
        known = {sku["product_id"] for sku in json.loads(SKUS_PATH.read_text(encoding="utf-8"))["skus"]}
        reconcile_sql.create_views(spark, f"{schemas['bronze']}.events", known)
    since = since_for(mode, previous_check_time(spark, table), overlap)
    run_id = f"{mode}-{now.strftime('%Y%m%dT%H%M%S')}"
    rows = run_set_rules(lambda sql: spark.sql(sql).first()["violations"], rules, schemas, mode, since, run_id, now)
    save_results(spark, rows, table)
    return rows
