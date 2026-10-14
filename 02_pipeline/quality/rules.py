"""선언형 품질 규칙을 읽고 검증하고 SQL로 바꾼다. Spark를 모르는 순수 함수다 (로컬 테스트용). 사용 방법은 README.md.

규칙은 rules.json의 목록이다. 종류는 둘이다.
  row  행 단위: 공통 이벤트 한 건을 보고 정상/격리를 가른다. 위반 행은 Quarantine(reason_code)으로 간다.
       {"id", "kind": "row", "applies_to": [이벤트 유형...], "expect": "<Spark SQL 불리언 식>", "reason_code", "detail"}
       expect는 "이 조건이 참이어야 정상"이다. 거짓이거나 NULL이면 위반이다.
       식에서 쓸 수 있는 것: 공통 이벤트 컬럼(silver_tables.EVENT_SCHEMA), 고차 함수(forall, exists), 자리표시자 {known_skus}
  set  집합 단위: SQL이 위반 건수 한 값(컬럼 이름 violations)을 돌려준다. max를 넘으면 위반이다.
       {"id", "kind": "set", "layer": "bronze|silver|gold", "sql", "max": 0, "severity": "error|warn",
        "scope": "incremental|full", "description"}
       자리표시자: {bronze} {silver} {gold}(스키마 이름), {since}(증분일 때 직전 확인 시각의 SQL 리터럴, 예: TIMESTAMP '...')
       scope가 incremental이면 sql에 {since}가 있어야 하고(이번에 들어온 행만 본다), full이면 없어야 한다.
규칙을 한 줄 추가했을 때 잘못된 모양이 실행 시점에 터지지 않도록 load_rules가 먼저 검사한다.
"""
import json
import re
from pathlib import Path

DEFAULT_RULES = Path(__file__).resolve().parent / "rules.json"
EVENT_TYPES = ("ORDER_CREATED", "ORDER_CANCELLED", "PAYMENT_SUCCEEDED", "PAYMENT_FAILED", "REFUND_SUCCEEDED")
REASON_CODES = ("MALFORMED_JSON", "UNKNOWN_EVENT_TYPE", "MISSING_REQUIRED", "INVALID_TYPE", "INVALID_VALUE", "UNKNOWN_CHANNEL")
LAYERS = ("bronze", "silver", "gold")
SEVERITIES = ("error", "warn")
SCOPES = ("incremental", "full")
SET_PLACEHOLDERS = {"bronze", "silver", "gold", "since"}
ROW_PLACEHOLDERS = {"known_skus"}
# 규칙 SQL은 읽기만 한다. 쓰기·스키마 변경 문장이 섞이면 거부한다.
FORBIDDEN_SQL = re.compile(r"\b(insert|update|delete|drop|alter|merge|create|truncate|grant|revoke|optimize|vacuum)\b", re.I)
PLACEHOLDER = re.compile(r"\{(\w+)\}")


class RuleError(ValueError):
    """규칙 모양이 잘못되었을 때 내는 오류. 메시지에 규칙 id와 이유를 적는다."""


def _fail(rule: dict, reason: str):
    raise RuleError(f"규칙 {rule.get('id', '(id 없음)')}: {reason}")


def _check_text(rule: dict, key: str, allowed_placeholders: set) -> str:
    text = rule.get(key)
    if not isinstance(text, str) or not text.strip():
        _fail(rule, f"{key}가 없거나 비어 있다")
    if ";" in text:
        _fail(rule, f"{key}에 ';'를 쓸 수 없다 (문장 하나만)")
    if FORBIDDEN_SQL.search(text):
        _fail(rule, f"{key}에 읽기 전용이 아닌 SQL 키워드가 있다")
    unknown = set(PLACEHOLDER.findall(text)) - allowed_placeholders
    if unknown:
        _fail(rule, f"{key}의 알 수 없는 자리표시자: {sorted(unknown)}")
    return text


def _check_row(rule: dict) -> None:
    applies = rule.get("applies_to")
    if not isinstance(applies, list) or not applies or not set(applies) <= set(EVENT_TYPES):
        _fail(rule, f"applies_to는 {EVENT_TYPES} 중 하나 이상의 목록이어야 한다")
    expect = _check_text(rule, "expect", ROW_PLACEHOLDERS)
    if re.search(r"\bselect\b", expect, re.I):
        _fail(rule, "expect에 서브쿼리를 쓸 수 없다 (참조 데이터는 자리표시자로 받는다)")
    if rule.get("reason_code") not in REASON_CODES:
        _fail(rule, f"reason_code는 {REASON_CODES} 중 하나여야 한다")


def _check_set(rule: dict) -> None:
    if rule.get("layer") not in LAYERS:
        _fail(rule, f"layer는 {LAYERS} 중 하나여야 한다")
    if rule.get("severity") not in SEVERITIES:
        _fail(rule, f"severity는 {SEVERITIES} 중 하나여야 한다")
    if rule.get("scope") not in SCOPES:
        _fail(rule, f"scope는 {SCOPES} 중 하나여야 한다")
    maximum = rule.get("max", 0)
    if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum < 0:
        _fail(rule, "max는 0 이상의 정수여야 한다")
    sql = _check_text(rule, "sql", SET_PLACEHOLDERS)
    if not re.match(r"\s*select\b", sql, re.I):
        _fail(rule, "sql은 SELECT로 시작해야 한다")
    if not re.search(r"\bas\s+violations\b", sql, re.I):
        _fail(rule, "sql은 위반 건수를 `AS violations` 컬럼으로 돌려줘야 한다")
    has_since = "{since}" in sql
    if rule["scope"] == "incremental" and not has_since:
        _fail(rule, "scope가 incremental이면 sql에 {since}가 있어야 한다")
    if rule["scope"] == "full" and has_since:
        _fail(rule, "scope가 full이면 sql에 {since}를 쓰지 않는다")


def validate_rules(rules: list) -> list[dict]:
    """규칙 목록을 검사해 그대로 돌려준다. 잘못되면 RuleError."""
    if not isinstance(rules, list) or not rules:
        raise RuleError("규칙 목록이 비어 있거나 목록이 아니다")
    seen = set()
    for rule in rules:
        if not isinstance(rule, dict):
            raise RuleError(f"규칙은 객체여야 한다: {rule!r}")
        rule_id = rule.get("id")
        if not isinstance(rule_id, str) or not re.fullmatch(r"[a-z][a-z0-9_]*", rule_id):
            _fail(rule, "id는 소문자·숫자·밑줄로 된 이름이어야 한다")
        if rule_id in seen:
            _fail(rule, "id가 중복이다")
        seen.add(rule_id)
        if rule.get("kind") == "row":
            _check_row(rule)
        elif rule.get("kind") == "set":
            _check_set(rule)
        else:
            _fail(rule, "kind는 row 또는 set이어야 한다")
    return rules


def load_rules(path=DEFAULT_RULES) -> list[dict]:
    """rules.json을 읽고 검사한다. enabled가 false인 규칙은 목록에서 뺀다."""
    rules = json.loads(Path(path).read_text(encoding="utf-8"))["rules"]
    return [rule for rule in validate_rules(rules) if rule.get("enabled", True)]


def sql_literal_list(values) -> str:
    """문자열 집합을 SQL IN 목록 문자열로 만든다. 작은따옴표는 두 개로 쓴다. 비어 있으면 NULL 하나 (어떤 값도 맞지 않는다)."""
    quoted = ", ".join("'" + str(v).replace("'", "''") + "'" for v in sorted(values))
    return quoted or "NULL"


def row_violation_condition(rule: dict, known_skus=()) -> str:
    """행 규칙의 위반 조건(참이면 격리)을 Spark SQL 식으로 만든다."""
    types = ", ".join(f"'{t}'" for t in rule["applies_to"])
    expect = rule["expect"].replace("{known_skus}", sql_literal_list(known_skus))
    return f"event_type IN ({types}) AND NOT COALESCE(({expect}), FALSE)"


def render_set_sql(rule: dict, schemas: dict, since: str | None = None) -> str:
    """집합 규칙의 SQL에 스키마 이름과 {since}를 채운다. schemas = {"bronze": ..., "silver": ..., "gold": ...}.

    incremental 규칙에 since를 주지 않으면 오류다 (전수 조사로 슬그머니 바뀌지 않게 한다).
    """
    sql = rule["sql"]
    if "{since}" in sql:
        if since is None:
            raise RuleError(f"규칙 {rule['id']}: incremental 규칙에는 since가 필요하다")
        sql = sql.replace("{since}", since)
    return PLACEHOLDER.sub(lambda m: schemas[m.group(1)], sql)


def judge(rule: dict, violations: int) -> str:
    """집합 규칙 한 건의 결과: 허용값 이하면 "pass", 넘으면 severity("error" 또는 "warn")."""
    return "pass" if violations <= rule.get("max", 0) else rule["severity"]
