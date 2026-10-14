"""Serving 테스트용 도우미: 고정 입력으로 Gold 6개 테이블을 sqlite에 만들고, Metric View 정의를 같은 값을 내는 일반 SQL로 바꿔 실행한다.

  gold_connection(rows)     Silver 행 -> Gold SQL(gold_sql) -> sqlite `gold` 스키마의 Fact 2개 + Dimension 5개
  plain_sql(definition, dims, measures)   Metric View의 MEASURE 조회와 같은 결과를 내는 일반 SQL (정의의 조인·식을 그대로 사용)
  FakeSpark                 `MEASURE()` 조회 문장을 plain_sql로 바꿔 sqlite에서 실행하는 가짜 spark (metric_check 검증용)
  register_spark_functions  결제주·비교 기간 식이 쓰는 Spark 함수(date_sub 등)를 같은 이름으로 sqlite에 등록한다. 식 문장은 Databricks와 같고,
                            함수 동작이 Spark와 같은지는 Databricks에서만 확인된다 (미검증)
Metric View 엔진 자체(MEASURE 해석, YAML 파싱)는 흉내 내지 않는다. 그것은 Databricks에서만 확인한다.
"""
import calendar
import datetime
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
for folder in ("02_pipeline/tests/gold", "02_pipeline/gold", "03_serving/metric_views", "03_serving/dashboards"):
    sys.path.insert(0, str(ROOT / folder))

import _silver_rows as silver  # noqa: E402
import dimensions  # noqa: E402
import gold_sql  # noqa: E402
import gold_tables  # noqa: E402
from metric_defs import DEFINITIONS  # noqa: E402

SKUS = ROOT / "01_contracts" / "contracts" / "skus.json"


def _day(value):
    return datetime.date.fromisoformat(str(value)[:10])


def _add_months(value, months):
    """Spark add_months와 같이 달 수를 더하고, 도착한 달에 그 날이 없으면 그 달의 마지막 날로 맞춘다 (2월 29일 -> 2월 28일)."""
    d = _day(value)
    index = d.year * 12 + d.month - 1 + months
    year, month = divmod(index, 12)
    return datetime.date(year, month + 1, min(d.day, calendar.monthrange(year, month + 1)[1])).isoformat()


def _format_number(value, digits):
    """Spark format_number: 천 단위 쉼표와 소수 digits자리(반올림은 짝수 쪽)."""
    return None if value is None else format(value, f",.{digits}f")


def _date_format(value, pattern):
    assert pattern == "yyyy-MM-dd", pattern   # 쓰는 형식만 지원한다
    return _day(value).isoformat()


def register_spark_functions(con) -> None:
    """Spark 함수 6개를 sqlite 사용자 함수로 등록한다. 날짜는 'YYYY-MM-DD' 문자열이다."""
    for name, arity, func in (
            ("date_sub", 2, lambda d, n: (_day(d) - datetime.timedelta(days=n)).isoformat()),
            ("datediff", 2, lambda end, start: (_day(end) - _day(start)).days),
            ("add_months", 2, _add_months),
            ("weekday", 1, lambda d: _day(d).weekday()),   # 월요일 0 ... 일요일 6 (Spark weekday와 같다)
            ("date_format", 2, _date_format),
            ("format_number", 2, _format_number)):
        con.create_function(name, arity, func)


def gold_connection(rows: dict):
    """Silver 행으로 Gold 테이블을 채운 sqlite 연결. 스키마 이름은 `gold`다."""
    con = silver.connect(rows)
    register_spark_functions(con)
    facts = {"fact_sales_item": silver.run(con, gold_sql.sales_item_sql("silver", silver.sqlite_date)),
             "fact_refund": silver.run(con, gold_sql.refund_sql("silver", silver.sqlite_date))}
    dims = {"dim_channel": dimensions.channel_rows(), "dim_payment_method": dimensions.payment_method_rows(),
            "dim_refund_profile": dimensions.refund_profile_rows(), "dim_product": dimensions.product_rows(SKUS)}
    con.execute("ATTACH DATABASE ':memory:' AS gold")
    for table, definition in gold_tables.TABLES.items():
        names = gold_tables.column_names(table)
        con.execute(f"CREATE TABLE gold.{table} ({', '.join(names)})")
        data = ([[r[n] for n in names] for r in facts[table]] if table in facts
                else [list(r) for r in dims[table]])
        con.executemany(f"INSERT INTO gold.{table} VALUES ({', '.join('?' for _ in names)})", data)
    return con


def plain_sql(definition: dict, dims: list, measures: list, where: str = "") -> str:
    """정의의 조인과 식으로 만든 일반 SQL. dims가 비어 있으면 전체를 한 행으로 집계한다. where는 이미 식으로 풀린 조건이다."""
    dim_expr, measure_expr = dict(definition["dimensions"]), dict(definition["measures"])
    joins = " ".join(f"LEFT JOIN gold.{table} AS {table} ON {condition}" for table, condition in definition["joins"])
    columns = [f"{dim_expr[d]} AS {d}" for d in dims] + [f"{measure_expr[m]} AS {m}" for m in measures]
    sql = f"SELECT {', '.join(columns)} FROM gold.{definition['source']} AS source {joins}"
    if where:
        sql += f" WHERE {where}"
    if dims:
        sql += " GROUP BY " + ", ".join(dim_expr[d] for d in dims)
    return sql


class FakeRow(dict):
    def asDict(self):
        return dict(self)


class FakeResult:
    def __init__(self, rows):
        self.rows = [FakeRow(r) for r in rows]

    def collect(self):
        return self.rows

    def first(self):
        return self.rows[0] if self.rows else None


class FakeSpark:
    """measure_sql이 만든 문장(SELECT [열,] MEASURE(x) AS y, ... FROM 스키마.뷰 [GROUP BY 열])만 이해한다."""

    PATTERN = re.compile(r"^SELECT (?:(?P<dim>\w+), )?(?P<measures>MEASURE\(.+?) FROM \w+\.\w+\.(?P<view>\w+)"
                         r"(?: GROUP BY (?P<group>\w+))?$")

    def __init__(self, con):
        self.con = con
        self.queries = []

    def sql(self, text):
        self.queries.append(text)
        match = self.PATTERN.match(text)
        assert match, f"이해하지 못하는 문장: {text}"
        pairs = re.findall(r"MEASURE\((\w+)\) AS (\w+)", match["measures"])
        definition = DEFINITIONS[match["view"]]
        dims = [match["dim"]] if match["dim"] else []
        assert dims == ([match["group"]] if match["group"] else []), text
        measure_names = [m for m, _ in pairs]
        rows = silver.run(self.con, plain_sql(definition, dims, measure_names))
        alias = dict(pairs)
        return FakeResult([{**{d: r[d] for d in dims}, **{alias[m]: r[m] for m in measure_names}} for r in rows])
