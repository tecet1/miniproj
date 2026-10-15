"""Silver → Gold 한 번 실행. 노트북에서 호출만 한다. 기준은 event_spec.md 6절, 계획은 plans/04다.

build_gold(spark, silver_schema, gold_schema, skus_path)가 순서대로 한다.
  1. Gold 테이블이 없으면 만든다.
  2. 상수·skus.json Dimension을 키로 MERGE한다 (갱신·추가만).
  3. fact_sales_item, fact_refund를 Silver에서 다시 계산해 키로 MERGE한다 (갱신·추가·삭제).
MERGE 전에 두 Fact 소스의 PK 중복을 검사하고, 중복이 있으면 아무것도 적재하지 않고 오류로 끝낸다 (event_spec.md 6.4절).
같은 Silver로 다시 실행해도 결과가 같다 (멱등).
pyspark는 import하지 않는다 (spark 객체를 받아 쓴다). Databricks에서만 실행한다 (미검증).
"""
import dimensions
import gold_sql
from gold_merge import merge_rows
from gold_tables import TABLES, create_tables, schema_ddl


def _merge_dimension(spark, gold_schema: str, table: str, rows: list[tuple]) -> None:
    df = spark.createDataFrame(rows, schema_ddl(table))
    merge_rows(spark, df, f"{gold_schema}.{table}", TABLES[table]["pk"])


def _merge_fact(spark, gold_schema: str, table: str, sql: str) -> None:
    merge_rows(spark, spark.sql(sql), f"{gold_schema}.{table}", TABLES[table]["pk"], delete_missing=True)


class SourceKeyError(ValueError):
    """Fact 소스에 같은 PK가 둘 이상 있을 때 내는 오류. 대상에 없는 키는 소스 안 중복이 모두 삽입될 수 있어 MERGE에 맡길 수 없다."""


def check_source_keys(spark, sources: dict) -> None:
    """sources = {Fact 이름: SELECT 문장}. PK가 겹치는 키가 있으면 SourceKeyError (어느 Fact에 몇 개인지 적는다)."""
    duplicates = {}
    for table, sql in sources.items():
        keys = ", ".join(TABLES[table]["pk"])
        count = spark.sql(f"SELECT COUNT(*) AS n FROM (SELECT {keys} FROM ({sql}) s GROUP BY {keys} HAVING COUNT(*) > 1) d").first()["n"]
        if count:
            duplicates[table] = count
    if duplicates:
        raise SourceKeyError(f"Fact 소스에 PK 중복이 있어 적재하지 않는다: {duplicates}")


def build_gold(spark, silver_schema: str, gold_schema: str, skus_path=dimensions.DEFAULT_SKUS) -> None:
    sources = {"fact_sales_item": gold_sql.sales_item_sql(silver_schema), "fact_refund": gold_sql.refund_sql(silver_schema)}
    check_source_keys(spark, sources)   # 아무것도 쓰기 전에 검사한다
    create_tables(spark, gold_schema)
    _merge_dimension(spark, gold_schema, "dim_channel", dimensions.channel_rows())
    _merge_dimension(spark, gold_schema, "dim_payment_method", dimensions.payment_method_rows())
    _merge_dimension(spark, gold_schema, "dim_refund_profile", dimensions.refund_profile_rows())
    _merge_dimension(spark, gold_schema, "dim_product", dimensions.product_rows(skus_path))
    for table, sql in sources.items():
        _merge_fact(spark, gold_schema, table, sql)
