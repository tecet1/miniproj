"""Silver → Gold 한 번 실행. 노트북에서 호출만 한다. 기준은 event_spec.md 6절, 계획은 plans/04다.

build_gold(spark, silver_schema, gold_schema, skus_path)가 순서대로 한다.
  1. Gold 테이블이 없으면 만든다.
  2. 상수·skus.json Dimension을 키로 MERGE한다 (갱신·추가만).
  3. fact_sales_item, fact_refund를 Silver에서 다시 계산해 키로 MERGE한다 (갱신·추가·삭제).
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


def build_gold(spark, silver_schema: str, gold_schema: str, skus_path=dimensions.DEFAULT_SKUS) -> None:
    create_tables(spark, gold_schema)
    _merge_dimension(spark, gold_schema, "dim_channel", dimensions.channel_rows())
    _merge_dimension(spark, gold_schema, "dim_payment_method", dimensions.payment_method_rows())
    _merge_dimension(spark, gold_schema, "dim_refund_profile", dimensions.refund_profile_rows())
    _merge_dimension(spark, gold_schema, "dim_product", dimensions.product_rows(skus_path))
    _merge_fact(spark, gold_schema, "fact_sales_item", gold_sql.sales_item_sql(silver_schema))
    _merge_fact(spark, gold_schema, "fact_refund", gold_sql.refund_sql(silver_schema))
