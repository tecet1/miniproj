"""Gold 테이블 정의: 컬럼·타입·PK. 기준은 event_spec.md 6.1·6.2절이다 (A사_ERD.pdf ②의 dim_date는 제외했다).

여기만 보면 Gold 테이블 6개의 모양을 알 수 있도록 한곳에 둔다.
  TABLES       테이블 이름 -> {"pk": [...], "columns": [(이름, Spark SQL 타입), ...]}. Dimension 먼저, Fact 나중
  schema_ddl   "이름 타입, ..." 문자열 (CREATE TABLE과 createDataFrame이 함께 쓴다)
  create_tables 테이블이 없으면 만든다
PK는 Delta가 강제하지 않는다. 중복 여부는 gold_check가 확인한다.
pyspark는 Databricks에서만 쓰므로 import하지 않는다 (spark 객체를 받아 쓴다).
"""

# { "테이블 이름": {"pk": [...], "columns": [(이름, Spark SQL 타입), ...]} }
TABLES = {
    "dim_channel": {
        "pk": ["channel"],
        "columns": [("channel", "STRING"), ("channel_name", "STRING")],
    },
    "dim_product": {
        "pk": ["product_id"],
        "columns": [("product_id", "STRING"), ("product_name", "STRING"), ("category", "STRING")],
    },
    "dim_payment_method": {
        "pk": ["payment_method"],
        "columns": [("payment_method", "STRING"), ("method_name", "STRING")],
    },
    "dim_refund_profile": {
        "pk": ["refund_profile_key"],
        "columns": [("refund_profile_key", "STRING"), ("refund_type", "STRING"), ("refund_reason", "STRING")],
    },
    "fact_sales_item": {
        "pk": ["channel", "order_item_id"],
        "columns": [("channel", "STRING"), ("order_item_id", "STRING"), ("product_id", "STRING"),
                    ("payment_date", "DATE"), ("payment_method", "STRING"), ("order_id", "STRING"),
                    ("user_id", "STRING"), ("payment_id", "STRING"), ("payment_at", "TIMESTAMP"),
                    ("quantity", "INT"), ("item_gross_amount", "BIGINT"), ("item_discount", "BIGINT"),
                    ("item_paid_amount", "BIGINT"), ("refund_quantity", "INT"), ("refund_amount", "BIGINT"),
                    ("last_refund_date", "DATE")],
    },
    "fact_refund": {
        "pk": ["channel", "refund_id"],
        "columns": [("channel", "STRING"), ("refund_id", "STRING"), ("product_id", "STRING"),
                    ("payment_date", "DATE"), ("refund_date", "DATE"), ("refund_profile_key", "STRING"),
                    ("order_item_id", "STRING"), ("order_id", "STRING"), ("payment_id", "STRING"),
                    ("refund_quantity", "INT"), ("refund_amount", "BIGINT")],
    },
}


def column_names(table: str) -> list[str]:
    """테이블의 컬럼 이름을 순서대로 돌려준다."""
    return [name for name, _ in TABLES[table]["columns"]]


def schema_ddl(table: str) -> str:
    """"channel STRING, channel_name STRING" 같은 컬럼 정의 문자열."""
    return ", ".join(f"{name} {sql_type}" for name, sql_type in TABLES[table]["columns"])


def create_table_sql(table: str, schema: str) -> str:
    """CREATE TABLE IF NOT EXISTS 문장을 만든다. schema는 "commerce_poc.gold" 같은 카탈로그.스키마."""
    return f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({schema_ddl(table)})"


def create_tables(spark, schema: str) -> None:
    """Gold 테이블 6개를 만든다. 이미 있으면 그대로 둔다 (재실행해도 안전)."""
    for table in TABLES:
        spark.sql(create_table_sql(table, schema))
