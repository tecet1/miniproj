"""Silver 테이블 정의: 컬럼·타입·PK와 공통 이벤트 JSON 스키마. 기준은 event_spec.md 1.3절·4절이다.

여기만 보면 Silver 테이블 6개의 모양을 알 수 있도록 한곳에 둔다.
  TABLES        테이블 이름 -> {"pk": [...], "columns": [(이름, Spark SQL 타입), ...]}
  EVENT_SCHEMA  convert_row가 만든 event_json을 from_json으로 읽을 때 쓰는 스키마 (공통 이벤트 모델, 시각은 문자열)
  create_tables 테이블이 없으면 만든다
PK는 Delta가 강제하지 않는다. 중복 여부는 silver_check가 확인한다.
pyspark는 Databricks에서만 쓰므로 import하지 않는다 (spark 객체를 받아 쓴다).
"""

# convert_row의 event_json을 읽는 스키마. 시각(event_time)은 ISO 8601 UTC 문자열이라 Spark에서 to_timestamp로 바꾼다.
# "컬럼명 타입", ...
EVENT_SCHEMA = (
    "event_id STRING, event_type STRING, channel STRING, order_id STRING, user_id STRING, event_time STRING, "
    "shipping_fee BIGINT, "
    "items ARRAY<STRUCT<order_item_id: STRING, product_id: STRING, product_name: STRING, category: STRING, "
    "quantity INT, unit_price BIGINT, item_discount BIGINT>>, "
    "cancel_reason_code STRING, "
    "payment_id STRING, payment_method STRING, payment_amount BIGINT, fail_reason_code STRING, "
    "refund_id STRING, order_item_id STRING, refund_quantity INT, refund_amount BIGINT, "
    "refund_type STRING, refund_reason_code STRING"
)

# { "테이블 이름": {"pk": [...], "columns": [(이름, Spark SQL 타입), ...]} }
TABLES = {
    "orders": {
        "pk": ["channel", "event_id"],
        "columns": [("channel", "STRING"), ("event_id", "STRING"), ("order_id", "STRING"), ("user_id", "STRING"),
                    ("shipping_fee", "BIGINT"), ("order_created_at", "TIMESTAMP"), ("received_at", "TIMESTAMP")],
    },
    "order_items": {
        "pk": ["channel", "order_item_id"],
        "columns": [("channel", "STRING"), ("order_item_id", "STRING"), ("order_id", "STRING"),
                    ("product_id", "STRING"), ("product_name", "STRING"), ("category", "STRING"),
                    ("quantity", "INT"), ("unit_price", "BIGINT"), ("item_discount", "BIGINT")],
    },
    "order_cancellations": {
        "pk": ["channel", "event_id"],
        "columns": [("channel", "STRING"), ("event_id", "STRING"), ("order_id", "STRING"), ("user_id", "STRING"),
                    ("cancel_reason_code", "STRING"), ("cancel_created_at", "TIMESTAMP"),
                    ("received_at", "TIMESTAMP")],
    },
    "payments": {
        "pk": ["channel", "event_id"],
        "columns": [("channel", "STRING"), ("event_id", "STRING"), ("order_id", "STRING"), ("user_id", "STRING"),
                    ("payment_id", "STRING"), ("status", "STRING"), ("payment_method", "STRING"),
                    ("payment_amount", "BIGINT"), ("payment_fail_reason_code", "STRING"),
                    ("payment_created_at", "TIMESTAMP"), ("received_at", "TIMESTAMP")],
    },
    "refunds": {
        "pk": ["channel", "event_id"],
        "columns": [("channel", "STRING"), ("event_id", "STRING"), ("order_id", "STRING"), ("user_id", "STRING"),
                    ("refund_id", "STRING"), ("payment_id", "STRING"), ("order_item_id", "STRING"),
                    ("refund_quantity", "INT"), ("refund_amount", "BIGINT"), ("refund_type", "STRING"),
                    ("refund_reason_code", "STRING"), ("refund_created_at", "TIMESTAMP"),
                    ("received_at", "TIMESTAMP")],
    },
    "quarantine": {
        "pk": ["eh_partition", "eh_offset"],
        "columns": [("channel", "STRING"), ("received_at", "TIMESTAMP"), ("payload", "STRING"),
                    ("eh_partition", "INT"), ("eh_offset", "BIGINT"), ("eh_enqueued_at", "TIMESTAMP"),
                    ("ingested_at", "TIMESTAMP"), ("reason_code", "STRING"), ("quarantined_at", "TIMESTAMP")],
    },
}


def column_names(table: str) -> list[str]:
    """테이블의 컬럼 이름을 순서대로 돌려준다."""
    return [name for name, _ in TABLES[table]["columns"]]


def create_table_sql(table: str, schema: str) -> str:
    """CREATE TABLE IF NOT EXISTS 문장을 만든다. schema는 "commerce_poc.silver" 같은 카탈로그.스키마."""
    columns = ", ".join(f"{name} {sql_type}" for name, sql_type in TABLES[table]["columns"])
    return f"CREATE TABLE IF NOT EXISTS {schema}.{table} ({columns})"


def create_tables(spark, schema: str) -> None:
    """Silver 테이블 6개를 만든다. 이미 있으면 그대로 둔다 (재실행해도 안전)."""
    for table in TABLES:
        spark.sql(create_table_sql(table, schema))
