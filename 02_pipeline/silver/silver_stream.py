"""Bronze -> Silver 적재 함수. 노트북은 run_silver만 호출한다. (Databricks에서만 실행 가능 — Spark 동작은 미검증)

흐름 (foreachBatch 안에서 배치 하나마다)
  Bronze 배치 -> convert_bronze(UDF로 행마다 convert_row)
    ├─ result.reason_code 있음 -> build_quarantine -> merge_insert(quarantine)
    └─ 없음 -> dedupe_events((channel, event_id)당 최초 1건) -> parse_events(event_json 읽기)
                 -> build_orders / build_order_items / build_cancellations / build_payments / build_refunds
                 -> 각각 merge_insert (insert-only MERGE)
pyspark는 Databricks에서만 쓰므로 없어도 모듈을 불러올 수 있게 try/except로 import한다 (로컬·CI 테스트가 pyspark 없이 돌게 하기 위함).

[설계 근거] plans/03_Silver_변환.md 설계 결정 1(UDF), 2(foreachBatch + insert-only MERGE), 5(최초 건 유지)
  - 중복 제거 순서: 이벤트 단위 (channel, event_id)를 먼저 제거한 뒤 테이블로 나눈다. 최초 건 = received_at, eh_partition, eh_offset 오름차순.
  - MERGE는 WHEN NOT MATCHED THEN INSERT만 쓴다. 이미 있는 키는 건드리지 않아 foreachBatch가 재시도돼도 행이 늘지 않는다.
  - 같은 배치 안의 중복은 MERGE 전에 dedupe_events가 이미 없앤다. (배치 사이의 중복은 MERGE가 막는다)
  - 시각: event_time은 ISO 8601 UTC 문자열이므로 F.to_timestamp로 바꾼다.
  - 컬럼 이름과 순서는 silver_tables.TABLES를 따른다 (명세 event_spec.md 1.3절). 공통 모델 필드명 -> 테이블 컬럼명 변환은 여기서 한다:
      event_time -> {orders: order_created_at, order_cancellations: cancel_created_at, payments: payment_created_at,
                     refunds: refund_created_at}, fail_reason_code -> payment_fail_reason_code,
      payments.status는 event_type에서 만든다 (SUCCEEDED / FAILED), 나머지는 같은 이름이다.
  - DataFrame.persist()는 쓰지 않는다. 서버리스 컴퓨트에서 지원되지 않을 수 있고, UDF가 결정적이라 여러 번 돌아도 결과는 같다
    (성능만 손해). 대량 부하 실험(04_experiments)에서 클래식 클러스터 + persist를 다시 검토한다.
"""
try:
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window
except ImportError:  # 로컬 테스트(pyspark 없음): 모듈만 불러오고 Spark 함수는 호출하지 않는다
    F = Window = None

from convert_row import convert_row
from silver_tables import EVENT_SCHEMA, TABLES, column_names

BRONZE_COLUMNS = ["channel", "received_at", "payload", "eh_partition", "eh_offset", "eh_enqueued_at", "ingested_at"]
RESULT_SCHEMA = "event_id string, event_type string, event_json string, reason_code string"


def read_bronze(spark, table: str):
    """Bronze 테이블을 스트림으로 읽는다. channel이 있는 행만 남긴다 (봉투가 없는 이전 테스트 메시지 제외).

    어디까지 읽었는지는 체크포인트가 기억한다. 시각으로 범위를 자르지 않는다.
    조건은 bronze_check.load_bronze_pairs, silver_check.count_bronze와 같다.
    """
    return spark.readStream.table(table).where(F.col("channel").isNotNull())


def convert_bronze(bronze_df, known_skus: set):
    """Bronze 행마다 convert_row를 돌려 결과를 struct 컬럼 `result`로 붙인다.

    lambda가 known_skus를 기억(클로저)하므로 UDF는 컬럼 두 개(channel, payload)만 받는다.
    convert_row는 예외를 내지 않는다 (실패는 reason_code로 돌려준다).
    """
    convert = F.udf(lambda channel, payload: convert_row(channel, payload, known_skus), RESULT_SCHEMA)
    return bronze_df.withColumn("result", convert("channel", "payload"))


def build_quarantine(converted_df):
    """result.reason_code가 있는 행을 quarantine 컬럼(BRONZE_COLUMNS + reason_code + quarantined_at)으로 바꾼다.

    Bronze 7컬럼은 값을 바꾸지 않고 그대로 옮긴다 (원본 payload와 Bronze 위치 보존).
    """
    return converted_df.where(F.col("result.reason_code").isNotNull()).select(
        *[F.col(c) for c in BRONZE_COLUMNS],
        F.col("result.reason_code").alias("reason_code"),
        F.current_timestamp().alias("quarantined_at"),
    )


def dedupe_events(converted_df):
    """정상 행(reason_code 없음)에서 (channel, event_id)마다 최초 1건만 남긴다.

    최초 건 = received_at, eh_partition, eh_offset 오름차순의 첫 행. result 안의 값은 event_id, event_type,
    event_json 컬럼으로 풀어 둔다.
    """
    window = Window.partitionBy("channel", F.col("result.event_id")).orderBy("received_at", "eh_partition", "eh_offset")
    return (converted_df
            .where(F.col("result.reason_code").isNull())
            .withColumn("row_num", F.row_number().over(window))
            .where(F.col("row_num") == 1)
            .select(*[F.col(c) for c in BRONZE_COLUMNS],
                    F.col("result.event_id").alias("event_id"),
                    F.col("result.event_type").alias("event_type"),
                    F.col("result.event_json").alias("event_json")))


def parse_events(deduped_df):
    """event_json을 EVENT_SCHEMA로 읽어 struct 컬럼 `e`로 만든다. 출력 컬럼: channel, received_at, e."""
    return deduped_df.select("channel", "received_at", F.from_json("event_json", EVENT_SCHEMA).alias("e"))


def ev(name: str):
    """공통 이벤트 struct `e`의 필드를 Column으로 꺼낸다. ev("order_id") == F.col("e.order_id")"""
    return F.col(f"e.{name}")


def select_table(df, table: str, columns: dict):
    """columns(컬럼 이름 -> Column 식)를 silver_tables.TABLES의 컬럼 순서대로 select한다.

    표에 있는 컬럼이 columns에 없으면 KeyError가 나므로 빠뜨린 컬럼을 바로 알 수 있다.
    """
    return df.select(*[columns[name].alias(name) for name in column_names(table)])


def build_orders(events_df):
    """ORDER_CREATED 이벤트 1건 -> orders 1행."""
    return select_table(events_df.where(ev("event_type") == "ORDER_CREATED"), "orders", {
        "channel": F.col("channel"),
        "event_id": ev("event_id"),
        "order_id": ev("order_id"),
        "user_id": ev("user_id"),
        "shipping_fee": ev("shipping_fee"),
        "order_created_at": F.to_timestamp(ev("event_time")),
        "received_at": F.col("received_at"),
    })


def build_order_items(events_df):
    """ORDER_CREATED 이벤트의 items[] 한 건 -> order_items 1행. received_at은 이 테이블에 없다."""
    items = events_df.where(ev("event_type") == "ORDER_CREATED").select(
        "channel", ev("order_id").alias("order_id"), F.explode(ev("items")).alias("item"))
    return select_table(items, "order_items", {
        "channel": F.col("channel"),
        "order_item_id": F.col("item.order_item_id"),
        "order_id": F.col("order_id"),
        "product_id": F.col("item.product_id"),
        "product_name": F.col("item.product_name"),
        "category": F.col("item.category"),
        "quantity": F.col("item.quantity"),
        "unit_price": F.col("item.unit_price"),
        "item_discount": F.col("item.item_discount"),
    })


def build_cancellations(events_df):
    """ORDER_CANCELLED -> order_cancellations. event_time은 cancel_created_at이 된다."""
    return select_table(events_df.where(ev("event_type") == "ORDER_CANCELLED"), "order_cancellations", {
        "channel": F.col("channel"),
        "event_id": ev("event_id"),
        "order_id": ev("order_id"),
        "user_id": ev("user_id"),
        "cancel_reason_code": ev("cancel_reason_code"),
        "cancel_created_at": F.to_timestamp(ev("event_time")),
        "received_at": F.col("received_at"),
    })


def build_payments(events_df):
    """PAYMENT_SUCCEEDED / PAYMENT_FAILED -> payments. status는 event_type에서 만든다."""
    payments = events_df.where(ev("event_type").isin("PAYMENT_SUCCEEDED", "PAYMENT_FAILED"))
    return select_table(payments, "payments", {
        "channel": F.col("channel"),
        "event_id": ev("event_id"),
        "order_id": ev("order_id"),
        "user_id": ev("user_id"),
        "payment_id": ev("payment_id"),
        "status": F.when(ev("event_type") == "PAYMENT_SUCCEEDED", "SUCCEEDED").otherwise("FAILED"),
        "payment_method": ev("payment_method"),
        "payment_amount": ev("payment_amount"),
        "payment_fail_reason_code": ev("fail_reason_code"),
        "payment_created_at": F.to_timestamp(ev("event_time")),
        "received_at": F.col("received_at"),
    })


def build_refunds(events_df):
    """REFUND_SUCCEEDED -> refunds. event_time은 refund_created_at이 된다."""
    return select_table(events_df.where(ev("event_type") == "REFUND_SUCCEEDED"), "refunds", {
        "channel": F.col("channel"),
        "event_id": ev("event_id"),
        "order_id": ev("order_id"),
        "user_id": ev("user_id"),
        "refund_id": ev("refund_id"),
        "payment_id": ev("payment_id"),
        "order_item_id": ev("order_item_id"),
        "refund_quantity": ev("refund_quantity"),
        "refund_amount": ev("refund_amount"),
        "refund_type": ev("refund_type"),
        "refund_reason_code": ev("refund_reason_code"),
        "refund_created_at": F.to_timestamp(ev("event_time")),
        "received_at": F.col("received_at"),
    })


BUILDERS = {
    "orders": build_orders,
    "order_items": build_order_items,
    "order_cancellations": build_cancellations,
    "payments": build_payments,
    "refunds": build_refunds,
}


def merge_insert(df, table: str, keys: list[str]) -> None:
    """df를 table에 insert-only MERGE한다. 키가 이미 있으면 아무것도 하지 않는다.

    foreachBatch 안에서도 df.sparkSession을 쓰면 임시 뷰와 SQL이 같은 세션에서 만난다.
    뷰 이름은 테이블마다 다르게 둔다 (batch_orders 등).
    """
    view = "batch_" + table.split(".")[-1]
    df.createOrReplaceTempView(view)
    condition = " AND ".join(f"t.{key} = s.{key}" for key in keys)
    df.sparkSession.sql(
        f"MERGE INTO {table} AS t USING {view} AS s ON {condition} "
        f"WHEN NOT MATCHED THEN INSERT *"
    )


def make_batch_handler(schema: str, known_skus: set, after_batch=None):
    """foreachBatch에 넘길 함수 (batch_df, batch_id)를 만든다. 위 흐름 그림대로 처리한다.

    schema와 known_skus는 handle이 기억(클로저)한다. 격리 -> 정상 5개 테이블 순서로 MERGE한다.
    after_batch(converted, batch_id)가 있으면 MERGE가 모두 끝난 뒤 부른다 (증분 검사 기록: quality/batch_check.py).
    """
    def handle(batch_df, batch_id):
        converted = convert_bronze(batch_df, known_skus)
        merge_insert(build_quarantine(converted), f"{schema}.quarantine", TABLES["quarantine"]["pk"])
        events = parse_events(dedupe_events(converted))
        for table, build in BUILDERS.items():
            merge_insert(build(events), f"{schema}.{table}", TABLES[table]["pk"])
        if after_batch:
            after_batch(converted, batch_id)

    return handle


def run_silver(spark, bronze_table: str, schema: str, checkpoint_path: str, known_skus: set, after_batch=None) -> None:
    """Bronze를 읽어 foreachBatch로 Silver에 쓰고, 쌓인 분량을 모두 처리하면 끝낸다.

    같은 체크포인트로 다시 실행하면 새 Bronze 행만 읽는다. 처음부터 다시 읽어 멱등을 확인하려면 새 체크포인트 경로를 쓴다.
    """
    query = (read_bronze(spark, bronze_table)
             .writeStream
             .foreachBatch(make_batch_handler(schema, known_skus, after_batch))
             .option("checkpointLocation", checkpoint_path)
             .trigger(availableNow=True)
             .start())
    query.awaitTermination()
