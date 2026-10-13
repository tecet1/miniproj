"""Bronze -> Silver 적재 함수. 노트북은 run_silver만 호출한다. (Databricks에서만 실행 가능 — 로컬 테스트 없음, 미검증)

흐름 (foreachBatch 안에서 배치 하나마다)
  Bronze 배치 -> convert_bronze(UDF로 행마다 convert_row) -> persist
    ├─ result.reason_code 있음 -> build_quarantine -> merge_insert(quarantine)
    └─ 없음 -> dedupe_events((channel, event_id)당 최초 1건) -> parse_events(event_json 읽기)
                 -> build_orders / build_order_items / build_cancellations / build_payments / build_refunds
                 -> 각각 merge_insert (insert-only MERGE)
pyspark는 Databricks에서만 쓰므로 함수 안에서 import한다 (로컬·CI 테스트가 pyspark 없이 돌게 하기 위함).

[설계 근거] plans/03_Silver_변환.md 설계 결정 1(UDF), 2(foreachBatch + insert-only MERGE), 5(최초 건 유지)
  - 중복 제거 순서: 이벤트 단위 (channel, event_id)를 먼저 제거한 뒤 테이블로 나눈다. 최초 건 = received_at, eh_partition, eh_offset 오름차순.
  - MERGE는 WHEN NOT MATCHED THEN INSERT만 쓴다. 이미 있는 키는 건드리지 않아 foreachBatch가 재시도돼도 행이 늘지 않는다.
  - 같은 배치 안의 중복은 MERGE 전에 dedupe_events가 이미 없앤다. (배치 사이의 중복은 MERGE가 막는다)
  - 시각: event_time은 ISO 8601 UTC 문자열이므로 F.to_timestamp로 바꾼다.
  - 컬럼 이름과 순서는 silver_tables.TABLES를 따른다 (명세 event_spec.md 1.3절). 공통 모델 필드명 -> 테이블 컬럼명 변환은 여기서 한다:
      event_time -> {orders: order_created_at, order_cancellations: cancel_created_at, payments: payment_created_at,
                     refunds: refund_created_at}, fail_reason_code -> payment_fail_reason_code,
      item_discount/unit_price 등 나머지는 같은 이름, payments.status는 event_type에서 만든다 (SUCCEEDED / FAILED).
"""
from convert_row import convert_row
from silver_tables import EVENT_SCHEMA, TABLES, column_names

BRONZE_COLUMNS = ["channel", "received_at", "payload", "eh_partition", "eh_offset", "eh_enqueued_at", "ingested_at"]
RESULT_SCHEMA = "event_id string, event_type string, event_json string, reason_code string"


def read_bronze(spark, table: str, since: str):
    """Bronze 테이블을 스트림으로 읽는다. channel이 있고 received_at이 since 이후인 행만 남긴다.

    힌트: spark.readStream.table(table).where(channel이 null이 아님 & received_at >= F.to_timestamp(F.lit(since)))
    조건은 bronze_check.load_bronze_pairs와 같다 (봉투가 없는 이전 테스트 메시지 제외).
    """
    raise NotImplementedError("TODO")


def convert_bronze(bronze_df, known_skus: set):
    """Bronze 행마다 convert_row를 돌려 결과를 struct 컬럼 `result`로 붙인다.

    힌트: convert = F.udf(lambda channel, payload: convert_row(channel, payload, known_skus), RESULT_SCHEMA)
          bronze_df.withColumn("result", convert("channel", "payload"))
    주의: convert_row는 예외를 내지 않는다 (실패는 reason_code로 돌려준다).
    """
    raise NotImplementedError("TODO")


def build_quarantine(converted_df):
    """result.reason_code가 있는 행을 quarantine 컬럼(BRONZE_COLUMNS + reason_code + quarantined_at)으로 바꾼다.

    힌트: where(F.col("result.reason_code").isNotNull()), reason_code는 alias, quarantined_at은 F.current_timestamp().
    Bronze 7컬럼은 값을 바꾸지 않고 그대로 옮긴다 (원본 payload와 Bronze 위치 보존).
    """
    raise NotImplementedError("TODO")


def dedupe_events(converted_df):
    """정상 행(reason_code 없음)에서 (channel, event_id)마다 최초 1건만 남긴다.

    힌트: Window.partitionBy("channel", F.col("result.event_id")).orderBy("received_at", "eh_partition", "eh_offset"),
          F.row_number().over(window) == 1 인 행만 남기고 보조 컬럼은 버린다.
    """
    raise NotImplementedError("TODO")


def parse_events(deduped_df):
    """result.event_json을 EVENT_SCHEMA로 읽어 struct 컬럼 `e`로 만든다. 출력 컬럼: channel, received_at, e.

    힌트: F.from_json("result.event_json", EVENT_SCHEMA).alias("e")
    """
    raise NotImplementedError("TODO")


def build_orders(events_df):
    """ORDER_CREATED 이벤트 1건 -> orders 1행. 컬럼과 순서는 column_names("orders")를 따른다.

    힌트: where(F.col("e.event_type") == "ORDER_CREATED"), select(... F.to_timestamp("e.event_time").alias("order_created_at") ...)
    """
    raise NotImplementedError("TODO")


def build_order_items(events_df):
    """ORDER_CREATED 이벤트의 items[] 한 건 -> order_items 1행. 컬럼과 순서는 column_names("order_items").

    힌트: F.explode("e.items").alias("item")로 펼친 뒤 item.order_item_id, item.product_id ... 를 꺼낸다.
    order_id는 이벤트(e.order_id)에서 가져온다. received_at은 이 테이블에 없다.
    """
    raise NotImplementedError("TODO")


def build_cancellations(events_df):
    """ORDER_CANCELLED -> order_cancellations. event_time은 cancel_created_at이 된다."""
    raise NotImplementedError("TODO")


def build_payments(events_df):
    """PAYMENT_SUCCEEDED / PAYMENT_FAILED -> payments. status는 event_type에서 만든다.

    힌트: F.when(F.col("e.event_type") == "PAYMENT_SUCCEEDED", "SUCCEEDED").otherwise("FAILED")
    e.fail_reason_code는 payment_fail_reason_code가 된다 (성공이면 null), event_time은 payment_created_at.
    """
    raise NotImplementedError("TODO")


def build_refunds(events_df):
    """REFUND_SUCCEEDED -> refunds. event_time은 refund_created_at이 된다."""
    raise NotImplementedError("TODO")


BUILDERS = {
    "orders": build_orders,
    "order_items": build_order_items,
    "order_cancellations": build_cancellations,
    "payments": build_payments,
    "refunds": build_refunds,
}


def merge_insert(df, table: str, keys: list[str]) -> None:
    """df를 table에 insert-only MERGE한다. 키가 이미 있으면 아무것도 하지 않는다.

    힌트: df.createOrReplaceTempView(뷰 이름) 후 df.sparkSession.sql(
            f"MERGE INTO {table} AS t USING {뷰 이름} AS s ON t.k1 = s.k1 AND t.k2 = s.k2 WHEN NOT MATCHED THEN INSERT *")
    foreachBatch 안에서는 batch_df.sparkSession을 쓴다. 뷰 이름은 테이블마다 다르게 둔다.
    """
    raise NotImplementedError("TODO")


def make_batch_handler(schema: str, known_skus: set):
    """foreachBatch에 넘길 함수 (batch_df, batch_id)를 만든다. 위 흐름 그림대로 처리한다.

    힌트: converted = convert_bronze(batch_df, known_skus).persist()  (UDF를 한 번만 돌리려고 저장, 끝나면 unpersist)
          quarantine -> merge_insert(df, f"{schema}.quarantine", TABLES["quarantine"]["pk"])
          events = parse_events(dedupe_events(converted)).persist()
          for table, build in BUILDERS.items(): merge_insert(build(events), f"{schema}.{table}", TABLES[table]["pk"])
    """
    raise NotImplementedError("TODO")


def run_silver(spark, bronze_table: str, schema: str, checkpoint_path: str, known_skus: set, since: str) -> None:
    """Bronze를 읽어 foreachBatch로 Silver에 쓰고, 쌓인 분량을 모두 처리하면 끝낸다.

    힌트: read_bronze(...).writeStream.foreachBatch(make_batch_handler(schema, known_skus))
          .option("checkpointLocation", checkpoint_path).trigger(availableNow=True).start() 후 awaitTermination().
    같은 체크포인트로 다시 실행하면 새 Bronze 행만 읽는다. 처음부터 다시 읽어 멱등을 확인하려면 새 체크포인트 경로를 쓴다.
    """
    raise NotImplementedError("TODO")
