"""Event Hubs → Bronze 적재 함수. 노트북은 이 함수들을 순서대로 호출만 한다.

흐름: kafka_options → read_events → parse_envelope → write_bronze
Event Hubs의 Kafka 호환 엔드포인트를 Spark Structured Streaming으로 읽는다.
pyspark는 Databricks에서만 쓰므로 함수 안에서 import한다 (로컬·CI 테스트가 pyspark 없이 돌게 하기 위함).

[입력] Kafka 소스 DataFrame의 컬럼 (Spark 고정 스키마)
  key binary, value binary, topic string, partition int, offset long, timestamp timestamp, timestampType int
  value = Function이 만든 봉투 JSON을 UTF-8 바이트로 쓴 것:
  {"channel":"A","received_at":"2026-10-12T02:09:45.123Z","payload":"<원본 JSON 문자열>"}

[출력] Bronze 테이블 컬럼 (append only, 중복·깨진 payload도 그대로)
  channel        string     봉투의 channel
  received_at    timestamp  봉투의 received_at (Function이 부여, UTC)
  payload        string     봉투의 payload. 해석하지 않고 문자열 그대로
  eh_partition   int        Kafka partition
  eh_offset      long       Kafka offset
  eh_enqueued_at timestamp  Kafka timestamp
  ingested_at    timestamp  적재 시각 (current_timestamp)
"""

def kafka_options(eventhub_fqdn: str, eventhub_name: str, service_credential: str, group_id: str | None = None) -> dict:
    """Event Hubs Kafka 소스 옵션을 만든다. Unity Catalog 서비스 자격 증명(액세스 커넥터)으로 인증한다.

    서비스 자격 증명을 쓸 때는 kafka.security.protocol, kafka.sasl.* 옵션을 함께 주지 않는다.
    """
    options = {
        "kafka.bootstrap.servers": f"{eventhub_fqdn}:9093",
        "subscribe": eventhub_name,
        "databricks.serviceCredential": service_credential,
        "startingOffsets": "earliest",
    }
    if group_id:
        options["kafka.group.id"] = group_id
    return options


def read_events(spark, options: dict):
    """kafka_options로 Event Hubs를 스트림으로 읽어 Kafka 소스 DataFrame을 돌려준다.

    구현: spark.readStream.format("kafka").options(**options).load()
    """
    events_df = spark.readStream.format("kafka").options(**options).load()
    return events_df


def parse_envelope(kafka_df):
    """Kafka 소스 DataFrame을 위 [출력] 컬럼 7개로 바꾼다.

    힌트: value를 string으로 cast한 뒤 봉투 스키마(channel, received_at, payload 모두 string)로 from_json,
    received_at은 to_timestamp, 나머지는 Kafka 컬럼 이름만 바꿔 가져온다.
    payload는 변환하지 않는다. (원본 보존 — 검증과 격리는 Silver 몫)
    """

    import pyspark.sql.functions as F

    envelope = F.from_json(F.col("value").cast("string"),
                           "channel string, received_at string, payload string")
    return kafka_df.select(
        envelope["channel"].alias("channel"),
        F.to_timestamp(envelope["received_at"]).alias("received_at"),
        envelope["payload"].alias("payload"),
        F.col("partition").alias("eh_partition"),
        F.col("offset").alias("eh_offset"),
        F.col("timestamp").alias("eh_enqueued_at"),
        F.current_timestamp().alias("ingested_at"),
    )


def write_bronze(df, table: str, checkpoint_path: str):
    """df를 Delta 테이블에 append하고, 쌓인 분량을 모두 처리하면 끝낸다.

    구현 힌트: writeStream.option("checkpointLocation", ...).trigger(availableNow=True).toTable(table)
    쿼리가 끝날 때까지 기다린다 (awaitTermination). 같은 체크포인트로 다시 실행하면 이미 읽은 offset은 읽지 않는다.
    """
    query = df.writeStream \
        .option("checkpointLocation", checkpoint_path).trigger(availableNow=True).toTable(table)
    query.awaitTermination()