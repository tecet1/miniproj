"""마이크로배치마다 "들어온 수 = Silver 반영 + 중복 제거 + 격리"가 성립하는지 확인하고 결과를 기록한다.

[언제] silver_stream의 foreachBatch가 MERGE를 끝낸 뒤 after_batch 콜백으로 부른다 (make_after_batch가 콜백을 만든다).
[세는 것] 배치 하나(converted: Bronze 행 + result 구조체)에 대해
  bronze_in         들어온 Bronze 행 수
  quarantine_found  격리 대상 중 Silver quarantine 테이블에 (eh_partition, eh_offset)으로 실제 있는 행 수
  unique_events     정상 행의 (channel, event_id) 종류 수
  events_found      그 종류 중 Silver 이벤트 테이블 4개(orders, order_cancellations, payments, refunds)에 실제 있는 수
  duplicates        정상 행 수 - unique_events (같은 배치 안의 중복 이벤트, 제거되었어야 하는 행)
  amount_in         정상 이벤트(중복 제거 뒤)의 결제금액 + 환불금액 합 (배치에서 읽은 값)
  amount_found      같은 이벤트의 Silver payments·refunds 금액 합 (Silver에 실제 쌓인 값)
[검사 1] 위반 건수 = bronze_in - (quarantine_found + events_found + duplicates). 0이어야 한다 (규칙 이름 silver_batch_balance).
[검사 2] 위반 건수 = |amount_in - amount_found|. 0이어야 한다 (규칙 이름 silver_batch_amounts). 건수가 같아도 금액이 달라지거나, 같은 배치를 다시 처리했는데 금액이 변하면 잡힌다 (금액 멱등성).
  - 반영이 빠지면(MERGE 누락, 필터 오류, 행이 어느 쪽에도 안 가는 경우) 양수가 된다.
  - 이미 Silver에 있던 이벤트(배치 사이 중복, 같은 배치 재처리)도 events_found에 잡히므로 재처리해도 같은 결과다 (멱등).
  - order_items는 이벤트 1건이 여러 행이 되어 이 식에 넣지 않는다 (ORDER_CREATED 이벤트가 orders에 있으면 반영된 것으로 본다).
[한계] 두 개의 Bronze 위치가 같은 (channel, event_id)를 가진 이벤트가 이전 배치에서 이미 반영된 경우도 events_found로 센다. 어느 배치에서 반영했는지는 구분하지 않는다.
Spark 함수(batch_counts, make_after_batch)는 로컬 Spark에서 DataFrame으로 확인했고 Databricks(Delta)에서는 아직 미검증이다.
"""
from datetime import datetime, timezone

from run_checks import RESULTS_TABLE, make_result, save_results

try:
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window
except ImportError:  # 로컬 테스트(pyspark 없음): 모듈만 불러온다
    F = Window = None

RULE_ID = "silver_batch_balance"
AMOUNT_RULE_ID = "silver_batch_amounts"
EVENT_TABLES = ("orders", "order_cancellations", "payments", "refunds")


def balance_violations(counts: dict) -> int:
    """위 [검사] 식. 반영·중복·격리로 설명되지 않는 행 수."""
    return counts["bronze_in"] - (counts["quarantine_found"] + counts["events_found"] + counts["duplicates"])


def amount_violations(counts: dict) -> int:
    """배치에서 읽은 금액 합과 Silver에 쌓인 금액 합의 차이(절대값)."""
    return abs(counts["amount_in"] - counts["amount_found"])


def has_rows(counts: dict) -> bool:
    """들어온 행이 있는 배치만 기록한다. 새 데이터가 없는 빈 배치(트리거만 돈 경우)는 results에 의미 없는 줄을 쌓지 않는다."""
    return counts["bronze_in"] > 0


def batch_result(counts: dict, batch_id: int, run_id: str, checked_at: datetime) -> dict:
    """집계 결과를 results 행으로 만든다. detail에 각 수를 남겨 어디서 어긋났는지 볼 수 있게 한다."""
    detail = ", ".join(f"{key}={counts[key]}" for key in
                       ("bronze_in", "quarantine_found", "unique_events", "events_found", "duplicates"))
    return make_result(RULE_ID, "silver", "batch", "batch", "error", 0, balance_violations(counts), run_id, checked_at,
                       None, batch_id, detail)


def amount_result(counts: dict, batch_id: int, run_id: str, checked_at: datetime) -> dict:
    """금액 합 검사를 results 행으로 만든다."""
    detail = f"amount_in={counts['amount_in']}, amount_found={counts['amount_found']}"
    return make_result(AMOUNT_RULE_ID, "silver", "batch", "batch", "error", 0, amount_violations(counts), run_id, checked_at,
                       None, batch_id, detail)


def batch_counts(converted_df, silver_event_keys_df, quarantine_keys_df, silver_amounts_df=None) -> dict:
    """배치의 일곱 수를 센다.

    converted_df          channel, eh_partition, eh_offset, result(event_id, reason_code ...)를 가진 배치
    silver_event_keys_df  Silver 이벤트 테이블 4개의 (channel, event_id)를 합친 것
    quarantine_keys_df    Silver quarantine의 (eh_partition, eh_offset)
    silver_amounts_df     Silver payments·refunds의 (channel, event_id, amount). 주면 금액 합도 센다 (없으면 0)
    """
    reason = F.col("result.reason_code")
    bronze_in = converted_df.count()
    quarantined = converted_df.where(reason.isNotNull()).select("eh_partition", "eh_offset")
    normal = converted_df.where(reason.isNull()).select("channel", F.col("result.event_id").alias("event_id"))
    unique = normal.distinct()
    unique_events = unique.count()
    amounts = {"amount_in": 0, "amount_found": 0}
    if silver_amounts_df is not None:
        window = Window.partitionBy("channel", "event_id").orderBy("received_at", "eh_partition", "eh_offset")
        first = (converted_df.where(reason.isNull())
                 .select("channel", F.col("result.event_id").alias("event_id"), "received_at", "eh_partition", "eh_offset",
                         F.coalesce(F.col("e.payment_amount"), F.col("e.refund_amount")).alias("amount"))
                 .where(F.col("amount").isNotNull())
                 .withColumn("row_num", F.row_number().over(window)).where(F.col("row_num") == 1))
        in_sum = first.agg(F.coalesce(F.sum("amount"), F.lit(0)).alias("s")).first()["s"]
        found_sum = (first.select("channel", "event_id")
                     .join(silver_amounts_df.withColumnRenamed("amount", "silver_amount"), ["channel", "event_id"])
                     .agg(F.coalesce(F.sum("silver_amount"), F.lit(0)).alias("s")).first()["s"])
        amounts = {"amount_in": int(in_sum), "amount_found": int(found_sum)}
    return {
        **amounts,
        "bronze_in": bronze_in,
        "quarantine_found": quarantined.join(quarantine_keys_df, ["eh_partition", "eh_offset"], "left_semi").count(),
        "unique_events": unique_events,
        "events_found": unique.join(silver_event_keys_df, ["channel", "event_id"], "left_semi").count(),
        "duplicates": normal.count() - unique_events,
    }


def silver_amounts(spark, schema: str):
    """Silver payments·refunds의 (channel, event_id, amount)."""
    payments = spark.table(f"{schema}.payments").select("channel", "event_id", F.col("payment_amount").alias("amount"))
    refunds = spark.table(f"{schema}.refunds").select("channel", "event_id", F.col("refund_amount").alias("amount"))
    return payments.unionByName(refunds)


def silver_event_keys(spark, schema: str):
    """Silver 이벤트 테이블 4개의 (channel, event_id)를 이어 붙인다."""
    frames = [spark.table(f"{schema}.{table}").select("channel", "event_id") for table in EVENT_TABLES]
    keys = frames[0]
    for frame in frames[1:]:
        keys = keys.unionByName(frame)
    return keys


def make_after_batch(schema: str, table: str = RESULTS_TABLE):
    """silver_stream.make_batch_handler에 넘길 콜백 (converted_df, batch_id)을 만든다. 결과를 results 테이블에 쌓는다."""
    def after_batch(converted_df, batch_id):
        spark = converted_df.sparkSession
        counts = batch_counts(converted_df, silver_event_keys(spark, schema),
                              spark.table(f"{schema}.quarantine").select("eh_partition", "eh_offset"), silver_amounts(spark, schema))
        if not has_rows(counts):
            return
        now = datetime.now(timezone.utc)
        run_id = f"batch-{now.strftime('%Y%m%dT%H%M%S')}"
        save_results(spark, [batch_result(counts, batch_id, run_id, now), amount_result(counts, batch_id, run_id, now)], table)

    return after_batch
