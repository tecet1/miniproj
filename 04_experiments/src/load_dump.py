"""덤프 이벤트 줄을 Bronze 테이블에 직접 넣는다 (Event Hubs를 거치지 않는다). 노트북은 load_dump만 호출한다.

흐름: read_dump_lines → build_rows → load_dump(DataFrame 만들기 → insert-only MERGE)
이 덤프는 이 실험에 한정하며 일반 Backfill 기능이 아니다 (plans/06 설계 결정 2).
pyspark는 load_dump 안에서만 import한다 (로컬·CI 테스트가 pyspark 없이 돌게 하기 위함).

[입력] results/dump/{A,B,C}/events.jsonl (build_dump.py 산출물). 한 줄 = {"channel": "A", "payload": "<원본 JSON 문자열>"}
[출력] Bronze 7컬럼 행 (bronze_stream.py의 [출력]과 같은 이름·타입). 합성 행 규칙(plans/06 카드 1-C):
  channel        줄의 channel
  received_at    이벤트의 event_time(UTC). payload에서 읽는다 (채널마다 형식이 다르다: A ISO, B epoch ms, C KST 문자열)
  payload        줄의 payload. 해석하지 않고 그대로
  eh_partition   -1 (Event Hubs에서 온 행은 0 이상이므로 겹치지 않는다)
  eh_offset      A → B → C 채널, 줄 순서로 0부터 이어 매긴 전역 일련번호. 중복 전송 줄도 줄마다 다르다
  eh_enqueued_at received_at과 같다
  ingested_at    적재 시각
같은 덤프를 다시 만들면 (eh_partition, eh_offset)가 같아서, 다시 적재해도 insert-only MERGE가 행을 늘리지 않는다.
"""
import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

CHANNELS = ("A", "B", "C")
SYNTHETIC_PARTITION = -1
KST = timezone(timedelta(hours=9))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
COLUMNS = ("channel", "received_at", "payload", "eh_partition", "eh_offset", "eh_enqueued_at", "ingested_at")


def read_dump_lines(dump_dir) -> dict[str, list[str]]:
    """dump_dir/{A,B,C}/events.jsonl의 줄 목록을 채널별로 읽는다 (줄 순서 그대로, 빈 줄은 없다)."""
    return {channel: (Path(dump_dir) / channel / "events.jsonl").read_text(encoding="utf-8").splitlines()
            for channel in CHANNELS}


def event_time(channel: str, payload: str) -> datetime:
    """원본 payload에서 이벤트 시각(UTC, aware)을 읽는다. 채널별 필드: A occurred_at, B occurredAtMs, C evt_dt(KST)."""
    raw = json.loads(payload)
    if channel == "A":
        return datetime.fromisoformat(raw["occurred_at"]).astimezone(UTC)
    if channel == "B":
        return EPOCH + timedelta(milliseconds=raw["occurredAtMs"])
    if channel == "C":
        return datetime.strptime(raw["evt_dt"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST).astimezone(UTC)
    raise ValueError(f"알 수 없는 채널: {channel}")


def build_rows(lines_by_channel: dict[str, list[str]], ingested_at: datetime) -> list[dict]:
    """채널별 줄 목록을 합성 Bronze 행 목록으로 바꾼다. 순서는 A → B → C, 줄 순서이고 offset은 0부터 하나씩 늘어난다."""
    rows, offset = [], 0
    for channel in CHANNELS:
        for line in lines_by_channel[channel]:
            envelope = json.loads(line)
            if envelope["channel"] != channel:
                raise ValueError(f"{channel} 디렉터리에 {envelope['channel']} 줄이 있다")
            received_at = event_time(channel, envelope["payload"])
            rows.append({"channel": channel, "received_at": received_at, "payload": envelope["payload"],
                         "eh_partition": SYNTHETIC_PARTITION, "eh_offset": offset,
                         "eh_enqueued_at": received_at, "ingested_at": ingested_at})
            offset += 1
    return rows


def merge_sql(table: str, source_view: str) -> str:
    """같은 (eh_partition, eh_offset)가 이미 있으면 건드리지 않고, 없는 행만 넣는 MERGE 문."""
    return (f"MERGE INTO {table} AS t USING {source_view} AS s "
            "ON t.eh_partition = s.eh_partition AND t.eh_offset = s.eh_offset "
            "WHEN NOT MATCHED THEN INSERT *")


def load_dump(spark, table: str, dump_dir, ingested_at: datetime | None = None, source_view: str = "dump_rows") -> int:
    """덤프를 읽어 table(Bronze)에 insert-only로 넣고, 만든 행 수(= 덤프 줄 수)를 돌려준다. 테이블이 이미 있어야 한다."""
    from pyspark.sql.types import IntegerType, LongType, StringType, StructField, StructType, TimestampType

    schema = StructType([
        StructField("channel", StringType()), StructField("received_at", TimestampType()),
        StructField("payload", StringType()), StructField("eh_partition", IntegerType()),
        StructField("eh_offset", LongType()), StructField("eh_enqueued_at", TimestampType()),
        StructField("ingested_at", TimestampType()),
    ])
    rows = build_rows(read_dump_lines(dump_dir), ingested_at or datetime.now(UTC))
    spark.createDataFrame([tuple(row[c] for c in COLUMNS) for row in rows], schema).createOrReplaceTempView(source_view)
    spark.sql(merge_sql(table, source_view))
    return len(rows)
