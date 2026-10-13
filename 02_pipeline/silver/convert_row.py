"""Bronze 행 하나(channel, payload)를 Spark UDF가 돌려줄 결과 한 건으로 바꾼다. Spark를 모르는 순수 함수다.

[입력] channel, payload(원본 JSON 문자열), known_skus(기준 SKU 집합)
[출력] dict 네 키. UDF 반환 스키마 "event_id string, event_type string, event_json string, reason_code string"과 같다.
  성공: {"event_id": 이벤트 ID, "event_type": 공통 이벤트 유형, "event_json": 공통 이벤트 JSON 문자열, "reason_code": None}
  실패: {"event_id": None, "event_type": None, "event_json": None, "reason_code": 격리 사유}
[순서] silver_row_to_event.to_common_event(행 단위 변환·값 범위 검증) -> validate_skus.validate_known_skus
       ConversionError가 나면 예외를 밖으로 내지 않고 실패 결과로 바꾼다 (UDF가 예외로 죽으면 배치 전체가 실패한다).
[event_json] 공통 이벤트 dict를 JSON으로 쓴 문자열이다. datetime(event_time)은 JSON으로 쓸 수 없으므로
  "2026-10-12T01:00:00.000Z" 형식의 UTC 문자열로 바꾼다. 한글이 깨지지 않게 ensure_ascii=False를 쓴다.
  Spark는 이 문자열을 silver_tables.EVENT_SCHEMA로 읽는다.
이 함수는 중복 제거나 테이블 분배를 하지 않는다.
"""
import json

from conversion_error import ConversionError
from silver_row_to_event import to_common_event
from validate_skus import validate_known_skus


def event_to_json(event: dict) -> str:
    """공통 이벤트 dict를 JSON 문자열로 쓴다. event_time(UTC aware datetime)을 위 형식의 문자열로 바꾼다.

    힌트: event["event_time"].isoformat(timespec="milliseconds")는 "...+00:00"으로 끝나므로 "Z"로 바꾼다.
    원본 event는 바꾸지 말고 복사본(dict(event))을 고친다.
    """
    event_copy = dict(event)
    event_copy["event_time"] = event_copy["event_time"].isoformat(timespec="milliseconds").replace("+00:00", "Z")
    return json.dumps(event_copy, ensure_ascii=False)

def convert_row(channel, payload, known_skus) -> dict:
    """위 [순서]대로 처리해 성공 또는 실패 결과를 돌려준다.

    힌트: try 안에서 to_common_event -> validate_known_skus, except ConversionError as error 에서 error.reason_code 사용.
    """
    try:
        event = to_common_event(channel, payload)
        validate_known_skus(event, known_skus)
        return {
            "event_id": event["event_id"],
            "event_type": event["event_type"],
            "event_json": event_to_json(event),
            "reason_code": None,
        }
    except ConversionError as error:
        return {
            "event_id": None,
            "event_type": None,
            "event_json": None,
            "reason_code": error.reason_code,
        }
