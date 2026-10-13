"""Bronze 행 하나(channel, payload 문자열)를 공통 이벤트 한 건으로 바꾼다. Spark UDF가 이 함수를 호출한다.

[입력] channel: "A" | "B" | "C" 가 아닐 수도 있는 문자열, payload: 원본 JSON 문자열
[출력] 공통 이벤트 dict (channels/channel_*.to_event의 출력과 같은 모양)
[오류] 변환할 수 없으면 ConversionError(reason_code). 순서와 사유는 다음과 같다.
  1. channel이 A/B/C가 아님          UNKNOWN_CHANNEL   (payload는 보지 않는다)
  2. payload가 JSON이 아님           MALFORMED_JSON
     JSON이지만 객체(dict)가 아님     MALFORMED_JSON    (예: 숫자, 배열)
  3. 채널 변환 실패                   UNKNOWN_EVENT_TYPE / MISSING_REQUIRED / INVALID_TYPE / INVALID_VALUE(환불 유형)
  4. 값 범위 검사 실패                INVALID_VALUE     (validate_values.validate_event_values)
이 함수는 중복 제거나 테이블 분배를 하지 않는다. 한 행 안에서 판단되는 일만 한다.
"""
import json

from channels import channel_a, channel_b, channel_c
from conversion_error import ConversionError
from validate_values import validate_event_values

# 채널 -> 채널 변환 함수
CONVERTERS = {
    "A": channel_a.to_event,
    "B": channel_b.to_event,
    "C": channel_c.to_event,
}


def parse_payload(payload: str) -> dict:
    """JSON 문자열을 dict로 읽는다. 읽을 수 없거나 객체가 아니면 MALFORMED_JSON.

    힌트: json.loads가 내는 오류는 ValueError(JSONDecodeError 포함). payload가 None이면 TypeError가 난다.
    """
    try:
        obj = json.loads(payload)
    except (ValueError, TypeError):
        raise ConversionError("MALFORMED_JSON", payload)
    if not isinstance(obj, dict):
        raise ConversionError("MALFORMED_JSON", payload)
    return obj

def to_common_event(channel: str, payload: str) -> dict:
    """위 순서대로 처리해 공통 이벤트를 돌려준다."""
    # 1. channel이 A/B/C가 아님
    if channel not in CONVERTERS:
        raise ConversionError("UNKNOWN_CHANNEL", payload)

    # 2. payload가 JSON이 아님 또는 객체가 아님
    obj = parse_payload(payload)

    # 3. 채널 변환 실패
    event = CONVERTERS[channel](obj) 

    # 4. 값 범위 검사 실패
    validate_event_values(event)
    return event
