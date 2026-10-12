"""A 채널(자사몰) 원본 dict -> 공통 이벤트 dict. 생성기의 channel_a.to_raw의 반대 방향이다.

[입력] json.loads로 읽은 원본 dict 하나 (Bronze payload 한 건 = 이벤트 한 건)
  공통: event_id, event_type, occurred_at(ISO 8601 UTC, 예 "2026-10-12T01:23:45.123Z"), order_id, user_id(비로그인 null)
  order.created      shipping_fee, items[{order_item_id, sku, product_name, category, quantity, unit_price, discount_amount}]
  order.cancelled    reason
  payment.succeeded  payment_id, method, amount
  payment.failed     payment_id, method, amount, failure_reason
  refund.succeeded   refund_id, payment_id, order_item_id, refund_quantity, amount, type, reason
  (이벤트 명세 2.1~2.3절의 매핑표를 따른다. 코드 값은 생성기 channel_a.py의 상수표와 같은 값이다)

[출력] 공통 이벤트 dict (이벤트 명세 1절)
  공통: event_id, event_type, channel("A"), order_id, user_id, event_time(UTC aware datetime)
  ORDER_CREATED     shipping_fee, items[{order_item_id, product_id, product_name, category, quantity, unit_price, item_discount}]
  ORDER_CANCELLED   cancel_reason_code
  PAYMENT_*         payment_id, payment_method, payment_amount, fail_reason_code (성공이면 None)
  REFUND_SUCCEEDED  refund_id, payment_id, order_item_id, refund_quantity, refund_amount, refund_type, refund_reason_code

[오류] 변환할 수 없으면 ConversionError(reason_code)를 낸다.
  UNKNOWN_EVENT_TYPE  event_type이 매핑표에 없음
  MISSING_REQUIRED    필수 필드 누락 (fields.require 사용)
  INVALID_TYPE        정수 필드가 정수가 아님 (fields.as_int 사용)
  사유·실패 코드가 매핑에 없으면 오류가 아니라 "OTHER"로 둔다 (명세 2.3절)
  값 범위 검증(음수 금액 등 INVALID_VALUE)은 이 파일이 아니라 공통 검증이 한다
"""
from conversion_error import ConversionError
from fields import as_int, require

CHANNEL = "A"


def to_event(raw: dict) -> dict:
    """원본 dict 하나를 공통 이벤트 dict로 바꾼다.

    구현 힌트
      - 매핑 상수(이벤트 코드, 결제수단, 사유, 실패 사유, 환불 유형)는 파일 위쪽에 표로 둔다
      - 공통 필드 만들기, 이벤트 종류별 필드 만들기를 작은 함수로 나누고 event_type 코드로 고른다
      - 시각 변환은 occurred_at 문자열 -> UTC datetime (생성기 timeutil.to_iso_utc의 반대)
    """
    raise NotImplementedError
