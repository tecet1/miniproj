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
  MISSING_REQUIRED    필수 필드 누락 (fields.get_required 사용)
  INVALID_TYPE        정수 필드가 정수가 아님 (fields.get_required_int 사용)
  결제수단·사유·실패 코드가 매핑에 없으면 오류가 아니라 "OTHER"로 둔다 (명세 2.3절)
  INVALID_VALUE   환불 유형(type)이 cancel/return이 아님 (OTHER가 없는 닫힌 집합)
  값 범위 검증(음수 금액 등 INVALID_VALUE)은 이 파일이 아니라 공통 검증이 한다
"""
from datetime import UTC, datetime

from conversion_error import ConversionError
from fields import get_required, get_required_int, map_code

CHANNEL = "A"

# ── 매핑 상수: 원본 코드 -> 공통 값 (이벤트 명세 2.1, 2.3절) ─────────────────────────────
EVENT_TYPES = {
    "order.created": "ORDER_CREATED",
    "order.cancelled": "ORDER_CANCELLED",
    "payment.succeeded": "PAYMENT_SUCCEEDED",
    "payment.failed": "PAYMENT_FAILED",
    "refund.succeeded": "REFUND_SUCCEEDED",
}
PAYMENT_METHODS = {"card": "CARD", "easy_pay": "EASY_PAY", "bank": "BANK_TRANSFER", "vbank": "VIRTUAL_ACCOUNT"}
REASONS = {"change_of_mind": "CHANGE_OF_MIND", "defect": "DEFECT", "wrong_item": "WRONG_ITEM", "delay": "DELAY"}
FAIL_REASONS = {"limit_exceeded": "LIMIT_EXCEEDED", "declined": "CARD_DECLINED", "timeout": "TIMEOUT"}
REFUND_TYPES = {"cancel": "CANCEL", "return": "RETURN"}   # OTHER 없음: 모르는 값은 INVALID_VALUE


# ── 작은 도구 ──────────────────────────────────────────────────────────────────────
def parse_time(text: str) -> datetime:
    """"2026-10-12T01:00:00.000Z" -> 2026-10-12 01:00:00 UTC (timezone 정보가 있는 datetime).

    힌트: datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    형식이 안 맞으면 ValueError(문자열이 아니면 TypeError)가 나므로 잡아서 ConversionError("INVALID_TYPE", ...)로 바꾼다.
    """
    try: 
        return datetime.strptime(text, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    except (ValueError, TypeError):
        raise ConversionError("INVALID_TYPE", f"occurred_at={text!r}")


# ── 이벤트 유형별 필드: raw -> 그 유형에만 있는 공통 필드 dict ────────────────────────────
def build_item(item: dict) -> dict:
    """order.created의 items[] 한 건 -> 공통 상품 행.

    입력  {order_item_id, sku, product_name, category, quantity, unit_price, discount_amount}
    출력  {order_item_id, product_id, product_name, category, quantity, unit_price, item_discount}
    규칙  sku -> product_id, discount_amount -> item_discount. 필수 필드는 get_required, 정수 필드는 get_required_int.
    """
    return {
        "order_item_id": get_required(item, "order_item_id"),
        "product_id": get_required(item, "sku"),
        "product_name": get_required(item, "product_name"),
        "category": get_required(item, "category"),
        "quantity": get_required_int(item, "quantity"),
        "unit_price": get_required_int(item, "unit_price"),
        "item_discount": get_required_int(item, "discount_amount"),
    }

def build_order(raw: dict) -> dict:
    """order.created -> {shipping_fee, items}. items는 build_item을 각 행에 적용한다 (필수, 정수 검사)."""
    return {
        "shipping_fee": get_required_int(raw, "shipping_fee"),
        "items": [build_item(item) for item in get_required(raw, "items")],
    }


def build_cancel(raw: dict) -> dict:
    """order.cancelled -> {cancel_reason_code}. 예시로 구현해 두었다. 나머지도 같은 모양으로 쓴다."""
    return {"cancel_reason_code": map_code(REASONS, raw.get("reason"))}


def build_payment_succeeded(raw: dict) -> dict:
    """payment.succeeded -> {payment_id, payment_method, payment_amount, fail_reason_code: None}.

    규칙  method -> payment_method(map_code), amount -> payment_amount(get_required_int).
    """
    return {
        "payment_id": get_required(raw, "payment_id"),
        "payment_method": map_code(PAYMENT_METHODS, get_required(raw, "method")),
        "payment_amount": get_required_int(raw, "amount"),
        "fail_reason_code": None,
    }


def build_payment_failed(raw: dict) -> dict:
    """payment.failed -> 성공과 같은 필드 + fail_reason_code(failure_reason을 map_code로).

    힌트: 성공 쪽과 겹치는 부분은 build_payment_succeeded를 호출해 재사용하고 fail_reason_code만 바꾼다.
    """
    fields = build_payment_succeeded(raw)                     # 겹치는 부분 재사용
    fields["fail_reason_code"] = map_code(FAIL_REASONS, raw.get("failure_reason"))                          # failure_reason을 map_code로 (없어도 OTHER)
    return fields


def build_refund(raw: dict) -> dict:
    """refund.succeeded -> {refund_id, payment_id, order_item_id, refund_quantity, refund_amount, refund_type, refund_reason_code}.

    규칙  amount -> refund_amount(get_required_int), type -> refund_type, reason -> refund_reason_code(map_code).
          type이 REFUND_TYPES에 없으면 ConversionError("INVALID_VALUE", ...).
    """
    refund_type = get_required(raw, "type")
    if refund_type not in REFUND_TYPES:                       # 환불 유형만 OTHER 없이 격리
        raise ConversionError("INVALID_VALUE", f"type={refund_type!r}")
    return {
        "refund_id": get_required(raw, "refund_id"),
        "payment_id": get_required(raw, "payment_id"),
        "order_item_id": get_required(raw, "order_item_id"),
        "refund_quantity": get_required_int(raw, "refund_quantity"),
        "refund_amount": get_required_int(raw, "amount"),
        "refund_type": REFUND_TYPES[refund_type],
        "refund_reason_code": map_code(REASONS, raw.get("reason")),
    }


# 공통 event_type -> 유형별 필드 함수
BUILDERS = {
    "ORDER_CREATED": build_order,
    "ORDER_CANCELLED": build_cancel,
    "PAYMENT_SUCCEEDED": build_payment_succeeded,
    "PAYMENT_FAILED": build_payment_failed,
    "REFUND_SUCCEEDED": build_refund,
}


# ── 진입점 ───────────────────────────────────────────────────────────────────────────
def to_event(raw: dict) -> dict:
    """원본 dict 하나를 공통 이벤트 dict로 바꾼다.

    순서
      1. get_required(raw, "event_type")를 EVENT_TYPES로 바꾼다. 표에 없으면 ConversionError("UNKNOWN_EVENT_TYPE", ...).
      2. 공통 필드를 만든다: event_id, event_type, channel(CHANNEL), order_id, user_id(없거나 null이면 None 그대로),
         event_time(parse_time(get_required(raw, "occurred_at"))).
      3. BUILDERS[공통 event_type](raw)의 결과를 공통 필드에 합쳐 돌려준다.
    """
    code = get_required(raw, "event_type")
    if code not in EVENT_TYPES:
        raise ConversionError("UNKNOWN_EVENT_TYPE", code)
    event_type = EVENT_TYPES[code]
    common = {
        "event_id": get_required(raw, "event_id"),
        "event_type": event_type,
        "channel": CHANNEL,
        "order_id": get_required(raw, "order_id"),
        "user_id": raw.get("user_id"),                        # 비로그인(null)은 그대로 통과
        "event_time": parse_time(get_required(raw, "occurred_at")),                                    # parse_time(get_required(raw, "occurred_at"))
    }
    return {**common, **BUILDERS[event_type](raw)}            # 공통 필드 + 유형별 필드 합치기
