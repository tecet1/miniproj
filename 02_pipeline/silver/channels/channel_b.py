"""B 채널(종합 오픈마켓) 원본 dict -> 공통 이벤트 dict. 생성기의 channel_b.to_raw의 반대 방향이다.

[입력] json.loads로 읽은 원본 dict 하나 (Bronze payload 한 건 = 이벤트 한 건). camelCase, 중첩 객체.
  공통: eventId, eventName, occurredAtMs(epoch 밀리초 정수), orderNo, buyer{memberId}(비로그인이면 buyer 또는 memberId 없음)
  ORDER_PLACED    delivery{fee}, orderItems[{lineId, sellerSku, itemName, categoryName, qty, unitPrice, discount{total}}]
  ORDER_CANCELED  cancelReason
  PAY_APPROVED    paymentId, payMethod, paidAmount
  PAY_DECLINED    paymentId, payMethod, paidAmount, declineCode
  REFUND_DONE     refundId, paymentId, lineId, refundQty, refundAmount, claimType, claimReason
  (이벤트 명세 2.1~2.3절의 매핑표를 따른다)

[출력] 공통 이벤트 dict (채널 A와 같은 모양, channel은 "B")

[오류] 채널 A와 같다. 새로 생긴 점만 적는다.
  - 중첩 객체(delivery, discount)는 fields.get_required_dict로 꺼낸다. 없으면 MISSING_REQUIRED, dict가 아니면 INVALID_TYPE.
  - occurredAtMs가 정수가 아니면 INVALID_TYPE (fields.get_required_int).
  - buyer는 선택 필드다. 없거나 memberId가 없으면 user_id는 None (오류 아님).
"""
from datetime import UTC, datetime, timedelta

from conversion_error import ConversionError
from fields import (get_optional_str, get_required, get_required_dict, get_required_dict_list, get_required_int,
                    get_required_str, lookup, map_code)

CHANNEL = "B"

# ── 매핑 상수: 원본 코드 -> 공통 값 (이벤트 명세 2.1, 2.3절) ─────────────────────────────
EVENT_TYPES = {
    "ORDER_PLACED": "ORDER_CREATED",
    "ORDER_CANCELED": "ORDER_CANCELLED",
    "PAY_APPROVED": "PAYMENT_SUCCEEDED",
    "PAY_DECLINED": "PAYMENT_FAILED",
    "REFUND_DONE": "REFUND_SUCCEEDED",
}
PAYMENT_METHODS = {"CARD": "CARD", "SIMPLE": "EASY_PAY", "TRANSFER": "BANK_TRANSFER", "VBANK": "VIRTUAL_ACCOUNT"}
REASONS = {"BUYER_REMORSE": "CHANGE_OF_MIND", "DEFECTIVE": "DEFECT", "WRONG_DELIVERY": "WRONG_ITEM", "DELAYED": "DELAY"}
FAIL_REASONS = {"LIMIT": "LIMIT_EXCEEDED", "DECLINED": "CARD_DECLINED", "TIMEOUT": "TIMEOUT"}
REFUND_TYPES = {"CANCEL": "CANCEL", "RETURN": "RETURN"}   # OTHER 없음: 모르는 값은 INVALID_VALUE


# ── 작은 도구 (A와 다른 부분) ─────────────────────────────────────────────────────────
def parse_time(epoch_ms: int) -> datetime:
    """1791766800000 -> 2026-10-12 01:00:00 UTC (timezone 정보가 있는 datetime).

    epoch_ms / 1000 으로 fromtimestamp에 넣으면 소수 오차가 생길 수 있어 timedelta로 더한다.
    너무 큰 값은 OverflowError가 나므로 INVALID_TYPE으로 바꾼다. 정수 여부는 호출하는 쪽(get_required_int)이 검사한다.
    """
    try:
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=epoch_ms)
    except OverflowError:
        raise ConversionError("INVALID_TYPE", f"occurredAtMs={epoch_ms!r}")


def read_user_id(raw: dict):
    """buyer.memberId를 돌려준다. buyer가 없거나 dict가 아니거나 memberId가 없으면 None. 오류가 아니다."""
    buyer = raw.get("buyer")
    return get_optional_str(buyer, "memberId") if isinstance(buyer, dict) else None


# ── 이벤트 유형별 필드: raw -> 그 유형에만 있는 공통 필드 dict ────────────────────────────
def build_item(item: dict) -> dict:
    """orderItems[] 한 건 -> 공통 상품 행. discount.total은 중첩 객체 안의 값이다."""
    return {
        "order_item_id": get_required_str(item, "lineId"),
        "product_id": get_required_str(item, "sellerSku"),
        "product_name": get_optional_str(item, "itemName"),
        "category": get_optional_str(item, "categoryName"),
        "quantity": get_required_int(item, "qty"),
        "unit_price": get_required_int(item, "unitPrice"),
        "item_discount": get_required_int(get_required_dict(item, "discount"), "total"),
    }


def build_order(raw: dict) -> dict:
    """ORDER_PLACED -> {shipping_fee, items}. shipping_fee는 delivery 객체 안의 fee."""
    return {
        "shipping_fee": get_required_int(get_required_dict(raw, "delivery"), "fee"),
        "items": [build_item(item) for item in get_required_dict_list(raw, "orderItems")],
    }


def build_cancel(raw: dict) -> dict:
    """ORDER_CANCELED -> {cancel_reason_code}. cancelReason은 선택 필드라 raw.get + map_code."""
    return {"cancel_reason_code": map_code(REASONS, raw.get("cancelReason"))}


def build_payment_succeeded(raw: dict) -> dict:
    """PAY_APPROVED -> {payment_id, payment_method, payment_amount, fail_reason_code: None}."""
    return {
        "payment_id": get_required_str(raw, "paymentId"),
        "payment_method": map_code(PAYMENT_METHODS, get_required(raw, "payMethod")),
        "payment_amount": get_required_int(raw, "paidAmount"),
        "fail_reason_code": None,
    }


def build_payment_failed(raw: dict) -> dict:
    """PAY_DECLINED -> 성공과 같은 필드 + fail_reason_code(declineCode를 map_code로)."""
    fields = build_payment_succeeded(raw)
    fields["fail_reason_code"] = map_code(FAIL_REASONS, raw.get("declineCode"))
    return fields


def build_refund(raw: dict) -> dict:
    """REFUND_DONE -> 환불 필드. claimType이 REFUND_TYPES에 없으면 INVALID_VALUE."""
    raw_type = get_required(raw, "claimType")
    refund_type = lookup(REFUND_TYPES, raw_type)
    if refund_type is None:
        raise ConversionError("INVALID_VALUE", f"claimType={raw_type!r}")
    return {
        "refund_id": get_required_str(raw, "refundId"),
        "payment_id": get_required_str(raw, "paymentId"),
        "order_item_id": get_required_str(raw, "lineId"),
        "refund_quantity": get_required_int(raw, "refundQty"),
        "refund_amount": get_required_int(raw, "refundAmount"),
        "refund_type": refund_type,
        "refund_reason_code": map_code(REASONS, raw.get("claimReason")),
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
    """원본 dict 하나를 공통 이벤트 dict로 바꾼다. 순서는 채널 A와 같다.

      1. eventName을 EVENT_TYPES로 바꾼다. 표에 없으면 ConversionError("UNKNOWN_EVENT_TYPE", ...).
      2. 공통 필드를 만든다.
      3. BUILDERS[event_type](raw)의 결과를 합쳐 돌려준다.
    """
    code = get_required(raw, "eventName")
    event_type = lookup(EVENT_TYPES, code)
    if event_type is None:
        raise ConversionError("UNKNOWN_EVENT_TYPE", repr(code))
    common = {
        "event_id": get_required_str(raw, "eventId"),
        "event_type": event_type,
        "channel": CHANNEL,
        "order_id": get_required_str(raw, "orderNo"),
        "user_id": read_user_id(raw),
        "event_time": parse_time(get_required_int(raw, "occurredAtMs")),
    }
    return {**common, **BUILDERS[event_type](raw)}
