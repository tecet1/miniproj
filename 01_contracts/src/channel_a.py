"""A 채널(자사몰) 변환. 중립 이벤트 -> A 원본 dict. snake_case, 평면 + items 배열, 시각은 ISO 8601 UTC.

필드 매핑은 이벤트 명세 2.2절, 코드 값은 2.3절을 따른다.
"""
from timeutil import to_iso_utc  # noqa: F401

CHANNEL = "A"

EVENT_CODES = {"ORDER_CREATED": "order.created", "ORDER_CANCELLED": "order.cancelled",
               "PAYMENT_SUCCEEDED": "payment.succeeded", "PAYMENT_FAILED": "payment.failed",
               "REFUND_SUCCEEDED": "refund.succeeded"}
METHODS = {"CARD": "card", "EASY_PAY": "easy_pay", "BANK_TRANSFER": "bank", "VIRTUAL_ACCOUNT": "vbank"}
REASONS = {"CHANGE_OF_MIND": "change_of_mind", "DEFECT": "defect", "WRONG_ITEM": "wrong_item", "DELAY": "delay"}
FAIL_REASONS = {"LIMIT_EXCEEDED": "limit_exceeded", "CARD_DECLINED": "declined", "TIMEOUT": "timeout"}
REFUND_TYPES = {"CANCEL": "cancel", "RETURN": "return"}

# 오류 주입(faults.py)이 건드릴 위치. 튜플은 중첩 경로이며 숫자는 배열 인덱스다.
FAULT_PATHS = {"payment_amount": ("amount",), "quantity": ("items", 0, "quantity"),
               "event_code": ("event_type",)}
UNKNOWN_EVENT_CODE = "shipping.started"   # 매핑표에 없는 이벤트 코드 (S21 변형 3)


def to_raw(event: dict) -> dict:
    """중립 이벤트 하나를 A 원본 dict로 바꾼다.

    공통: event_id, event_type(코드 변환), occurred_at, order_id, user_id(비로그인이면 None -> null)
    유형별 필드명은 test_channel_a.py 의 기대 dict를 보면 가장 빠르게 알 수 있다.
    주의: 결제 성공에는 failure_reason 키를 넣지 않는다.
    """
    raise NotImplementedError
