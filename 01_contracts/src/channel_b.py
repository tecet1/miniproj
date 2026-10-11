"""B 채널(종합 오픈마켓) 변환. 중립 이벤트 -> B 원본 dict. camelCase, 중첩 객체, 시각은 epoch 밀리초.

필드 매핑은 이벤트 명세 2.2절, 코드 값은 2.3절을 따른다.
"""
from timeutil import to_epoch_ms  # noqa: F401

CHANNEL = "B"

EVENT_CODES = {"ORDER_CREATED": "ORDER_PLACED", "ORDER_CANCELLED": "ORDER_CANCELED",
               "PAYMENT_SUCCEEDED": "PAY_APPROVED", "PAYMENT_FAILED": "PAY_DECLINED",
               "REFUND_SUCCEEDED": "REFUND_DONE"}
METHODS = {"CARD": "CARD", "EASY_PAY": "SIMPLE", "BANK_TRANSFER": "TRANSFER", "VIRTUAL_ACCOUNT": "VBANK"}
REASONS = {"CHANGE_OF_MIND": "BUYER_REMORSE", "DEFECT": "DEFECTIVE", "WRONG_ITEM": "WRONG_DELIVERY",
           "DELAY": "DELAYED"}
FAIL_REASONS = {"LIMIT_EXCEEDED": "LIMIT", "CARD_DECLINED": "DECLINED", "TIMEOUT": "TIMEOUT"}
REFUND_TYPES = {"CANCEL": "CANCEL", "RETURN": "RETURN"}

FAULT_PATHS = {"payment_amount": ("paidAmount",), "quantity": ("orderItems", 0, "qty"),
               "event_code": ("eventName",)}
UNKNOWN_EVENT_CODE = "SHIP_STARTED"


def to_raw(event: dict) -> dict:
    """중립 이벤트 하나를 B 원본 dict로 바꾼다.

    A와 다른 점: 회원은 {"buyer": {"memberId": ...}} 로 중첩하고 비로그인이면 memberId 키 자체를 뺀다
    ("buyer": {}). 배송비는 {"delivery": {"fee": ...}}, 상품 할인은 {"discount": {"total": ...}}.
    상품 행 ID(lineId)에는 order_item_id 전체를 넣는다.
    """
    raise NotImplementedError
