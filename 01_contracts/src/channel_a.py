"""A 채널(자사몰) 변환. 중립 이벤트 -> A 원본 dict. snake_case, 평면 + items 배열, 시각은 ISO 8601 UTC.

필드 매핑은 이벤트 명세 2.2절, 코드 값은 2.3절을 따른다.
"""
from timeutil import to_iso_utc

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
    """중립 이벤트 하나를 A 원본 dict로 바꾼다."""
    event_type = event["event_type"]
    raw = {
        "event_id": event["event_id"],
        "event_type": EVENT_CODES[event_type],
        "occurred_at": to_iso_utc(event["event_time"]),
        "order_id": event["order_id"],
        "user_id": event["user_id"],            # 비로그인이면 None -> JSON null
    }
    if event_type == "ORDER_CREATED":
        raw["shipping_fee"] = event["shipping_fee"]
        raw["items"] = [_item_to_raw(item) for item in event["items"]]
    elif event_type == "ORDER_CANCELLED":
        raw["reason"] = REASONS[event["cancel_reason_code"]]
    elif event_type in ("PAYMENT_SUCCEEDED", "PAYMENT_FAILED"):
        raw.update(payment_id=event["payment_id"], method=METHODS[event["payment_method"]],
                   amount=event["payment_amount"])
        if event_type == "PAYMENT_FAILED":
            raw["failure_reason"] = FAIL_REASONS[event["fail_reason_code"]]
    else:  # REFUND_SUCCEEDED
        raw.update(refund_id=event["refund_id"], payment_id=event["payment_id"],
                   order_item_id=event["order_item_id"], refund_quantity=event["refund_quantity"],
                   amount=event["refund_amount"],
                   type=REFUND_TYPES[event["refund_type"]], reason=REASONS[event["refund_reason_code"]])
    return raw


def _item_to_raw(item: dict) -> dict:
    return {"order_item_id": item["order_item_id"], "sku": item["product_id"],
            "product_name": item["product_name"], "category": item["category"],
            "quantity": item["quantity"], "unit_price": item["unit_price"],
            "discount_amount": item["item_discount"]}
