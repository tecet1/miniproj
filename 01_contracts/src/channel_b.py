"""B 채널(종합 오픈마켓) 변환. 중립 이벤트 -> B 원본 dict. camelCase, 중첩 객체, 시각은 epoch 밀리초.

필드 매핑은 이벤트 명세 2.2절, 코드 값은 2.3절을 따른다.
"""
from timeutil import to_epoch_ms

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

    회원은 {"buyer": {"memberId": ...}} 로 중첩하고, 비로그인이면 memberId 키를 뺀다("buyer": {}).
    상품 행 ID(lineId)에는 order_item_id 전체를 넣는다.
    """
    event_type = event["event_type"]
    raw = {
        "eventId": event["event_id"],
        "eventName": EVENT_CODES[event_type],
        "occurredAtMs": to_epoch_ms(event["event_time"]),
        "orderNo": event["order_id"],
        "buyer": {} if event["user_id"] is None else {"memberId": event["user_id"]},
    }
    if event_type == "ORDER_CREATED":
        raw["delivery"] = {"fee": event["shipping_fee"]}
        raw["orderItems"] = [_item_to_raw(item) for item in event["items"]]
    elif event_type == "ORDER_CANCELLED":
        raw["cancelReason"] = REASONS[event["cancel_reason_code"]]
    elif event_type in ("PAYMENT_SUCCEEDED", "PAYMENT_FAILED"):
        raw.update(paymentId=event["payment_id"], payMethod=METHODS[event["payment_method"]],
                   paidAmount=event["payment_amount"])
        if event_type == "PAYMENT_FAILED":
            raw["declineCode"] = FAIL_REASONS[event["fail_reason_code"]]
    else:  # REFUND_SUCCEEDED
        raw.update(refundId=event["refund_id"], paymentId=event["payment_id"],
                   lineId=event["order_item_id"], refundAmount=event["refund_amount"],
                   claimType=REFUND_TYPES[event["refund_type"]],
                   claimReason=REASONS[event["refund_reason_code"]])
    return raw


def _item_to_raw(item: dict) -> dict:
    return {"lineId": item["order_item_id"], "sellerSku": item["product_id"],
            "itemName": item["product_name"], "categoryName": item["category"],
            "qty": item["quantity"], "unitPrice": item["unit_price"],
            "discount": {"total": item["item_discount"]}}
