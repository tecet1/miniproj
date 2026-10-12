"""C 채널(스포츠 전문몰) 변환. 중립 이벤트 -> C 원본 dict. 약어 snake_case, 시각은 KST 문자열(시간대 표기 없음).

필드 매핑은 이벤트 명세 2.2절, 코드 값은 2.3절을 따른다.
"""
from timeutil import to_kst_string

CHANNEL = "C"

EVENT_CODES = {"ORDER_CREATED": "O01", "ORDER_CANCELLED": "O09", "PAYMENT_SUCCEEDED": "P01",
               "PAYMENT_FAILED": "P02", "REFUND_SUCCEEDED": "R01"}
METHODS = {"CARD": "10", "EASY_PAY": "20", "BANK_TRANSFER": "30", "VIRTUAL_ACCOUNT": "40"}
REASONS = {"CHANGE_OF_MIND": "01", "DEFECT": "02", "WRONG_ITEM": "03", "DELAY": "04"}
FAIL_REASONS = {"LIMIT_EXCEEDED": "F1", "CARD_DECLINED": "F2", "TIMEOUT": "F3"}
REFUND_TYPES = {"CANCEL": "1", "RETURN": "2"}

FAULT_PATHS = {"payment_amount": ("pay_amt",), "quantity": ("lines", 0, "qty"), "event_code": ("evt_cd",)}
UNKNOWN_EVENT_CODE = "D01"


def to_raw(event: dict) -> dict:
    """중립 이벤트 하나를 C 원본 dict로 바꾼다.

    비로그인이면 mem_no를 빈 문자열("")로 둔다. 상품 행은 order_item_id 대신 순번(seq, 정수)을 쓰며,
    order_item_id는 ord_no + "-" + seq 로 복원된다. 환불 이벤트도 seq 로 대상 상품을 가리킨다.
    """
    event_type = event["event_type"]
    raw = {
        "evt_id": event["event_id"],
        "evt_cd": EVENT_CODES[event_type],
        "evt_dt": to_kst_string(event["event_time"]),
        "ord_no": event["order_id"],
        "mem_no": event["user_id"] or "",
    }
    if event_type == "ORDER_CREATED":
        raw["dlv_amt"] = event["shipping_fee"]
        raw["lines"] = [_item_to_raw(item) for item in event["items"]]
    elif event_type == "ORDER_CANCELLED":
        raw["rsn_cd"] = REASONS[event["cancel_reason_code"]]
    elif event_type in ("PAYMENT_SUCCEEDED", "PAYMENT_FAILED"):
        raw.update(pay_no=event["payment_id"], pay_type=METHODS[event["payment_method"]],
                   pay_amt=event["payment_amount"])
        if event_type == "PAYMENT_FAILED":
            raw["fail_cd"] = FAIL_REASONS[event["fail_reason_code"]]
    else:  # REFUND_SUCCEEDED
        raw.update(rfd_no=event["refund_id"], pay_no=event["payment_id"],
                   seq=_seq(event["order_item_id"]), rfd_qty=event["refund_quantity"],
                   rfd_amt=event["refund_amount"],
                   clm_tp=REFUND_TYPES[event["refund_type"]], rsn_cd=REASONS[event["refund_reason_code"]])
    return raw


def _seq(order_item_id: str) -> int:
    """'C-S09-1-1' -> 1 (마지막 '-' 뒤의 순번)."""
    return int(order_item_id.rsplit("-", 1)[1])


def _item_to_raw(item: dict) -> dict:
    return {"seq": _seq(item["order_item_id"]), "goods_cd": item["product_id"],
            "goods_nm": item["product_name"], "cate_nm": item["category"],
            "qty": item["quantity"], "price": item["unit_price"], "dc_amt": item["item_discount"]}
