"""공통 이벤트 한 건의 값 범위를 검사한다. 어긋나면 ConversionError("INVALID_VALUE", 상세)를 낸다.

[입력] 채널 변환이 끝난 공통 이벤트 dict (channels/channel_*.to_event의 출력)
[출력] 없음. 통과하면 조용히 끝나고, 실패하면 예외를 낸다.

[규칙] 이벤트 명세 4절 INVALID_VALUE: 음수 금액, 수량 0 이하, 할인 > 단가 x 수량
  ORDER_CREATED     shipping_fee >= 0
                    items 각 행: quantity > 0, unit_price >= 0, item_discount >= 0, item_discount <= unit_price x quantity
  PAYMENT_*         payment_amount >= 0
  REFUND_SUCCEEDED  refund_quantity > 0, refund_amount >= 0
  ORDER_CANCELLED   검사할 값 없음
금액 0은 허용한다 (할인이 단가 x 수량과 같은 경우도 허용).
타입 검사는 이미 채널 변환에서 끝났으므로 여기서는 하지 않는다.
단일 이벤트 안에서 판단되는 규칙만 둔다. 다른 이벤트와 맞춰 보는 검증(결제금액 = 상품 합 + 배송비 등)은 Gold의 몫이다.
"""
from conversion_error import ConversionError


def invalid(detail: str):
    """INVALID_VALUE 오류를 낸다. 검사 함수들이 같이 쓴다."""
    raise ConversionError("INVALID_VALUE", detail)


def check_item(item: dict) -> None:
    """상품 행 하나를 검사한다. 위반하면 invalid(...)로 어느 필드가 문제인지 적는다."""
    if item["quantity"] <= 0:
        invalid(f"quantity={item['quantity']}")
    if item["unit_price"] < 0:
        invalid(f"unit_price={item['unit_price']}")
    if item["item_discount"] < 0:
        invalid(f"item_discount={item['item_discount']}")
    if item["item_discount"] > item["unit_price"] * item["quantity"]:
        invalid(f"item_discount={item['item_discount']} > unit_price * quantity")


def check_order(event: dict) -> None:
    """shipping_fee >= 0 이고, 모든 상품 행이 check_item을 통과해야 한다."""
    if event["shipping_fee"] < 0:
        invalid(f"shipping_fee={event['shipping_fee']}")
    for item in event["items"]:
        check_item(item)


def check_payment(event: dict) -> None:
    """payment_amount >= 0."""
    if event["payment_amount"] < 0:
        invalid(f"payment_amount={event['payment_amount']}")


def check_refund(event: dict) -> None:
    """refund_quantity > 0, refund_amount >= 0."""
    if event["refund_quantity"] <= 0:
        invalid(f"refund_quantity={event['refund_quantity']}")
    if event["refund_amount"] < 0:
        invalid(f"refund_amount={event['refund_amount']}")


# 공통 event_type -> 검사 함수 (ORDER_CANCELLED는 검사할 것이 없어 표에 없다)
CHECKS = {
    "ORDER_CREATED": check_order,
    "PAYMENT_SUCCEEDED": check_payment,
    "PAYMENT_FAILED": check_payment,
    "REFUND_SUCCEEDED": check_refund,
}


def validate_event_values(event: dict) -> None:
    """event_type에 맞는 검사 함수를 찾아 실행한다. 표에 없는 유형은 검사 없이 통과한다.

    힌트: CHECKS.get(event["event_type"])
    """
    check_func = CHECKS.get(event["event_type"])
    if check_func:
        check_func(event)
