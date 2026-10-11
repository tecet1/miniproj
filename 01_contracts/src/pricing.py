"""금액 계산 규칙. scenarios.md의 '금액' 규칙을 그대로 코드로 옮긴다."""

FREE_SHIPPING_THRESHOLD = 20000   # 상품 결제금액 합이 이 값 이상이면 배송비 0
SHIPPING_FEE = 4000


def item_amounts(unit_price: int, quantity: int, discount: int) -> tuple[int, int]:
    """(item_gross_amount, item_paid_amount)를 돌려준다.

    gross = 단가 x 수량 (할인 전), paid = gross - 할인
    """
    return unit_price * quantity, unit_price * quantity - discount

def shipping_fee(paid_total: int) -> int:
    """한 주문의 item_paid_amount 합계로 배송비를 정한다. 20,000 이상 0, 미만 4,000."""
    return 0 if paid_total >= FREE_SHIPPING_THRESHOLD else SHIPPING_FEE


def refund_amount(paid: int, quantity: int, refund_quantity: int) -> int:
    """부분 환불 금액. paid x 환불수량 / 수량 (정수 나눗셈).

    예: paid 70,000, 수량 2, 환불수량 1 -> 35,000
    """
    return paid * refund_quantity // quantity
