"""시나리오 하나를 '채널 중립 이벤트' 목록으로 펼친다. 이 프로젝트의 핵심 로직이다.

이 중립 이벤트는 생성기 내부의 중간 표현이며 파이프라인 Silver 모델과는 별개다.
채널 변환기(channel_a/b/c)는 이 형식만 입력으로 받는다.

중립 이벤트 = dict. 모든 이벤트의 공통 키:
    event_id, event_type, order_id, user_id, event_time(UTC aware datetime),
    fault(None 또는 오류 이름), send_count(기본 1)
유형별 추가 키:
    ORDER_CREATED     shipping_fee, items[{order_item_id, product_id, product_name, category,
                                           quantity, unit_price, item_discount}]
    ORDER_CANCELLED   cancel_reason_code
    PAYMENT_*         payment_id, payment_method, payment_amount, fail_reason_code(성공이면 None)
    REFUND_SUCCEEDED  refund_id, payment_id, order_item_id, refund_quantity, refund_amount,
                      refund_type, refund_reason_code
"""
import pricing  # noqa: F401  (구현 시 사용)


def build_events(scenario: dict, skus: dict[str, dict], channel: str) -> list[dict]:
    """scenario의 모든 주문을 발생 순서대로 중립 이벤트로 만든다.

    구현 순서 힌트
    1. 시작 시각: datetime.fromisoformat(scenario["start_kst"]) 에 주문의 start_offset_sec(기본 0)을 더한다.
    2. 주문 ID: f"{channel}-{scenario['id']}-{order['order_no']}"
    3. 상품 행 만들기: order_item_id = f"{order_id}-{순번}", 단가·상품명·카테고리는 skus에서 가져온다.
    4. 배송비: pricing.shipping_fee(상품 paid 합계). 결제금액 = paid 합계 + 배송비.
    5. 이벤트마다: event_id = f"{order_id}-E{n}", 시각 = 주문 시작 + offset_sec (UTC로 변환).
    6. 결제 이벤트는 결제 시도 번호를 센다(P1, P2 ...). 환불 이벤트는 같은 주문의 성공 결제를 가리킨다.
    7. 환불 금액은 pricing.refund_amount로 계산한다. R1, R2 ... 번호를 센다.
    8. fault, send_count는 시나리오 이벤트에 있으면 그대로 옮긴다.
    """
    raise NotImplementedError
