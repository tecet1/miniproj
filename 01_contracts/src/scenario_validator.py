"""scenarios.json이 skus.json과 이벤트 정의대로 쓰였는지 검사한다. 읽기(scenario_loader)와 검사는 분리한다."""

EVENT_TYPES = {"ORDER_CREATED", "ORDER_CANCELLED", "PAYMENT_SUCCEEDED", "PAYMENT_FAILED", "REFUND_SUCCEEDED"}


def validate_scenarios(scenarios: list[dict], skus: dict[str, dict]) -> None:
    """잘못된 입력을 생성 전에 막는다. 문제가 있으면 ValueError.

    검사 항목 (이 두 가지만 검사한다)
      - 주문 상품의 sku(items[].sku)가 skus에 있는가
      - 이벤트 type(events[].type)이 EVENT_TYPES에 있는가
    오류 메시지에는 시나리오 ID와 주문 번호(order_no)를 넣어 어디가 문제인지 알 수 있게 한다.
    """
    raise NotImplementedError
