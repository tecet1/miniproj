"""scenarios.json이 skus.json과 이벤트 정의대로 쓰였는지 검사한다. 읽기(scenario_loader)와 검사는 분리한다."""

EVENT_TYPES = {"ORDER_CREATED", "ORDER_CANCELLED", "PAYMENT_SUCCEEDED", "PAYMENT_FAILED", "REFUND_SUCCEEDED"}


def validate_scenarios(scenarios: list[dict], skus: dict[str, dict]) -> None:
    """잘못된 입력을 생성 전에 막는다. 문제가 있으면 ValueError.

    검사 항목
      - 주문 상품의 sku(items[].sku)가 skus에 있는가
      - 이벤트 type(events[].type)이 EVENT_TYPES에 있는가
      - 오류 사례 키: items[].reuse_item_id_of는 앞선 행 번호, 환불의 "payment"는 "failed"이고 앞선 결제 실패가 있어야 하며,
        환불의 "order_no"는 같은 시나리오의 주문 번호
    오류 메시지에는 시나리오 ID와 주문 번호(order_no)를 넣어 어디가 문제인지 알 수 있게 한다.
    """
    for scenario in scenarios:
        order_numbers = {order["order_no"] for order in scenario["orders"]}
        for order in scenario["orders"]:
            order_no = order["order_no"]
            where = f"시나리오 {scenario['id']} 주문 {order_no}"
            for index, item in enumerate(order["items"], start=1):
                sku = item["sku"]
                if sku not in skus:
                    raise ValueError(f"{where} 상품 {sku}는 skus.json에 없음")
                reused = item.get("reuse_item_id_of")
                if reused is not None and not (isinstance(reused, int) and 1 <= reused < index):
                    raise ValueError(f"{where} 상품 행 {index}: reuse_item_id_of {reused}는 앞선 행 번호여야 함")
            failed_seen = False
            for event in order["events"]:
                event_type = event["type"]
                if event_type not in EVENT_TYPES:
                    raise ValueError(f"{where} 이벤트 {event_type}는 정의되지 않음")
                failed_seen = failed_seen or event_type == "PAYMENT_FAILED"
                if event_type == "REFUND_SUCCEEDED":
                    if "payment" in event and (event["payment"] != "failed" or not failed_seen):
                        raise ValueError(f"{where} 환불: payment는 앞선 결제 실패가 있을 때 \"failed\"만 쓸 수 있음")
                    if "order_no" in event and event["order_no"] not in order_numbers:
                        raise ValueError(f"{where} 환불: order_no {event['order_no']}는 이 시나리오에 없음")
