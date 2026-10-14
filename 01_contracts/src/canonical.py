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
from datetime import datetime, timedelta, timezone

import pricing
from ids import ReadableIds


def build_events(scenario: dict, skus: dict[str, dict], channel: str, ids=None) -> list[dict]:
    """scenario의 모든 주문을 발생 순서대로 중립 이벤트로 만든다.

    ids는 ID를 만드는 객체다(ids.py). 생략하면 고정 픽스처의 읽기 쉬운 규칙(ReadableIds)이다.
    """
    ids = ids or ReadableIds()
    scenario_start = datetime.fromisoformat(scenario["start_kst"])
    events = []
    for order in scenario["orders"]:
        order_id = ids.order_id(channel, scenario["id"], order["order_no"])
        order_start = scenario_start + timedelta(seconds=order.get("start_offset_sec", 0))
        events += _build_order_events(order, order_id, order_start, skus, ids)
    return events


def _build_items(order: dict, order_id: str, skus: dict[str, dict], ids) -> tuple[list[dict], list[int]]:
    """주문 상품 행 목록과, 행별 할인 후 금액(item_paid_amount) 목록을 함께 돌려준다."""
    items, paid_amounts = [], []
    for seq, line in enumerate(order["items"], start=1):
        sku = skus[line["sku"]]
        _, paid = pricing.item_amounts(sku["unit_price"], line["quantity"], line["discount"])
        items.append({
            "order_item_id": ids.order_item_id(order_id, seq),
            "product_id": sku["product_id"],
            "product_name": sku["product_name"],
            "category": sku["category"],
            "quantity": line["quantity"],
            "unit_price": sku["unit_price"],
            "item_discount": line["discount"],
        })
        paid_amounts.append(paid)
    return items, paid_amounts


def _build_order_events(order: dict, order_id: str, order_start: datetime,
                        skus: dict[str, dict], ids) -> list[dict]:
    """주문 하나의 이벤트를 만든다. 번호(E, P, R)와 성공 결제 ID를 세면서 순서대로 처리한다."""
    items, paid_amounts = _build_items(order, order_id, skus, ids)
    shipping = pricing.shipping_fee(sum(paid_amounts))
    payment_amount = sum(paid_amounts) + shipping

    events = []
    payment_no = refund_no = 0
    succeeded_payment_id = None
    for event_no, spec in enumerate(order["events"], start=1):
        event_time = order_start + timedelta(seconds=spec["offset_sec"])
        event = {
            "event_id": ids.event_id(order_id, event_no),
            "event_type": spec["type"],
            "order_id": order_id,
            "user_id": order["user_id"],
            "event_time": event_time.astimezone(timezone.utc),
            "fault": spec.get("fault"),
            "send_count": spec.get("send_count", 1),
        }
        if spec["type"] == "ORDER_CREATED":
            event.update(shipping_fee=shipping, items=items)
        elif spec["type"] == "ORDER_CANCELLED":
            event.update(cancel_reason_code=spec["reason"])
        elif spec["type"] in ("PAYMENT_SUCCEEDED", "PAYMENT_FAILED"):
            payment_no += 1
            event.update(
                payment_id=ids.payment_id(order_id, payment_no),
                payment_method=spec["method"],
                payment_amount=payment_amount,
                fail_reason_code=spec.get("fail_reason"),
            )
            if spec["type"] == "PAYMENT_SUCCEEDED":
                succeeded_payment_id = event["payment_id"]
        else:  # REFUND_SUCCEEDED
            refund_no += 1
            line_index = spec["item"] - 1
            event.update(
                refund_id=ids.refund_id(order_id, refund_no),
                payment_id=succeeded_payment_id,
                order_item_id=items[line_index]["order_item_id"],
                refund_quantity=spec["quantity"],
                refund_amount=pricing.refund_amount(
                    paid_amounts[line_index], items[line_index]["quantity"], spec["quantity"]),
                refund_type=spec["refund_type"],
                refund_reason_code=spec["reason"],
            )
        events.append(event)
    return events
