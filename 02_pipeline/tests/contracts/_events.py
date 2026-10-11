"""채널 변환 테스트용 중립 이벤트 샘플 (canonical.build_events의 출력 형식을 손으로 작성한 것)."""
from datetime import datetime, timezone

UTC = timezone.utc


def _base(channel, event_no, event_type, seconds, user_id="U01", order="S01-1"):
    order_id = f"{channel}-{order}"
    return {
        "event_id": f"{order_id}-E{event_no}", "event_type": event_type, "order_id": order_id,
        "user_id": user_id, "event_time": datetime(2026, 10, 12, 1, 0, seconds, tzinfo=UTC),
        "fault": None, "send_count": 1,
    }


def created(channel, user_id="U01"):
    event = _base(channel, 1, "ORDER_CREATED", 0, user_id)
    event["shipping_fee"] = 0
    event["items"] = [{
        "order_item_id": f"{channel}-S01-1-1", "product_id": "SKU-004", "product_name": "러닝화",
        "category": "신발", "quantity": 1, "unit_price": 89000, "item_discount": 0}]
    return event


def payment_ok(channel):
    event = _base(channel, 2, "PAYMENT_SUCCEEDED", 30)
    event.update(payment_id=f"{channel}-S01-1-P1", payment_method="CARD",
                 payment_amount=89000, fail_reason_code=None)
    return event


def payment_failed(channel):
    event = _base(channel, 2, "PAYMENT_FAILED", 20, order="S06-1")
    event.update(payment_id=f"{channel}-S06-1-P1", payment_method="CARD",
                 payment_amount=59000, fail_reason_code="LIMIT_EXCEEDED")
    return event


def cancelled(channel):
    event = _base(channel, 2, "ORDER_CANCELLED", 30, order="S08-1")
    event["cancel_reason_code"] = "CHANGE_OF_MIND"
    return event


def refund(channel):
    event = _base(channel, 3, "REFUND_SUCCEEDED", 40, order="S09-1")
    event.update(refund_id=f"{channel}-S09-1-R1", payment_id=f"{channel}-S09-1-P1",
                 order_item_id=f"{channel}-S09-1-1", refund_quantity=1, refund_amount=35000,
                 refund_type="RETURN", refund_reason_code="DEFECT")
    return event
