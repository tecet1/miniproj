"""Pure channel-A v1/v2 -> common event mapping. Delta storage is a separate step."""

from datetime import datetime, timezone


EVENT_TYPES = {
    "order_created": "ORDER_CREATED",
    "order_cancelled": "ORDER_CANCELLED",
    "payment_failed": "PAYMENT_FAILED",
    "payment_succeeded": "PAYMENT_SUCCEEDED",
    "refund_completed": "REFUND_COMPLETED",
}
ORDER_TOTAL_FIELDS = {"1.0": "total", "2.0": "order_total"}


def text(record: dict, field: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"MISSING_OR_INVALID_FIELD: {field}")
    return value


def integer(record: dict, field: str) -> int:
    value = record.get(field)
    if type(value) is not int:
        raise ValueError(f"INVALID_INTEGER: {field}")
    return value


def utc_time(value: str) -> str:
    time = datetime.fromisoformat(value)
    if time.tzinfo is None:
        raise ValueError("TIMEZONE_REQUIRED")
    return time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_items(payload: dict, mapping: dict, event_type: str) -> list[dict]:
    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError("MISSING_OR_INVALID_FIELD: items")
    result = []
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("INVALID_ITEM")
        source_sku = text(item, "sku")
        if source_sku not in mapping:
            raise ValueError(f"UNMAPPED_SKU: {source_sku}")
        sku_id = mapping[source_sku]
        if not isinstance(sku_id, str) or not sku_id.strip():
            raise ValueError(f"INVALID_SKU_MAPPING: {source_sku}")
        row = {"order_item_id": text(item, "item_no"), "sku_id": sku_id}
        quantity, price = integer(item, "qty"), integer(item, "price")
        if event_type == "ORDER_CREATED":
            row.update(quantity=quantity, unit_price=price, line_amount=quantity * price)
        elif event_type == "ORDER_CANCELLED":
            row.update(cancelled_quantity=quantity, cancelled_amount=quantity * price)
        else:
            row.update(refunded_quantity=quantity, refunded_amount=quantity * price)
        result.append(row)
    return result


def normalize_event(event: dict, sku_mapping: dict) -> dict:
    """Return JSON-friendly common business fields without mutating the source."""
    if not isinstance(event, dict):
        raise ValueError("INVALID_EVENT")
    if event.get("channel_id") != "A":
        raise ValueError("UNSUPPORTED_CHANNEL")
    version = text(event, "schema_version")
    if version not in ORDER_TOTAL_FIELDS:
        raise ValueError(f"UNSUPPORTED_SCHEMA_VERSION: {version}")
    event_name = text(event, "event_name")
    if event_name not in EVENT_TYPES:
        raise ValueError(f"UNSUPPORTED_EVENT_TYPE: {event_name}")
    event_type = EVENT_TYPES[event_name]
    payload = event.get("payload")
    if not isinstance(payload, dict):
        raise ValueError("INVALID_PAYLOAD")
    if event.get("customer_no") is not None:
        text(event, "customer_no")
    result = {
        "schema_version": "1.0", "event_id": text(event, "event_id"),
        "channel_id": "A", "event_type": event_type,
        "event_time": utc_time(text(event, "occurred_at")),
        "order_id": text(event, "order_no"), "uid": event.get("customer_no"),
    }
    if event_type == "ORDER_CREATED":
        data = {"order_amount": integer(payload, ORDER_TOTAL_FIELDS[version]),
                "items": normalize_items(payload, sku_mapping, event_type)}
    elif event_type == "ORDER_CANCELLED":
        if text(payload, "reason") != "customer":
            raise ValueError("UNSUPPORTED_CANCEL_REASON")
        data = {"cancellation_id": text(payload, "cancel_no"), "cancel_reason": "CUSTOMER_REQUEST",
                "items": normalize_items(payload, sku_mapping, event_type)}
    elif event_type in {"PAYMENT_FAILED", "PAYMENT_SUCCEEDED"}:
        if text(payload, "method") != "card":
            raise ValueError("UNSUPPORTED_PAYMENT_METHOD")
        amount_field = "requested_amount" if event_type == "PAYMENT_FAILED" else "paid_amount"
        data = {"payment_id": text(payload, "payment_no"), "payment_method": "CARD",
                amount_field: integer(payload, "amount")}
        if event_type == "PAYMENT_FAILED":
            if text(payload, "error") != "card_declined":
                raise ValueError("UNSUPPORTED_FAILURE_CODE")
            data["failure_code"] = "CARD_DECLINED"
    else:
        data = {"refund_id": text(payload, "refund_no"), "payment_id": text(payload, "payment_no"),
                "refund_amount": integer(payload, "amount"),
                "items": normalize_items(payload, sku_mapping, event_type)}
    result["data"] = data
    return result
