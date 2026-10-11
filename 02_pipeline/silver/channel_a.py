"""Pure channel-A v1/v2 -> common event mapping. Delta storage is a separate step."""

import hashlib
import json
from datetime import datetime, timezone


EVENT_TYPES = {
    "order_created": "ORDER_CREATED",
    "order_cancelled": "ORDER_CANCELLED",
    "payment_failed": "PAYMENT_FAILED",
    "payment_succeeded": "PAYMENT_SUCCEEDED",
    "refund_completed": "REFUND_COMPLETED",
}
ORDER_TOTAL_FIELDS = {"1.0": "total", "2.0": "order_total"}


class DataQualityError(ValueError):
    """Expected input failure; programming and storage errors must propagate."""

    def __init__(self, rule_id: str, code: str, detail: str = ""):
        self.rule_id = rule_id
        self.code = code
        super().__init__(f"{code}: {detail}" if detail else code)


def canonical_json(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def reject_json_constant(value: str):
    raise DataQualityError("DQ-03", "INVALID_JSON", value)


def inspect_raw_event(raw_payload: str, sku_mapping: dict) -> dict:
    """Return a common JSON or one structured error; no I/O or source mutation."""
    event = None
    fingerprint_text = raw_payload
    try:
        try:
            event = json.loads(raw_payload, parse_constant=reject_json_constant)
        except (ValueError, TypeError) as error:
            raise DataQualityError("DQ-03", "INVALID_JSON", str(error)) from error
        if isinstance(event, dict):
            # Transmission metadata is not part of a business or error identity.
            fingerprint_text = canonical_json({
                key: value for key, value in event.items()
                if key not in {"test_run_id", "published_at"}
            })
        common = normalize_event(event, sku_mapping)
        error_fields = {"rule_id": None, "error_code": None, "error_message": None}
        common_json = canonical_json(common)
    except DataQualityError as error:
        common_json = None
        error_fields = {"rule_id": error.rule_id, "error_code": error.code,
                        "error_message": str(error)}
    source = event if isinstance(event, dict) else {}
    return {
        "common_json": common_json,
        "source_channel_id": source.get("channel_id") if isinstance(source.get("channel_id"), str) else None,
        "source_event_id": source.get("event_id") if isinstance(source.get("event_id"), str) else None,
        "input_hash": hashlib.sha256((fingerprint_text or "").encode("utf-8")).hexdigest(),
        **error_fields,
    }


def text(record: dict, field: str) -> str:
    value = record.get(field)
    if value is None or (isinstance(value, str) and not value.strip()):
        raise DataQualityError("DQ-02", "MISSING_OR_INVALID_FIELD", field)
    if not isinstance(value, str):
        raise DataQualityError("DQ-03", "INVALID_STRING", field)
    return value


def integer(record: dict, field: str, *, bits: int | None = None) -> int:
    value = record.get(field)
    if value is None:
        raise DataQualityError("DQ-02", "MISSING_OR_INVALID_FIELD", field)
    if type(value) is not int:
        raise DataQualityError("DQ-03", "INVALID_INTEGER", field)
    if bits is not None and not -(2 ** (bits - 1)) <= value < 2 ** (bits - 1):
        raise DataQualityError("DQ-03", "INTEGER_OVERFLOW", field)
    if bits is None:
        decimal_amount(value, field)
    return value


def decimal_amount(value: int, field: str) -> int:
    if abs(value) >= 10 ** 18:
        raise DataQualityError("DQ-03", "DECIMAL_OVERFLOW", field)
    return value


def utc_time(value: str) -> str:
    try:
        time = datetime.fromisoformat(value)
    except ValueError as error:
        raise DataQualityError("DQ-03", "INVALID_TIMESTAMP", value) from error
    if time.tzinfo is None:
        raise DataQualityError("DQ-03", "TIMEZONE_REQUIRED")
    try:
        return time.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except (ValueError, OverflowError) as error:
        raise DataQualityError("DQ-03", "INVALID_TIMESTAMP", value) from error


def normalize_items(payload: dict, mapping: dict, event_type: str) -> list[dict]:
    items = payload.get("items")
    if items is None or items == []:
        raise DataQualityError("DQ-02", "MISSING_OR_INVALID_FIELD", "items")
    if not isinstance(items, list):
        raise DataQualityError("DQ-03", "INVALID_ITEMS")
    result = []
    for item in items:
        if item is None:
            raise DataQualityError("DQ-02", "NULL_ITEM")
        if not isinstance(item, dict):
            raise DataQualityError("DQ-03", "INVALID_ITEM")
        source_sku = text(item, "sku")
        if source_sku not in mapping:
            raise DataQualityError("DQ-04", "UNMAPPED_SKU", source_sku)
        sku_id = mapping[source_sku]
        if not isinstance(sku_id, str) or not sku_id.strip():
            raise DataQualityError("DQ-04", "INVALID_SKU_MAPPING", source_sku)
        row = {"order_item_id": text(item, "item_no"), "sku_id": sku_id}
        quantity, price = integer(item, "qty", bits=32), integer(item, "price")
        line_amount = decimal_amount(quantity * price, "qty * price")
        if event_type == "ORDER_CREATED":
            row.update(quantity=quantity, unit_price=price, line_amount=line_amount)
        elif event_type == "ORDER_CANCELLED":
            row.update(cancelled_quantity=quantity, cancelled_amount=line_amount)
        else:
            row.update(refunded_quantity=quantity, refunded_amount=line_amount)
        result.append(row)
    return result


def normalize_event(event: dict, sku_mapping: dict) -> dict:
    """Return JSON-friendly common business fields without mutating the source."""
    if not isinstance(event, dict):
        raise DataQualityError("DQ-03", "INVALID_EVENT")
    text(event, "channel_id")
    if event.get("channel_id") != "A":
        raise DataQualityError("DQ-04", "UNSUPPORTED_CHANNEL")
    version = text(event, "schema_version")
    if version not in ORDER_TOTAL_FIELDS:
        raise DataQualityError("DQ-04", "UNSUPPORTED_SCHEMA_VERSION", version)
    event_name = text(event, "event_name")
    if event_name not in EVENT_TYPES:
        raise DataQualityError("DQ-03", "UNSUPPORTED_EVENT_TYPE", event_name)
    event_type = EVENT_TYPES[event_name]
    payload = event.get("payload")
    if payload is None:
        raise DataQualityError("DQ-02", "MISSING_OR_INVALID_FIELD", "payload")
    if not isinstance(payload, dict):
        raise DataQualityError("DQ-03", "INVALID_PAYLOAD")
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
            raise DataQualityError("DQ-03", "UNSUPPORTED_CANCEL_REASON")
        data = {"cancellation_id": text(payload, "cancel_no"), "cancel_reason": "CUSTOMER_REQUEST",
                "items": normalize_items(payload, sku_mapping, event_type)}
    elif event_type in {"PAYMENT_FAILED", "PAYMENT_SUCCEEDED"}:
        if text(payload, "method") != "card":
            raise DataQualityError("DQ-03", "UNSUPPORTED_PAYMENT_METHOD")
        amount_field = "requested_amount" if event_type == "PAYMENT_FAILED" else "paid_amount"
        data = {"payment_id": text(payload, "payment_no"), "payment_method": "CARD",
                amount_field: integer(payload, "amount")}
        if event_type == "PAYMENT_FAILED":
            if text(payload, "error") != "card_declined":
                raise DataQualityError("DQ-03", "UNSUPPORTED_FAILURE_CODE")
            data["failure_code"] = "CARD_DECLINED"
    else:
        data = {"refund_id": text(payload, "refund_no"), "payment_id": text(payload, "payment_no"),
                "refund_amount": integer(payload, "amount"),
                "items": normalize_items(payload, sku_mapping, event_type)}
    result["data"] = data
    return result
