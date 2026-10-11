"""Generate six fixed channel-A PoC messages without external dependencies."""

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


BASE_TIME = datetime(2026, 10, 10, 3, 0, tzinfo=timezone.utc)
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "data" / "fixtures" / "channel_a"


def make_item(item_no: str, sku: str, qty: int, price: int) -> dict:
    return {"item_no": item_no, "sku": sku, "qty": qty, "price": price}


def make_event(sequence: int, *, event_name: str, order_no: str, payload: dict) -> dict:
    return {
        "schema_version": "1.0",
        "channel_id": "A",
        "event_id": f"a-poc-v1-{sequence:02d}",
        "event_name": event_name,
        "occurred_at": (BASE_TIME + timedelta(minutes=sequence - 1)).isoformat(),
        "order_no": order_no,
        "customer_no": "U-001",
        "payload": payload,
    }


def build_events(schema_version: str = "1.0") -> list[dict]:
    if schema_version not in {"1.0", "2.0"}:
        raise ValueError("Unsupported A schema version")
    events = [
        make_event(1, event_name="order_created", order_no="A-001", payload={
            "total": 50000,
            "items": [make_item("A-001-1", "A-SKU-001", 2, 25000)],
        }),
        make_event(2, event_name="order_cancelled", order_no="A-001", payload={
            "cancel_no": "AC-001", "reason": "customer",
            "items": [make_item("A-001-1", "A-SKU-001", 2, 25000)],
        }),
        make_event(3, event_name="order_created", order_no="A-002", payload={
            "total": 80000,
            "items": [
                make_item("A-002-1", "A-SKU-001", 2, 25000),
                make_item("A-002-2", "A-SKU-002", 1, 30000),
            ],
        }),
        make_event(4, event_name="payment_failed", order_no="A-002", payload={
            "payment_no": "AP-001", "amount": 80000,
            "method": "card", "error": "card_declined",
        }),
        make_event(5, event_name="payment_succeeded", order_no="A-002", payload={
            "payment_no": "AP-002", "amount": 80000, "method": "card",
        }),
        make_event(6, event_name="refund_completed", order_no="A-002", payload={
            "refund_no": "AR-001", "payment_no": "AP-002", "amount": 25000,
            "items": [make_item("A-002-1", "A-SKU-001", 1, 25000)],
        }),
    ]
    for event in events:
        event["schema_version"] = schema_version
        if schema_version == "2.0" and event["event_name"] == "order_created":
            payload = event["payload"]
            payload["order_total"] = payload.pop("total")
    return events


def write_events(events: list[dict], output_dir: Path = OUTPUT_DIR, *, filename: str = "events.jsonl") -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / filename
    with path.open("w", encoding="utf-8", newline="\r\n") as stream:
        for event in events:
            stream.write(json.dumps(event, ensure_ascii=False, allow_nan=False) + "\n")
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema-version", choices=("1.0", "2.0"), default="1.0")
    args = parser.parse_args()
    events = build_events(args.schema_version)
    filename = "events.jsonl" if args.schema_version == "1.0" else "events_v2.jsonl"
    path = write_events(events, filename=filename)
    print(f"Created {len(events)} events: {path}")
