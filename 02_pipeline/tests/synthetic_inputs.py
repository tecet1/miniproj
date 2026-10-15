"""파이프라인 테스트가 함께 쓰는 합성 입력: 시나리오에서 생성기로 만든 줄, 필드를 하나씩 망가뜨린 변형 줄.

data_fixtures 파일을 읽지 않고 시나리오(scenarios.json)에서 생성기로 직접 만든다.
"""
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "01_contracts" / "src")]

import generate  # noqa: E402
import scenario_loader  # noqa: E402

# 채널별 필드 이름 (이벤트 명세 2.2절). 손으로 만든 변형 이벤트를 만들 때 쓴다.
FIELDS = {
    "A": {"payment_id": "payment_id", "code": "event_type", "id": "event_id", "order_key": "order_id", "cancel_code": "order.cancelled", "order_code": "order.created", "pay_code": "payment.succeeded", "refund_code": "refund.succeeded",
          "items": "items", "sku": "sku", "qty": "quantity", "discount": (None, "discount_amount"), "shipping": (None, "shipping_fee"),
          "method": "method", "amount": "amount", "refund_type": "type", "refund_qty": "refund_quantity", "user": (None, "user_id")},
    "B": {"payment_id": "paymentId", "code": "eventName", "id": "eventId", "order_key": "orderNo", "cancel_code": "ORDER_CANCELED", "order_code": "ORDER_PLACED", "pay_code": "PAY_APPROVED", "refund_code": "REFUND_DONE",
          "items": "orderItems", "sku": "sellerSku", "qty": "qty", "discount": ("discount", "total"), "shipping": ("delivery", "fee"),
          "method": "payMethod", "amount": "paidAmount", "refund_type": "claimType", "refund_qty": "refundQty", "user": ("buyer", "memberId")},
    "C": {"payment_id": "pay_no", "code": "evt_cd", "id": "evt_id", "order_key": "ord_no", "cancel_code": "O09", "order_code": "O01", "pay_code": "P01", "refund_code": "R01",
          "items": "lines", "sku": "goods_cd", "qty": "qty", "discount": (None, "dc_amt"), "shipping": (None, "dlv_amt"),
          "method": "pay_type", "amount": "pay_amt", "refund_type": "clm_tp", "refund_qty": "rfd_qty", "user": (None, "mem_no")},
}


def _set(container, path, value):
    """path = (바깥 키 또는 None, 안쪽 키). 바깥 키가 있으면 그 객체 안의 안쪽 키를 바꾼다. value가 DELETE면 지운다."""
    outer, inner = path
    target = container if outer is None else container[outer]
    key = inner
    if value is DELETE:
        target.pop(key, None)
    else:
        target[key] = value


DELETE = object()


def scenario_lines(channel):
    """시나리오(scenarios.json)에서 생성기가 만든 채널 원본 줄. data_fixtures 파일을 읽지 않고 같은 생성기로 직접 만든다."""
    skus = scenario_loader.load_skus(ROOT / "01_contracts" / "contracts" / "skus.json")
    scenarios = scenario_loader.load_scenarios(ROOT / "01_contracts" / "contracts" / "scenarios.json")
    return generate.build_lines(channel, scenarios, skus)


def crafted_lines():
    """채널마다 정상 이벤트 하나씩을 골라 필드를 하나씩 망가뜨린 변형 줄을 만든다 (Silver가 격리하는지·받아들이는지 경계를 맞춰 본다)."""
    out = []
    base = {}
    for channel in "ABC":
        names = FIELDS[channel]
        for line in scenario_lines(channel):
            raw = json.loads(json.loads(line)["payload"])
            for kind, code in (("order", names["order_code"]), ("pay", names["pay_code"]), ("refund", names["refund_code"]), ("cancel", names["cancel_code"])):
                if raw[names["code"]] == code and ({"refund": "S09", "cancel": "S08"}.get(kind, "S04") in raw[names["id"]]):
                    base.setdefault((channel, kind), raw)
    counter = 0
    for channel in "ABC":
        n = FIELDS[channel]
        variants = [
            ("pay", "missing method", lambda r: _set(r, (None, n["method"]), DELETE)),
            ("pay", "empty payment id", lambda r: _set(r, (None, n["payment_id"]), "")),
            ("pay", "unmapped method", lambda r: _set(r, (None, n["method"]), "bitcoin")),
            ("pay", "negative amount", lambda r: _set(r, (None, n["amount"]), -5)),
            ("pay", "zero amount", lambda r: _set(r, (None, n["amount"]), 0)),
            ("order", "unknown sku", lambda r: _set(r[n["items"]][0], (None, n["sku"]), "SKU-999")),
            ("order", "zero quantity", lambda r: _set(r[n["items"]][0], (None, n["qty"]), 0)),
            ("order", "text quantity", lambda r: _set(r[n["items"]][0], (None, n["qty"]), "N/A")),
            ("order", "huge discount", lambda r: _set(r[n["items"]][0], n["discount"], 10 ** 9)),
            ("order", "empty items", lambda r: r.__setitem__(n["items"], [])),
            ("order", "missing items", lambda r: r.pop(n["items"])),
            ("order", "negative shipping", lambda r: _set(r, n["shipping"], -1)),
            ("order", "missing event id", lambda r: r.pop(n["id"])),
            ("order", "empty event id", lambda r: _set(r, (None, n["id"]), "")),
            ("order", "empty order id", lambda r: _set(r, (None, n["order_key"]), "")),
            ("order", "empty user", lambda r: _set(r, n["user"], "")),
            ("order", "missing user", lambda r: _set(r, n["user"], DELETE)),
            ("cancel", "cancel of a paid order (S01, 다른 변형이 건드리지 않은 주문)", lambda r: r.__setitem__(n["order_key"], f"{channel}-S01-1")),
            ("refund", "unknown refund type", lambda r: _set(r, (None, n["refund_type"]), "weird")),
            ("refund", "zero refund quantity", lambda r: _set(r, (None, n["refund_qty"]), 0)),
        ]
        for kind, label, mutate in variants:
            raw = json.loads(json.dumps(base[(channel, kind)]))
            mutate(raw)
            counter += 1
            if n["id"] in raw:
                raw[n["id"]] = f"CRAFTED-{channel}-{counter}"
            out.append({"channel": channel, "payload": json.dumps(raw, ensure_ascii=False)})
    return out
