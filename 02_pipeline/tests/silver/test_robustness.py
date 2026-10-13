"""비정상 입력에도 변환이 ConversionError만 내는지 확인한다 (채널 A·B·C 공통).

[왜] Spark UDF에서 ConversionError가 아닌 예외가 나면 그 한 건이 격리되지 않고 배치 전체가 실패한다.
[기대값] 이 파일에 직접 적은 값이다. 변환 코드의 상수를 가져다 쓰지 않는다.
  1. 이름 붙인 사례: ID가 비었거나 문자열이 아님, items 형태 오류, 선택 필드, 해시할 수 없는 코드 값
  2. 전수 검사: 샘플 이벤트의 모든 필드를 이상한 값으로 바꾸거나 지워도 '오류(ConversionError)' 또는 '올바른 공통 이벤트'만 나온다
"""
import copy
import sys
import unittest
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "02_pipeline" / "silver"))

from channels import channel_a, channel_b, channel_c
from conversion_error import ConversionError

TO_EVENT = {"A": channel_a.to_event, "B": channel_b.to_event, "C": channel_c.to_event}

# 채널별 샘플 원본: 주문 생성, 결제 성공, 결제 실패, 주문 취소, 환불 (고정 입력과 같은 값)
SAMPLES = {
    "A": {
        "order": {"event_id": "A-S01-1-E1", "event_type": "order.created", "occurred_at": "2026-10-12T01:00:00.000Z",
                  "order_id": "A-S01-1", "user_id": "U01", "shipping_fee": 0,
                  "items": [{"order_item_id": "A-S01-1-1", "sku": "SKU-004", "product_name": "러닝화", "category": "신발",
                             "quantity": 1, "unit_price": 89000, "discount_amount": 0}]},
        "pay_ok": {"event_id": "A-S01-1-E2", "event_type": "payment.succeeded", "occurred_at": "2026-10-12T01:00:30.000Z",
                   "order_id": "A-S01-1", "user_id": "U01", "payment_id": "A-S01-1-P1", "method": "card", "amount": 89000},
        "pay_fail": {"event_id": "A-S06-1-E2", "event_type": "payment.failed", "occurred_at": "2026-10-12T01:30:20.000Z",
                     "order_id": "A-S06-1", "user_id": "U04", "payment_id": "A-S06-1-P1", "method": "card",
                     "amount": 59000, "failure_reason": "limit_exceeded"},
        "cancel": {"event_id": "A-S08-1-E2", "event_type": "order.cancelled", "occurred_at": "2026-10-12T01:40:30.000Z",
                   "order_id": "A-S08-1", "user_id": "U05", "reason": "change_of_mind"},
        "refund": {"event_id": "A-S09-1-E3", "event_type": "refund.succeeded", "occurred_at": "2026-10-12T02:00:30.000Z",
                   "order_id": "A-S09-1", "user_id": "U06", "refund_id": "A-S09-1-R1", "payment_id": "A-S09-1-P1",
                   "order_item_id": "A-S09-1-1", "refund_quantity": 1, "amount": 35000, "type": "return",
                   "reason": "defect"},
    },
    "B": {
        "order": {"eventId": "B-S01-1-E1", "eventName": "ORDER_PLACED", "occurredAtMs": 1791766800000,
                  "orderNo": "B-S01-1", "buyer": {"memberId": "U01"}, "delivery": {"fee": 0},
                  "orderItems": [{"lineId": "B-S01-1-1", "sellerSku": "SKU-004", "itemName": "러닝화",
                                  "categoryName": "신발", "qty": 1, "unitPrice": 89000, "discount": {"total": 0}}]},
        "pay_ok": {"eventId": "B-S01-1-E2", "eventName": "PAY_APPROVED", "occurredAtMs": 1791766830000,
                   "orderNo": "B-S01-1", "buyer": {"memberId": "U01"}, "paymentId": "B-S01-1-P1", "payMethod": "CARD",
                   "paidAmount": 89000},
        "pay_fail": {"eventId": "B-S06-1-E2", "eventName": "PAY_DECLINED", "occurredAtMs": 1791768620000,
                     "orderNo": "B-S06-1", "buyer": {"memberId": "U04"}, "paymentId": "B-S06-1-P1", "payMethod": "CARD",
                     "paidAmount": 59000, "declineCode": "LIMIT"},
        "cancel": {"eventId": "B-S08-1-E2", "eventName": "ORDER_CANCELED", "occurredAtMs": 1791769230000,
                   "orderNo": "B-S08-1", "buyer": {"memberId": "U05"}, "cancelReason": "BUYER_REMORSE"},
        "refund": {"eventId": "B-S09-1-E3", "eventName": "REFUND_DONE", "occurredAtMs": 1791770430000,
                   "orderNo": "B-S09-1", "buyer": {"memberId": "U06"}, "refundId": "B-S09-1-R1",
                   "paymentId": "B-S09-1-P1", "lineId": "B-S09-1-1", "refundQty": 1, "refundAmount": 35000,
                   "claimType": "RETURN", "claimReason": "DEFECTIVE"},
    },
    "C": {
        "order": {"evt_id": "C-S01-1-E1", "evt_cd": "O01", "evt_dt": "2026-10-12 10:00:00", "ord_no": "C-S01-1",
                  "mem_no": "U01", "dlv_amt": 0,
                  "lines": [{"seq": 1, "goods_cd": "SKU-004", "goods_nm": "러닝화", "cate_nm": "신발", "qty": 1,
                             "price": 89000, "dc_amt": 0}]},
        "pay_ok": {"evt_id": "C-S01-1-E2", "evt_cd": "P01", "evt_dt": "2026-10-12 10:00:30", "ord_no": "C-S01-1",
                   "mem_no": "U01", "pay_no": "C-S01-1-P1", "pay_type": "10", "pay_amt": 89000},
        "pay_fail": {"evt_id": "C-S06-1-E2", "evt_cd": "P02", "evt_dt": "2026-10-12 10:30:20", "ord_no": "C-S06-1",
                     "mem_no": "U04", "pay_no": "C-S06-1-P1", "pay_type": "10", "pay_amt": 59000, "fail_cd": "F1"},
        "cancel": {"evt_id": "C-S08-1-E2", "evt_cd": "O09", "evt_dt": "2026-10-12 10:40:30", "ord_no": "C-S08-1",
                   "mem_no": "U05", "rsn_cd": "01"},
        "refund": {"evt_id": "C-S09-1-E3", "evt_cd": "R01", "evt_dt": "2026-10-12 11:00:30", "ord_no": "C-S09-1",
                   "mem_no": "U06", "rfd_no": "C-S09-1-R1", "pay_no": "C-S09-1-P1", "seq": 1, "rfd_qty": 1,
                   "rfd_amt": 35000, "clm_tp": "2", "rsn_cd": "02"},
    },
}

# 채널별 원본 필드 이름 (이벤트 명세 2.2절)
NAMES = {
    "A": {"event_id": "event_id", "order_id": "order_id", "code": "event_type", "items": "items", "sku": "sku",
          "item_id": "order_item_id", "item_name": "product_name", "item_category": "category", "user": "user_id",
          "payment_id": "payment_id", "method": "method", "fail": "failure_reason", "cancel": "reason",
          "refund_id": "refund_id", "refund_type": "type", "refund_reason": "reason"},
    "B": {"event_id": "eventId", "order_id": "orderNo", "code": "eventName", "items": "orderItems", "sku": "sellerSku",
          "item_id": "lineId", "item_name": "itemName", "item_category": "categoryName", "user": None,
          "payment_id": "paymentId", "method": "payMethod", "fail": "declineCode", "cancel": "cancelReason",
          "refund_id": "refundId", "refund_type": "claimType", "refund_reason": "claimReason"},
    "C": {"event_id": "evt_id", "order_id": "ord_no", "code": "evt_cd", "items": "lines", "sku": "goods_cd",
          "item_id": None, "item_name": "goods_nm", "item_category": "cate_nm", "user": "mem_no",
          "payment_id": "pay_no", "method": "pay_type", "fail": "fail_cd", "cancel": "rsn_cd",
          "refund_id": "rfd_no", "refund_type": "clm_tp", "refund_reason": "rsn_cd"},
}


def reason_of(channel, raw):
    """변환 결과가 오류면 그 reason_code, 성공이면 None. ConversionError 외 예외는 그대로 올라온다."""
    try:
        TO_EVENT[channel](raw)
    except ConversionError as exc:
        return exc.reason_code
    return None


def with_value(raw, path, value):
    """raw의 복사본에서 path 위치의 값을 바꿔 돌려준다."""
    data = copy.deepcopy(raw)
    target = data
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    return data


class TestIdFields(unittest.TestCase):
    """ID 필드: 비었으면 MISSING_REQUIRED, 문자열이 아니면 INVALID_TYPE."""

    def id_paths(self, channel):
        n = NAMES[channel]
        paths = [("order", (n["event_id"],)), ("order", (n["order_id"],)), ("order", (n["items"], 0, n["sku"])),
                 ("pay_ok", (n["payment_id"],)), ("refund", (n["refund_id"],)), ("refund", (n["payment_id"],))]
        if n["item_id"]:
            paths += [("order", (n["items"], 0, n["item_id"])), ("refund", (n["item_id"],))]
        return paths

    def test_empty_string_is_missing_required(self):
        for channel in "ABC":
            for sample, path in self.id_paths(channel):
                with self.subTest(channel=channel, path=path):
                    raw = with_value(SAMPLES[channel][sample], path, "")
                    self.assertEqual(reason_of(channel, raw), "MISSING_REQUIRED")

    def test_non_string_is_invalid_type(self):
        for channel in "ABC":
            for sample, path in self.id_paths(channel):
                for bad in (123, ["x"], {"a": 1}):
                    with self.subTest(channel=channel, path=path, bad=bad):
                        raw = with_value(SAMPLES[channel][sample], path, bad)
                        self.assertEqual(reason_of(channel, raw), "INVALID_TYPE")


class TestItemsShape(unittest.TestCase):
    """items: 비어 있으면 MISSING_REQUIRED, 배열이 아니거나 원소가 객체가 아니면 INVALID_TYPE."""

    def test_empty_items_is_missing_required(self):
        for channel in "ABC":
            with self.subTest(channel=channel):
                raw = with_value(SAMPLES[channel]["order"], (NAMES[channel]["items"],), [])
                self.assertEqual(reason_of(channel, raw), "MISSING_REQUIRED")

    def test_bad_shape_is_invalid_type(self):
        for channel in "ABC":
            for bad in ("abc", 123, {"a": 1}, [None], [1], ["x"]):
                with self.subTest(channel=channel, bad=bad):
                    raw = with_value(SAMPLES[channel]["order"], (NAMES[channel]["items"],), bad)
                    self.assertEqual(reason_of(channel, raw), "INVALID_TYPE")


class TestOptionalText(unittest.TestCase):
    """선택 필드(user_id, 상품명, 카테고리)는 없거나 null이거나 빈 문자열이면 None이다 (명세 1.2절, 2절)."""

    def test_item_name_and_category_are_optional(self):
        for channel in "ABC":
            n = NAMES[channel]
            for key in (n["item_name"], n["item_category"]):
                for how in ("missing", None, ""):
                    with self.subTest(channel=channel, key=key, how=how):
                        raw = copy.deepcopy(SAMPLES[channel]["order"])
                        if how == "missing":
                            del raw[n["items"]][0][key]
                        else:
                            raw[n["items"]][0][key] = how
                        item = TO_EVENT[channel](raw)["items"][0]
                        self.assertIsNone(item["product_name" if key == n["item_name"] else "category"])

    def test_empty_user_id_is_none(self):
        for channel, sample in (("A", with_value(SAMPLES["A"]["order"], ("user_id",), "")),
                                ("B", with_value(SAMPLES["B"]["order"], ("buyer", "memberId"), "")),
                                ("C", with_value(SAMPLES["C"]["order"], ("mem_no",), ""))):
            with self.subTest(channel=channel):
                self.assertIsNone(TO_EVENT[channel](sample)["user_id"])

    def test_non_string_user_id_is_invalid_type(self):
        for channel, sample in (("A", with_value(SAMPLES["A"]["order"], ("user_id",), 5)),
                                ("B", with_value(SAMPLES["B"]["order"], ("buyer", "memberId"), 5)),
                                ("C", with_value(SAMPLES["C"]["order"], ("mem_no",), 5))):
            with self.subTest(channel=channel):
                self.assertEqual(reason_of(channel, sample), "INVALID_TYPE")


class TestCodeValues(unittest.TestCase):
    """코드 값이 리스트나 객체여도 예외 없이 처리한다: 이벤트 코드는 UNKNOWN_EVENT_TYPE, 환불 유형은 INVALID_VALUE, 나머지는 OTHER."""

    UNHASHABLE = ([], {}, ["card"], {"a": 1})

    def test_event_code(self):
        for channel in "ABC":
            for bad in self.UNHASHABLE:
                with self.subTest(channel=channel, bad=bad):
                    raw = with_value(SAMPLES[channel]["order"], (NAMES[channel]["code"],), bad)
                    self.assertEqual(reason_of(channel, raw), "UNKNOWN_EVENT_TYPE")

    def test_refund_type(self):
        for channel in "ABC":
            for bad in self.UNHASHABLE:
                with self.subTest(channel=channel, bad=bad):
                    raw = with_value(SAMPLES[channel]["refund"], (NAMES[channel]["refund_type"],), bad)
                    self.assertEqual(reason_of(channel, raw), "INVALID_VALUE")

    def test_other_codes_become_other(self):
        for channel in "ABC":
            n = NAMES[channel]
            cases = (("pay_ok", n["method"], "payment_method"), ("pay_fail", n["fail"], "fail_reason_code"),
                     ("cancel", n["cancel"], "cancel_reason_code"), ("refund", n["refund_reason"], "refund_reason_code"))
            for sample, key, field in cases:
                for bad in self.UNHASHABLE:
                    with self.subTest(channel=channel, key=key, bad=bad):
                        raw = with_value(SAMPLES[channel][sample], (key,), bad)
                        self.assertEqual(TO_EVENT[channel](raw)[field], "OTHER")


# ── 전수 검사 ────────────────────────────────────────────────────────────────────────
METHODS = {"CARD", "EASY_PAY", "BANK_TRANSFER", "VIRTUAL_ACCOUNT", "OTHER"}
REASONS = {"CHANGE_OF_MIND", "DEFECT", "WRONG_ITEM", "DELAY", "OTHER"}
FAIL_REASONS = {"LIMIT_EXCEEDED", "CARD_DECLINED", "TIMEOUT", "OTHER"}
EVENT_TYPES = {"ORDER_CREATED", "ORDER_CANCELLED", "PAYMENT_SUCCEEDED", "PAYMENT_FAILED", "REFUND_SUCCEEDED"}
WEIRD_VALUES = (None, "", "abc", 123, -5, 1.5, True, [], {}, [None], ["x"], {"a": 1}, 10**30, -10**30)


def is_text(value):
    return isinstance(value, str) and value != ""


def is_int(value):
    return type(value) is int


def problems_in(event):
    """공통 이벤트가 명세 1절의 형태를 지키는지 검사해 어긋난 곳을 문장 목록으로 돌려준다."""
    found = []
    if not (is_text(event.get("event_id")) and is_text(event.get("order_id"))):
        found.append("event_id/order_id")
    if event.get("event_type") not in EVENT_TYPES:
        found.append("event_type")
    if event.get("user_id") is not None and not is_text(event["user_id"]):
        found.append("user_id")
    time = event.get("event_time")
    if not (isinstance(time, datetime) and time.tzinfo is not None):
        found.append("event_time")
    kind = event.get("event_type")
    if kind == "ORDER_CREATED":
        if not is_int(event.get("shipping_fee")):
            found.append("shipping_fee")
        items = event.get("items")
        if not (isinstance(items, list) and items):
            found.append("items")
        for item in items or []:
            if not (is_text(item.get("order_item_id")) and is_text(item.get("product_id"))):
                found.append("item ids")
            for key in ("product_name", "category"):
                if item.get(key) is not None and not is_text(item[key]):
                    found.append(key)
            if not all(is_int(item.get(key)) for key in ("quantity", "unit_price", "item_discount")):
                found.append("item numbers")
    elif kind in ("PAYMENT_SUCCEEDED", "PAYMENT_FAILED"):
        if not is_text(event.get("payment_id")):
            found.append("payment_id")
        if event.get("payment_method") not in METHODS or not is_int(event.get("payment_amount")):
            found.append("payment method/amount")
        if event.get("fail_reason_code") is not None and event["fail_reason_code"] not in FAIL_REASONS:
            found.append("fail_reason_code")
    elif kind == "ORDER_CANCELLED":
        if event.get("cancel_reason_code") not in REASONS:
            found.append("cancel_reason_code")
    elif kind == "REFUND_SUCCEEDED":
        if not all(is_text(event.get(key)) for key in ("refund_id", "payment_id", "order_item_id")):
            found.append("refund ids")
        if not (is_int(event.get("refund_quantity")) and is_int(event.get("refund_amount"))):
            found.append("refund numbers")
        if event.get("refund_type") not in ("CANCEL", "RETURN") or event.get("refund_reason_code") not in REASONS:
            found.append("refund type/reason")
    return found


def all_paths(node, prefix=()):
    """중첩 dict/list 안의 모든 위치를 튜플 경로로 내준다."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield prefix + (key,)
            yield from all_paths(value, prefix + (key,))
    elif isinstance(node, list):
        for index, value in enumerate(node):
            yield from all_paths(value, prefix + (index,))


def without(raw, path):
    data = copy.deepcopy(raw)
    target = data
    for key in path[:-1]:
        target = target[key]
    if isinstance(target, dict):
        del target[path[-1]]
    else:
        target.pop(path[-1])
    return data


class TestExhaustiveMutation(unittest.TestCase):
    def test_every_field_survives_weird_values(self):
        """모든 필드를 이상한 값으로 바꾸거나 지워도 ConversionError이거나 올바른 공통 이벤트다."""
        failures = []
        for channel in "ABC":
            for sample, raw in SAMPLES[channel].items():
                for path in all_paths(raw):
                    variants = [with_value(raw, path, value) for value in WEIRD_VALUES] + [without(raw, path)]
                    for variant in variants:
                        try:
                            event = TO_EVENT[channel](variant)
                        except ConversionError:
                            continue
                        except Exception as exc:  # 이게 나오면 UDF가 죽는다
                            failures.append(f"{channel}/{sample}/{path}: {type(exc).__name__}: {exc}")
                            continue
                        for problem in problems_in(event):
                            failures.append(f"{channel}/{sample}/{path}: accepted bad {problem}")
        self.assertEqual(failures[:10], [], f"총 {len(failures)}건")


if __name__ == "__main__":
    unittest.main()
