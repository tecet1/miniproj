"""C 채널(스포츠 전문몰) 원본 dict -> 공통 이벤트 dict. 생성기의 channel_c.to_raw의 반대 방향이다.

[입력] json.loads로 읽은 원본 dict 하나 (Bronze payload 한 건 = 이벤트 한 건). 약어 snake_case.
  공통: evt_id, evt_cd, evt_dt("yyyy-MM-dd HH:mm:ss", KST, 시간대 표기 없음), ord_no, mem_no(비로그인이면 "")
  O01  dlv_amt, lines[{seq, goods_cd, goods_nm, cate_nm, qty, price, dc_amt}]
  O09  rsn_cd
  P01  pay_no, pay_type, pay_amt
  P02  pay_no, pay_type, pay_amt, fail_cd
  R01  rfd_no, pay_no, seq(환불 대상 상품 순번), rfd_qty, rfd_amt, clm_tp, rsn_cd
  (이벤트 명세 2.1~2.3절의 매핑표를 따른다)

[출력] 공통 이벤트 dict (채널 A와 같은 모양, channel은 "C")

[오류] 채널 A와 같다. 새로 생긴 점만 적는다.
  - order_item_id는 원본에 없고 ord_no + "-" + seq로 만든다 (주문과 환불에서 같은 규칙). seq는 정수여야 한다.
  - evt_dt는 KST다. UTC로 바꾼다. 형식이 틀리면 INVALID_TYPE.
  - mem_no가 빈 문자열이거나 없으면 user_id는 None (오류 아님).
"""
from datetime import UTC, datetime, timedelta, timezone

from conversion_error import ConversionError
from fields import (get_optional_str, get_required, get_required_dict_list, get_required_int,
                    get_required_str, lookup, map_code)

CHANNEL = "C"
KST = timezone(timedelta(hours=9))   # 한국은 서머타임이 없어 고정 오프셋으로 충분하다 (표준 라이브러리만 사용)

# ── 매핑 상수: 원본 코드 -> 공통 값 (이벤트 명세 2.1, 2.3절) ─────────────────────────────
EVENT_TYPES = {
    "O01": "ORDER_CREATED",
    "O09": "ORDER_CANCELLED",
    "P01": "PAYMENT_SUCCEEDED",
    "P02": "PAYMENT_FAILED",
    "R01": "REFUND_SUCCEEDED",
}
PAYMENT_METHODS = {"10": "CARD", "20": "EASY_PAY", "30": "BANK_TRANSFER", "40": "VIRTUAL_ACCOUNT"}
REASONS = {"01": "CHANGE_OF_MIND", "02": "DEFECT", "03": "WRONG_ITEM", "04": "DELAY"}
FAIL_REASONS = {"F1": "LIMIT_EXCEEDED", "F2": "CARD_DECLINED", "F3": "TIMEOUT"}
REFUND_TYPES = {"1": "CANCEL", "2": "RETURN"}   # OTHER 없음: 모르는 값은 INVALID_VALUE


# ── 작은 도구 (A, B와 다른 부분) ───────────────────────────────────────────────────────
def parse_time(text: str) -> datetime:
    """"2026-10-12 10:00:00" (KST) -> 2026-10-12 01:00:00 UTC (timezone 정보가 있는 datetime)."""
    try:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S").replace(tzinfo=KST).astimezone(UTC)
    except (ValueError, TypeError):
        raise ConversionError("INVALID_TYPE", f"evt_dt={text!r}")


def read_user_id(raw: dict):
    """mem_no를 돌려준다. 빈 문자열이거나 없으면 None. 오류가 아니다."""
    return get_optional_str(raw, "mem_no")


def make_item_id(order_no: str, seq: int) -> str:
    """상품 행 ID. ord_no + "-" + seq (예: "C-S09-1" + 1 -> "C-S09-1-1")."""
    return f"{order_no}-{seq}"


# ── 이벤트 유형별 필드: raw -> 그 유형에만 있는 공통 필드 dict ────────────────────────────
def build_item(order_no: str, item: dict) -> dict:
    """lines[] 한 건 -> 공통 상품 행. order_item_id는 주문 번호와 seq로 만든다."""
    return {
        "order_item_id": make_item_id(order_no, get_required_int(item, "seq")),
        "product_id": get_required_str(item, "goods_cd"),
        "product_name": get_optional_str(item, "goods_nm"),
        "category": get_optional_str(item, "cate_nm"),
        "quantity": get_required_int(item, "qty"),
        "unit_price": get_required_int(item, "price"),
        "item_discount": get_required_int(item, "dc_amt"),
    }


def build_order(raw: dict) -> dict:
    """O01 -> {shipping_fee, items}."""
    order_no = get_required_str(raw, "ord_no")
    return {
        "shipping_fee": get_required_int(raw, "dlv_amt"),
        "items": [build_item(order_no, item) for item in get_required_dict_list(raw, "lines")],
    }


def build_cancel(raw: dict) -> dict:
    """O09 -> {cancel_reason_code}. rsn_cd는 선택 필드라 raw.get + map_code."""
    return {"cancel_reason_code": map_code(REASONS, raw.get("rsn_cd"))}


def build_payment_succeeded(raw: dict) -> dict:
    """P01 -> {payment_id, payment_method, payment_amount, fail_reason_code: None}."""
    return {
        "payment_id": get_required_str(raw, "pay_no"),
        "payment_method": map_code(PAYMENT_METHODS, get_required(raw, "pay_type")),
        "payment_amount": get_required_int(raw, "pay_amt"),
        "fail_reason_code": None,
    }


def build_payment_failed(raw: dict) -> dict:
    """P02 -> 성공과 같은 필드 + fail_reason_code(fail_cd를 map_code로)."""
    fields = build_payment_succeeded(raw)
    fields["fail_reason_code"] = map_code(FAIL_REASONS, raw.get("fail_cd"))
    return fields


def build_refund(raw: dict) -> dict:
    """R01 -> 환불 필드. clm_tp가 REFUND_TYPES에 없으면 INVALID_VALUE. order_item_id는 ord_no + seq."""
    raw_type = get_required(raw, "clm_tp")
    refund_type = lookup(REFUND_TYPES, raw_type)
    if refund_type is None:
        raise ConversionError("INVALID_VALUE", f"clm_tp={raw_type!r}")
    return {
        "refund_id": get_required_str(raw, "rfd_no"),
        "payment_id": get_required_str(raw, "pay_no"),
        "order_item_id": make_item_id(get_required_str(raw, "ord_no"), get_required_int(raw, "seq")),
        "refund_quantity": get_required_int(raw, "rfd_qty"),
        "refund_amount": get_required_int(raw, "rfd_amt"),
        "refund_type": refund_type,
        "refund_reason_code": map_code(REASONS, raw.get("rsn_cd")),
    }


# 공통 event_type -> 유형별 필드 함수
BUILDERS = {
    "ORDER_CREATED": build_order,
    "ORDER_CANCELLED": build_cancel,
    "PAYMENT_SUCCEEDED": build_payment_succeeded,
    "PAYMENT_FAILED": build_payment_failed,
    "REFUND_SUCCEEDED": build_refund,
}


# ── 진입점 ───────────────────────────────────────────────────────────────────────────
def to_event(raw: dict) -> dict:
    """원본 dict 하나를 공통 이벤트 dict로 바꾼다. 순서는 채널 A와 같다.

      1. evt_cd를 EVENT_TYPES로 바꾼다. 표에 없으면 ConversionError("UNKNOWN_EVENT_TYPE", ...).
      2. 공통 필드를 만든다.
      3. BUILDERS[event_type](raw)의 결과를 합쳐 돌려준다.
    """
    code = get_required(raw, "evt_cd")
    event_type = lookup(EVENT_TYPES, code)
    if event_type is None:
        raise ConversionError("UNKNOWN_EVENT_TYPE", repr(code))
    common = {
        "event_id": get_required_str(raw, "evt_id"),
        "event_type": event_type,
        "channel": CHANNEL,
        "order_id": get_required_str(raw, "ord_no"),
        "user_id": read_user_id(raw),
        "event_time": parse_time(get_required(raw, "evt_dt")),
    }
    return {**common, **BUILDERS[event_type](raw)}
