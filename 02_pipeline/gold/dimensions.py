"""Dimension 행을 만든다 (순수 Python). 기준은 event_spec.md 6.2절이다.

  channel_rows, payment_method_rows, refund_profile_rows   상수 (코드값은 event_spec.md 1.2·2.3절)
  product_rows(path)                                       skus.json (현재 값만, SCD 없음)
각 함수는 gold_tables.TABLES의 컬럼 순서와 같은 튜플 목록을 돌려준다.
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SKUS = ROOT / "01_contracts" / "contracts" / "skus.json"

CHANNELS = [("A", "자사몰"), ("B", "종합 오픈마켓"), ("C", "스포츠 전문몰")]
PAYMENT_METHODS = [("CARD", "카드"), ("EASY_PAY", "간편결제"), ("BANK_TRANSFER", "계좌이체"),
                   ("VIRTUAL_ACCOUNT", "가상계좌"), ("OTHER", "기타")]
REFUND_TYPES = ("CANCEL", "RETURN")
REFUND_REASONS = ("CHANGE_OF_MIND", "DEFECT", "WRONG_ITEM", "DELAY", "OTHER")


def channel_rows() -> list[tuple]:
    return list(CHANNELS)


def payment_method_rows() -> list[tuple]:
    return list(PAYMENT_METHODS)


def refund_profile_rows() -> list[tuple]:
    """(refund_profile_key, refund_type, refund_reason) 10행. 키는 "{유형}-{사유}"."""
    return [(f"{kind}-{reason}", kind, reason) for kind in REFUND_TYPES for reason in REFUND_REASONS]


def product_rows(path=DEFAULT_SKUS) -> list[tuple]:
    """skus.json의 (product_id, product_name, category)."""
    with open(path, "r", encoding="utf-8") as f:
        skus = json.load(f)["skus"]
    return [(sku["product_id"], sku["product_name"], sku["category"]) for sku in skus]
