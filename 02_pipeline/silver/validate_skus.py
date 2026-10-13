"""참조 데이터(skus.json)에 없는 SKU를 검증한다. 어긋나면 ConversionError("INVALID_VALUE", 상세)를 낸다.

[기준] sku_spec.md '참조 데이터에 없는 SKU', event_spec.md 4절 INVALID_VALUE (2026-10-13 승인)
[입력] 채널 변환이 끝난 공통 이벤트 dict, 기준 SKU 집합 known_skus(set of product_id)
[규칙] ORDER_CREATED의 items 각 행의 product_id가 known_skus에 없으면 그 이벤트 전체를 INVALID_VALUE로 격리한다.
       다른 이벤트 유형은 검사하지 않는다 (items가 없다).
기준 SKU 집합은 한 번 읽어 두고 행마다 다시 읽지 않는다 (Spark UDF가 집합을 함께 가져간다).
"""
import json
from pathlib import Path

from conversion_error import ConversionError

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SKUS = ROOT / "01_contracts" / "contracts" / "skus.json"


def load_known_skus(path=DEFAULT_SKUS) -> set:
    """skus.json을 읽어 product_id 집합을 돌려준다.

    힌트: json.load(f)["skus"]가 {"product_id", ...} 객체의 목록이다. UTF-8로 연다.
    """
    with open(path, "r", encoding="utf-8") as f:
        skus = json.load(f)["skus"]
    return {sku["product_id"] for sku in skus}

def validate_known_skus(event: dict, known_skus: set) -> None:
    """위 규칙대로 검사한다. 통과하면 아무것도 돌려주지 않는다.

    힌트: event["event_type"]이 "ORDER_CREATED"가 아니면 바로 끝낸다. 오류 상세에는 모르는 product_id를 적는다.
    """
    if event["event_type"] != "ORDER_CREATED":
        return

    for item in event["items"]:
        if item["product_id"] not in known_skus:
            raise ConversionError("INVALID_VALUE", f"Unknown product_id: {item['product_id']!r}")