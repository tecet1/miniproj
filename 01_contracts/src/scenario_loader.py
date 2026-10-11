"""scenarios.json, skus.json 읽기와 구조 검증. 파일을 읽는 일만 한다(변환은 하지 않음)."""

EVENT_TYPES = {"ORDER_CREATED", "ORDER_CANCELLED", "PAYMENT_SUCCEEDED", "PAYMENT_FAILED", "REFUND_SUCCEEDED"}


def load_scenarios(path) -> list[dict]:
    """scenarios.json의 "scenarios" 배열을 파일에 적힌 순서 그대로 돌려준다. (UTF-8)"""
    raise NotImplementedError


def load_skus(path) -> dict[str, dict]:
    """skus.json을 {product_id: sku 딕셔너리} 형태로 돌려준다. 상품 조회가 빠르도록 키로 둔다."""
    raise NotImplementedError


def validate_scenarios(scenarios: list[dict], skus: dict[str, dict]) -> None:
    """잘못된 입력을 생성 전에 막는다. 문제가 있으면 ValueError.

    검사 항목: 주문 상품의 sku가 skus에 있는가, 이벤트 type이 EVENT_TYPES에 있는가.
    """
    raise NotImplementedError
