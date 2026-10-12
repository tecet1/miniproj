"""scenarios.json, skus.json 읽기. 파일을 읽는 일만 한다(변환은 하지 않음)."""
import json


def load_scenarios(path) -> list[dict]:
    """scenarios.json의 "scenarios" 배열을 파일에 적힌 순서 그대로 돌려준다. (UTF-8)"""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)["scenarios"]

def load_skus(path) -> dict[str, dict]:
    """skus.json을 {product_id: sku 딕셔너리} 형태로 돌려준다. 상품 조회가 빠르도록 키로 둔다."""
    with open(path, "r", encoding="utf-8") as f:
        skus = json.load(f)["skus"]
        return {s["product_id"]: s for s in skus}