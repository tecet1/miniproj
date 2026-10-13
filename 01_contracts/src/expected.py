"""독립 기대값 계산. 생성기·파이프라인 코드를 import하지 않고, 시나리오와 SKU만 읽어서 계산한다.

(생성기와 로직을 공유하면 같은 실수를 하고도 서로 일치해 버리기 때문이다.)
이 파일은 canonical.py, pricing.py 등을 import하면 안 된다.
"""
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CHANNELS = ("A", "B", "C")
COUNT_KEYS = ("bronze_rows", "silver_orders", "silver_order_items", "silver_payments",
              "silver_order_cancellations", "silver_refunds", "quarantine", "fact_sales_item",
              "fact_refund", "total_sales", "refund_amount", "paid_item_count", "paying_customers")


def compute_expected(scenarios: list[dict], skus: list[dict]) -> dict:
    """{"per_channel": {"A": {...}, "B": {...}, "C": {...}}, "total": {...}} 를 돌려준다.

    scenarios, skus 는 JSON을 읽은 그대로의 리스트다 (scenario_loader를 쓰지 않는다).
    각 항목의 키:
      bronze_rows, silver_orders, silver_order_items, silver_payments,
      silver_order_cancellations, silver_refunds, quarantine, fact_sales_item, fact_refund,
      total_sales(총 판매금액 = 결제 성공 상품의 할인 후 금액 합), refund_amount, net_sales,
      paid_item_count, paying_customers(결제 성공 주문의 user_id distinct),
      refund_ratio(환불금액/총 판매금액), unit_price_per_item(총 판매금액/상품결제건수),
      average_order_value(객단가 = 총 판매금액/결제고객수)
    계산 규칙
      - fault가 있는 이벤트는 격리(quarantine)로 세고 Silver·Gold에는 반영하지 않는다.
      - 중복 전송(send_count)은 bronze_rows에만 모두 반영하고 Silver부터는 1건이다.
      - fact_sales_item 은 결제 성공이 있는 주문의 상품 행 수다.
    total 은 세 채널 합이다. 단 paying_customers 는 채널+user_id 로 구분하므로 채널별 값의 합이다.
    """
    prices = {sku["product_id"]: sku["unit_price"] for sku in skus}
    # 시나리오는 세 채널에 같은 내용이므로 한 채널분을 계산하면 된다.
    one_channel = _count_one_channel(scenarios, prices)
    per_channel = {channel: _add_metrics(dict(one_channel)) for channel in CHANNELS}
    total = _add_metrics({key: sum(per_channel[c][key] for c in CHANNELS) for key in COUNT_KEYS})
    return {"per_channel": per_channel, "total": total}


def _count_one_channel(scenarios: list[dict], prices: dict[str, int]) -> dict:
    counts = dict.fromkeys(COUNT_KEYS, 0)
    customers = set()
    for scenario in scenarios:
        for order in scenario["orders"]:
            paid = [prices[i["sku"]] * i["quantity"] - i["discount"] for i in order["items"]]
            has_successful_payment = False
            for event in order["events"]:
                counts["bronze_rows"] += event.get("send_count", 1)
                if event.get("fault"):
                    counts["quarantine"] += 1
                    continue
                kind = event["type"]
                if kind == "ORDER_CREATED":
                    counts["silver_orders"] += 1
                    counts["silver_order_items"] += len(order["items"])
                elif kind == "ORDER_CANCELLED":
                    counts["silver_order_cancellations"] += 1
                elif kind == "REFUND_SUCCEEDED":
                    index = event["item"] - 1
                    counts["silver_refunds"] += 1
                    counts["fact_refund"] += 1
                    counts["refund_amount"] += paid[index] * event["quantity"] // order["items"][index]["quantity"]
                else:  # 결제 성공 또는 실패
                    counts["silver_payments"] += 1
                    has_successful_payment |= kind == "PAYMENT_SUCCEEDED"
            if has_successful_payment:
                counts["fact_sales_item"] += len(order["items"])
                counts["paid_item_count"] += len(order["items"])
                counts["total_sales"] += sum(paid)
                customers.add(order["user_id"])
    counts["paying_customers"] = len(customers)
    return counts


def _add_metrics(counts: dict) -> dict:
    counts["net_sales"] = counts["total_sales"] - counts["refund_amount"]
    counts["refund_ratio"] = counts["refund_amount"] / counts["total_sales"]
    counts["unit_price_per_item"] = counts["total_sales"] / counts["paid_item_count"]
    counts["average_order_value"] = counts["total_sales"] / counts["paying_customers"]
    return counts


def main() -> None:
    """compute_expected 결과를 01_contracts/data_fixtures/expected/expected.json 에 쓴다."""
    contracts = ROOT / "01_contracts" / "contracts"
    scenarios = json.loads((contracts / "scenarios.json").read_text(encoding="utf-8"))["scenarios"]
    skus = json.loads((contracts / "skus.json").read_text(encoding="utf-8"))["skus"]
    out = ROOT / "01_contracts" / "data_fixtures" / "expected" / "expected.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(compute_expected(scenarios, skus), ensure_ascii=False, indent=2)
    out.write_text(text + "\n", encoding="utf-8", newline="\n")
    print(f"wrote {out}")


if __name__ == "__main__":
    main()
