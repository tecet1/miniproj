"""독립 기대값 계산. 생성기·파이프라인 코드를 import하지 않고, 시나리오와 SKU만 읽어서 계산한다.

(생성기와 로직을 공유하면 같은 실수를 하고도 서로 일치해 버리기 때문이다.)
이 파일은 canonical.py, pricing.py 등을 import하면 안 된다.
"""


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
    raise NotImplementedError


def main() -> None:
    """compute_expected 결과를 01_contracts/data/fixtures/expected/expected.json 에 쓴다."""
    raise NotImplementedError


if __name__ == "__main__":
    main()
