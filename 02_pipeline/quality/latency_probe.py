"""지연 관찰: 이미 Event Hubs로 보낸 테스트 주문이 Bronze, Silver, Gold에 처음 보이는 시각을 잰다 (계획 07 카드 7-G).

관찰만 한다. 정량 목표는 없고 값은 트리거 간격(pipeline_config.json)을 조정할 때 참고한다.
[사용법] 테스트 주문(주문 생성 + 결제 성공 이벤트)을 보내는 즉시 run_latency.py에 그 주문 번호와 보낸 시각을 넣고 돈다.
[한계] 한 주문의 관찰값이다. 여러 번 반복하고 부하가 있을 때와 없을 때를 나눠 기록해야 대표값이 된다. polling 간격만큼 오차가 있다.
"""
import time


def probe_sql(layer: str, order_id: str) -> str:
    """계층별로 그 주문이 보이면 1 이상을 돌려주는 SQL. order_id는 영숫자·밑줄·하이픈만 허용한다 (SQL에 그대로 들어간다)."""
    if not order_id or not all(ch.isalnum() or ch in "_-" for ch in order_id):
        raise ValueError(f"주문 번호 형식이 잘못되었다: {order_id!r}")
    return {
        "bronze": f"SELECT COUNT(*) AS n FROM commerce_poc.bronze.events WHERE instr(payload, '{order_id}') > 0",
        "silver": f"SELECT COUNT(*) AS n FROM commerce_poc.silver.orders WHERE order_id = '{order_id}'",
        "gold": f"SELECT COUNT(*) AS n FROM commerce_poc.gold.fact_sales_item WHERE order_id = '{order_id}'",
    }[layer]


def watch(count_of, order_id: str, timeout_seconds: float = 600, interval_seconds: float = 2,
          layers=("bronze", "silver", "gold"), clock=time.monotonic, sleep=time.sleep) -> dict:
    """계층마다 처음 보인 시각(시작 후 경과 초)을 돌려준다. 시간 안에 못 보면 None.

    count_of(sql) -> 건수. clock·sleep은 테스트에서 가짜로 바꾼다."""
    start = clock()
    seen = {layer: None for layer in layers}
    while clock() - start <= timeout_seconds:
        for layer in layers:
            if seen[layer] is None and count_of(probe_sql(layer, order_id)) > 0:
                seen[layer] = round(clock() - start, 1)
        if all(value is not None for value in seen.values()):
            break
        sleep(interval_seconds)
    return seen


def summarize(seen: dict) -> dict:
    """계층 사이 구간 지연. 앞 계층이 안 보였으면 그 뒤 구간은 None."""
    order = ["bronze", "silver", "gold"]
    out = {"처음 보인 시각(초)": seen}
    previous = 0.0
    for layer in order:
        value = seen.get(layer)
        out[f"{layer} 구간(초)"] = None if value is None or previous is None else round(value - previous, 1)
        previous = value
    return out
