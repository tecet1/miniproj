"""1년치 합성 시나리오를 만든다. scenarios.json과 같은 형식의 dict 목록을 돌려준다 (파일은 쓰지 않는다).

[입력] skus: {product_id: sku}(scenario_loader.load_skus), seed, 시작일, 종료일, 하루 기준 주문 수
[출력] 하루에 시나리오 하나(id "L{YYYYMMDD}"), 주문 번호는 하루 안에서 1부터. 정해진 시드면 항상 같은 결과다.
[설계]
  - 하루마다 독립된 난수(random.Random(f"{seed}-{날짜}"))를 쓴다. 기간을 바꿔도 같은 날의 시나리오는 그대로다.
  - 주문 수는 요일·월 계수와 잡음으로 변한다. 시간대는 저녁이 많다. 자정을 넘는 결제와 며칠 뒤 환불(결제일 != 환불일)이 생긴다.
  - 주문 유형: 결제 성공, 실패 후 재시도 성공, 결제 실패로 끝, 결제 전 취소, 결제 성공 후 환불.
  - 결제수단·사유는 채널 변환기가 매핑하는 값만 쓴다 (OTHER는 변환기가 만들 수 없다). 오류 주입(fault)은 하지 않는다 (픽스처가 이미 다룬다).
  - 이벤트 일부는 중복 전송(send_count 2)한다. Silver 중복 제거를 대량으로 확인하기 위해서다.
  - 환불 시각이 종료일 끝을 넘으면 그 환불은 만들지 않는다 (아직 일어나지 않은 일이므로).
expected.py와 canonical.py를 import하지 않는다 (기대값 독립성).
"""
import random
from datetime import date, datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
PAYMENT_METHODS = (("CARD", 55), ("EASY_PAY", 25), ("BANK_TRANSFER", 12), ("VIRTUAL_ACCOUNT", 8))
REASONS = (("CHANGE_OF_MIND", 40), ("DEFECT", 25), ("WRONG_ITEM", 15), ("DELAY", 20))
FAIL_REASONS = (("LIMIT_EXCEEDED", 40), ("CARD_DECLINED", 40), ("TIMEOUT", 20))
ORDER_KINDS = (("PAID", 70), ("RETRY_PAID", 8), ("FAILED_ONLY", 3), ("CANCELLED", 5), ("REFUNDED", 14))
HOUR_WEIGHTS = (1, 1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 6, 7, 6, 6, 6, 6, 7, 8, 9, 11, 12, 10, 5)  # 0~23시
WEEKDAY_FACTOR = (1.0, 1.0, 1.0, 1.0, 1.15, 1.35, 1.2)   # 월~일
MONTH_FACTOR = {11: 1.4, 12: 1.3, 1: 0.9, 2: 0.9}        # 그 외 달은 1.0
USER_POOL = 500
DISCOUNT_RATE = 0.15
DUPLICATE_RATE = 0.01
REFUND_MIN_DELAY_SEC = 3600
REFUND_MAX_DELAY_SEC = 10 * 86400
CANCEL_WITHIN_SEC = 24 * 3600        # 이 안의 환불은 발송 전 취소(CANCEL), 넘으면 반품(RETURN)


def pick(rng: random.Random, weighted: tuple):
    """(값, 가중치) 목록에서 가중치대로 하나 고른다."""
    return rng.choices([value for value, _ in weighted], weights=[w for _, w in weighted])[0]


def orders_in_day(rng: random.Random, day: date, base: int) -> int:
    """요일·월 계수와 ±20% 잡음을 곱한 그날의 주문 수 (최소 1)."""
    factor = WEEKDAY_FACTOR[day.weekday()] * MONTH_FACTOR.get(day.month, 1.0) * rng.uniform(0.8, 1.2)
    return max(1, round(base * factor))


def start_second(rng: random.Random, latest: int) -> int:
    """하루 안의 주문 시작 초(KST 00:00 기준). 시간대 가중치로 시를 고른다. latest 이하로 자른다."""
    hour = rng.choices(range(24), weights=HOUR_WEIGHTS)[0]
    return min(hour * 3600 + rng.randrange(3600), latest)


def make_items(rng: random.Random, skus: dict) -> list[dict]:
    """주문 상품 1~3행. 일부 행은 단가x수량의 20% 이하, 500원 단위 할인을 받는다."""
    lines = []
    for sku_id in rng.sample(sorted(skus), rng.choice((1, 1, 1, 2, 2, 3))):
        quantity = rng.choice((1, 1, 1, 2, 2, 3))
        max_discount = skus[sku_id]["unit_price"] * quantity // 5 // 500 * 500
        discount = rng.randrange(500, max_discount + 1, 500) if max_discount >= 500 and rng.random() < DISCOUNT_RATE else 0
        lines.append({"sku": sku_id, "quantity": quantity, "discount": discount})
    return lines


def make_refunds(rng: random.Random, items: list[dict], paid_offset: int, room_sec: int) -> list[dict]:
    """환불 이벤트 1~2건 (상품 행마다 최대 1건). room_sec 안에 못 들어가면 빈 목록이다.

    room_sec: 주문 시작부터 종료 시점까지 남은 초. 환불은 결제 1시간~10일 뒤이며 이 안에 있어야 한다.
    """
    refunds = []
    lines = rng.sample(range(len(items)), min(len(items), 2 if rng.random() < 0.2 else 1))
    for index in lines:
        offset = paid_offset + rng.randint(REFUND_MIN_DELAY_SEC, REFUND_MAX_DELAY_SEC)
        if offset > room_sec:
            continue
        refunds.append({"type": "REFUND_SUCCEEDED", "offset_sec": offset, "item": index + 1,
                        "quantity": rng.randint(1, items[index]["quantity"]),
                        "refund_type": "CANCEL" if offset - paid_offset < CANCEL_WITHIN_SEC else "RETURN",
                        "reason": pick(rng, REASONS)})
    return sorted(refunds, key=lambda r: r["offset_sec"])


def make_events(rng: random.Random, kind: str, items: list[dict], room_sec: int) -> list[dict]:
    """주문 유형에 맞는 이벤트 목록. 시각은 주문 시작 기준 초(offset_sec)다."""
    events = [{"type": "ORDER_CREATED", "offset_sec": 0}]
    method = pick(rng, PAYMENT_METHODS)
    if kind == "CANCELLED":
        events.append({"type": "ORDER_CANCELLED", "offset_sec": rng.randint(30, 600), "reason": pick(rng, REASONS)})
        return events
    if kind in ("RETRY_PAID", "FAILED_ONLY"):
        events.append({"type": "PAYMENT_FAILED", "offset_sec": rng.randint(10, 40), "method": method,
                       "fail_reason": pick(rng, FAIL_REASONS)})
        if kind == "FAILED_ONLY":
            return events
    paid_offset = rng.randint(90, 200) if kind == "RETRY_PAID" else rng.randint(20, 60)
    events.append({"type": "PAYMENT_SUCCEEDED", "offset_sec": paid_offset, "method": method})
    if kind == "REFUNDED":
        events += make_refunds(rng, items, paid_offset, room_sec)
    return events


def mark_duplicates(rng: random.Random, events: list[dict]) -> None:
    """이벤트 일부를 두 번 보내도록 send_count를 2로 한다 (제자리 수정)."""
    for event in events:
        if rng.random() < DUPLICATE_RATE:
            event["send_count"] = 2


def make_order(rng: random.Random, order_no: int, skus: dict, scenario_start: datetime,
               day_offset: int, limit: datetime) -> dict:
    """주문 하나. day_offset은 시나리오 시작(그날 00:00 KST)부터 주문 시작까지의 초, limit은 데이터 끝 시각."""
    items = make_items(rng, skus)
    room_sec = int((limit - scenario_start).total_seconds()) - day_offset
    events = make_events(rng, pick(rng, ORDER_KINDS), items, room_sec)
    mark_duplicates(rng, events)
    user = f"U{int(rng.random() ** 2 * USER_POOL) + 1:04d}"
    return {"order_no": order_no, "user_id": user, "start_offset_sec": day_offset, "items": items, "events": events}


def make_day(skus: dict, seed: int, day: date, base: int, limit: datetime) -> dict:
    """하루치 시나리오. 그날의 난수는 (시드, 날짜)로만 정해진다."""
    rng = random.Random(f"{seed}-{day.isoformat()}")
    scenario_start = datetime(day.year, day.month, day.day, tzinfo=KST)
    # 데이터 끝 직전 주문은 10분 안에 결제가 끝나도록 마지막 시각을 제한한다
    latest = min(86399, int((limit - scenario_start).total_seconds()) - 600)
    orders = [make_order(rng, no, skus, scenario_start, start_second(rng, latest), limit)
              for no in range(1, orders_in_day(rng, day, base) + 1)]
    return {"id": f"L{day:%Y%m%d}", "title": f"장기 데이터 {day.isoformat()}", "start_kst": scenario_start.isoformat(),
            "orders": orders}


def build_scenarios(skus: dict, seed: int, start: date, end: date, base_orders_per_day: int) -> list[dict]:
    """start~end(양 끝 포함) 날짜마다 시나리오 하나씩, 날짜 순서로 돌려준다."""
    limit = datetime(end.year, end.month, end.day, tzinfo=KST) + timedelta(days=1)
    days = (end - start).days + 1
    return [make_day(skus, seed, start + timedelta(days=n), base_orders_per_day, limit) for n in range(days)]
