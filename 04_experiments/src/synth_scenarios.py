"""1년치 합성 시나리오를 만든다. scenarios.json과 같은 형식의 dict 목록을 돌려준다 (파일은 쓰지 않는다).

[입력] skus: {product_id: sku}(scenario_loader.load_skus), config: configs/dump.json을 읽은 dict
[출력] 하루에 시나리오 하나(id "L{YYYYMMDD}"), 주문 번호는 그날 안에서 시각 순서로 1부터. 같은 시드·설정이면 항상 같은 결과다.
[모형] 근거와 파라미터는 04_experiments/docs/기업_프로필과_성장_모형.md
  - 월 수준: 시작 기준월을 1.0으로 두고 매달 exp(mu + e)를 곱한다. e ~ N(0, sigma^2), e는 (시드, 달)로 고정한다.
    shock_correlation(기본 0)만큼의 분산은 시장 전체 충격(shared_seed)이라 채널이 함께 움직이고, 나머지는 채널 고유 충격이다 (총 분산은 sigma^2).
  - 그날의 평균 주문 수 = D0 x 월 수준 x 월 계절 계수 x 요일 계수 x 판촉일 계수.
    요일 계수와 판촉일 계수는 평균이 1이 되도록 나눠서 D0와 월 수준이 규모를 정하게 한다.
  - 그날의 주문 수: 평균이 위 값인 음이항분포(감마-포아송). 분산이 평균보다 크다.
  - 주문 하나: 시각(저녁이 많다), 상품·수량·할인, 유형(결제 성공·재시도·실패·취소·환불), 결제수단, 환불 지연(1시간~10일).
[설계]
  - 같은 날의 시나리오는 (시드, 날짜, 월 경로)로만 정해진다. 월 경로는 시작 기준월부터 누적하므로, 종료일을 바꿔도 종료 11일 전까지의 날은 그대로다
    (그 뒤 날짜는 종료일을 넘는 환불이 빠져서 달라진다).
  - 결제수단·사유는 채널 변환기가 매핑하는 값만 쓴다 (OTHER는 적재 단계가 결측·미정의 값을 바꿔 만드는 값이라 이벤트에서 만들지 않는다).
  - 이벤트 일부는 중복 전송(send_count 2)한다.
  - 오류 주입(2026-10-15): 설정의 error_rates에 종류별 비율(주문 단위)을 주면 그 비율만큼 주문을 오류 주문으로 바꾼다. 비율이 없거나 모두 0이면 아무것도 바꾸지 않는다.
    종류는 ERROR_KINDS(격리 대상 오류, 이중 결제, 주문 생성 중복, 실패 결제를 가리키는 환불, 환불 order_id 불일치, items 상품 ID 중복)다.
    주입은 (시드, 날짜, 주문 번호)로 따로 정한 난수를 쓰므로 오류가 없는 주문은 주입 여부와 상관없이 그대로이고, 같은 설정이면 같은 주문이 같은 오류가 된다.
    종류가 그 주문에 맞지 않으면(환불이 없는 주문의 환불 오류 등) 바꾸지 않으므로 실제 비율은 설정보다 작을 수 있다. 오류가 된 주문에는 "error": 종류 키가 붙는다.
    오류 형식은 01_contracts의 시나리오 키(fault, payment: failed, order_no, reuse_item_id_of)와 같다.
  - 환불 시각이 종료일 끝을 넘으면 그 환불은 만들지 않는다 (아직 일어나지 않은 일이므로).
  - 포아송 표본은 평균 30 미만은 Knuth 방식, 이상은 정규 근사(반올림)다. 음이항의 과산포가 근사 오차보다 훨씬 크다.
expected.py와 canonical.py를 import하지 않는다 (기대값 독립성).
"""

import math
import random
from datetime import date, datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))
PAYMENT_METHODS = (("CARD", 55), ("EASY_PAY", 25), ("BANK_TRANSFER", 12), ("VIRTUAL_ACCOUNT", 8))
REASONS = (("CHANGE_OF_MIND", 40), ("DEFECT", 25), ("WRONG_ITEM", 15), ("DELAY", 20))
FAIL_REASONS = (("LIMIT_EXCEEDED", 40), ("CARD_DECLINED", 40), ("TIMEOUT", 20))
ORDER_KINDS = (("PAID", 70), ("RETRY_PAID", 8), ("FAILED_ONLY", 3), ("CANCELLED", 5), ("REFUNDED", 14))
LINE_COUNTS = ((1, 70), (2, 25), (3, 5))       # 주문 상품 행 수. 평균 주문 금액 8만 원 안팎이 되도록 정했다
QUANTITIES = ((1, 75), (2, 22), (3, 3))        # 행별 수량
DISCOUNT_RATE = 0.15
REFUND_MIN_DELAY_SEC = 3600
REFUND_MAX_DELAY_SEC = 10 * 86400
CANCEL_WITHIN_SEC = 24 * 3600        # 이 안의 환불은 발송 전 취소(CANCEL), 넘으면 반품(RETURN)
POISSON_NORMAL_FROM = 30             # 평균이 이 이상이면 정규 근사


def pick(rng: random.Random, weighted: tuple):
    """(값, 가중치) 목록에서 가중치대로 하나 고른다."""
    return rng.choices([value for value, _ in weighted], weights=[w for _, w in weighted])[0]


# ── 일별 주문 수 모형 ─────────────────────────────────────────────────────────────────
def anchor_of(config: dict) -> tuple[int, int]:
    """시작 기준월 (연, 월). 설정의 "YYYY-MM"."""
    year, month = config["anchor_month"].split("-")
    return int(year), int(month)


def month_index(day: date, config: dict) -> int:
    """기준월을 0으로 한 달 번호."""
    year, month = anchor_of(config)
    return (day.year - year) * 12 + day.month - month


def month_levels(config: dict, last_index: int) -> list[float]:
    """달 번호 0..last_index의 월 수준. 기준월 = 1.0, 이후 매달 exp(mu + e)를 곱한다.

    mu = ln(1 + 연 성장률) / 12. 충격 e는 (시드, 달 번호)로 고정되어 종료일과 무관하다.
    """
    mu = math.log(1 + config["growth_annual"]) / 12
    rho = config.get("shock_correlation", 0.0)
    market_seed = config.get("shared_seed", config["seed"])
    levels, log_level = [1.0], 0.0
    for index in range(1, last_index + 1):
        own = random.Random(f"{config['seed']}-shock-{index}").gauss(0, 1)
        market = random.Random(f"{market_seed}-market-shock-{index}").gauss(0, 1)
        shock = config["sigma_monthly"] * (math.sqrt(rho) * market + math.sqrt(1 - rho) * own)
        log_level += mu + shock
        levels.append(math.exp(log_level))
    return levels


def days_in_month(year: int, month: int) -> int:
    first = date(year, month, 1)
    return ((date(year + month // 12, month % 12 + 1, 1)) - first).days


def promo_days(config: dict, year: int, month: int) -> dict[int, float]:
    """그 달의 판촉일 {일: 배수}. 개수와 날짜와 배수는 (시드, 연, 월)로 고정된다."""
    rng = random.Random(f"{config['seed']}-promo-{year}-{month}")
    low, high = config["promo"]["days_per_month"]
    low_mult, high_mult = config["promo"]["multiplier"]
    chosen = rng.sample(range(1, days_in_month(year, month) + 1), rng.randint(low, high))
    return {day: rng.uniform(low_mult, high_mult) for day in chosen}


def promo_expectation(config: dict, year: int, month: int) -> float:
    """판촉일이 하루 평균 주문 수에 곱하는 기대 배수. 이것으로 나눠 판촉일이 월 규모를 바꾸지 않게 한다."""
    low, high = config["promo"]["days_per_month"]
    low_mult, high_mult = config["promo"]["multiplier"]
    share = (low + high) / 2 / days_in_month(year, month)
    return 1 + share * ((low_mult + high_mult) / 2 - 1)


def customer_pools(config: dict) -> list[int]:
    """기간의 날짜별 고객 풀 크기. 시작 고객 수 x 월 수준이라 주문량과 같은 경로로 늘어난다 (가입자가 늘어야 주문이 는다는 가정)."""
    start, end = date.fromisoformat(config["start"]), date.fromisoformat(config["end"])
    levels = month_levels(config, month_index(end, config))
    return [round(config["customers_start"] * levels[month_index(start + timedelta(days=offset), config)])
            for offset in range((end - start).days + 1)]


def daily_mean_orders(config: dict) -> list[tuple[date, float]]:
    """기간의 (날짜, 그날의 평균 주문 수). 난수는 월 충격과 판촉일에만 쓰인다 (표본 추출 전의 평균)."""
    start, end = date.fromisoformat(config["start"]), date.fromisoformat(config["end"])
    levels = month_levels(config, month_index(end, config))
    weekday = config["weekday_factor"]
    weekday_mean = sum(weekday) / len(weekday)
    promos, result = {}, []
    for offset in range((end - start).days + 1):
        day = start + timedelta(days=offset)
        key = (day.year, day.month)
        if key not in promos:
            promos[key] = promo_days(config, *key)
        mean = (config["base_orders_per_day"] * levels[month_index(day, config)]
                * config["month_factor"].get(str(day.month), 1.0)
                * weekday[day.weekday()] / weekday_mean
                * promos[key].get(day.day, 1.0) / promo_expectation(config, *key))
        result.append((day, mean))
    return result


def poisson(rng: random.Random, mean: float) -> int:
    """포아송 표본. 평균이 작으면 Knuth 방식, 크면 정규 근사(반올림, 0 이상)."""
    if mean >= POISSON_NORMAL_FROM:
        return max(0, round(rng.gauss(mean, math.sqrt(mean))))
    limit, count, product = math.exp(-mean), 0, rng.random()
    while product > limit:
        count += 1
        product *= rng.random()
    return count


def draw_count(rng: random.Random, mean: float, dispersion: float) -> int:
    """평균 mean, 분산 mean + mean^2 / dispersion 인 음이항 표본 (감마로 평균을 흔든 포아송)."""
    return poisson(rng, rng.gammavariate(dispersion, mean / dispersion))


# ── 주문 하나 ─────────────────────────────────────────────────────────────────────────
def start_second(rng: random.Random, latest: int, hour_weights: list) -> int:
    """하루 안의 주문 시작 초(KST 00:00 기준). 시간대 가중치(configs의 hour_weights, 0~23시)로 시를 고른다. latest 이하로 자른다."""
    hour = rng.choices(range(24), weights=hour_weights)[0]
    return min(hour * 3600 + rng.randrange(3600), latest)


def make_items(rng: random.Random, skus: dict) -> list[dict]:
    """주문 상품 1~3행. 일부 행은 단가x수량의 20% 이하, 500원 단위 할인을 받는다."""
    lines = []
    for sku_id in rng.sample(sorted(skus), pick(rng, LINE_COUNTS)):
        quantity = pick(rng, QUANTITIES)
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


def mark_duplicates(rng: random.Random, events: list[dict], rate: float) -> None:
    """이벤트 일부를 두 번 보내도록 send_count를 2로 한다 (제자리 수정)."""
    for event in events:
        if rng.random() < rate:
            event["send_count"] = 2


# ── 오류 주입 ─────────────────────────────────────────────────────────────────────
ERROR_KINDS = ("fault", "double_payment", "duplicate_order_created", "refund_to_failed_payment", "refund_order_mismatch",
               "duplicate_item_id")
FAULTS = ("MISSING_REQUIRED", "INVALID_TYPE", "UNKNOWN_EVENT_TYPE")   # 채널 원본에 심는 오류. 적용 가능한 이벤트가 다르다 (faults.py)


def error_rates(config: dict) -> dict:
    """설정의 error_rates 중 0이 아닌 것. 모르는 종류, 0~1 밖의 값, 합이 1 초과면 ValueError."""
    rates = config.get("error_rates") or {}
    unknown = set(rates) - set(ERROR_KINDS)
    if unknown:
        raise ValueError(f"error_rates의 알 수 없는 종류: {sorted(unknown)}")
    for kind, rate in rates.items():
        if isinstance(rate, bool) or not isinstance(rate, (int, float)) or not 0 <= rate <= 1:
            raise ValueError(f"error_rates.{kind}는 0~1 사이여야 한다: {rate!r}")
    if sum(rates.values()) > 1:
        raise ValueError("error_rates의 합이 1을 넘는다")
    return {kind: rate for kind, rate in rates.items() if rate > 0}


def _copy_event(event: dict, offset_sec: int) -> dict:
    """event의 복사본. 시각만 바꾸고 중복 전송·오류 표시는 가져오지 않는다."""
    copy = {key: value for key, value in event.items() if key not in ("send_count", "fault")}
    copy["offset_sec"] = offset_sec
    return copy


def _inject_fault(rng: random.Random, order: dict) -> bool:
    """이벤트 하나에 채널 원본 오류를 심는다. 누락은 결제 이벤트에만, 형식 오류는 주문 생성에만 맞다."""
    events = order["events"]
    candidates = [("UNKNOWN_EVENT_TYPE", e) for e in events]
    candidates += [("MISSING_REQUIRED", e) for e in events if e["type"].startswith("PAYMENT_")]
    candidates += [("INVALID_TYPE", e) for e in events if e["type"] == "ORDER_CREATED"]
    fault, event = rng.choice(candidates)
    event["fault"] = fault
    return True


def _inject_double_payment(rng: random.Random, order: dict) -> bool:
    events = order["events"]
    paid = next((e for e in events if e["type"] == "PAYMENT_SUCCEEDED"), None)
    if paid is None:
        return False
    events.insert(events.index(paid) + 1, _copy_event(paid, paid["offset_sec"] + rng.randint(5, 60)))
    return True


def _inject_duplicate_order_created(rng: random.Random, order: dict) -> bool:
    created = order["events"][0]      # 주문 생성은 항상 첫 이벤트다 (make_events)
    order["events"].insert(1, _copy_event(created, rng.randint(1, 10)))
    return True


def _inject_refund_to_failed_payment(rng: random.Random, order: dict) -> bool:
    """환불이 직전 결제 실패를 가리키게 한다. 결제 실패가 없던 주문에는 성공 결제 앞에 실패 한 건을 넣는다."""
    events = order["events"]
    refund = next((e for e in events if e["type"] == "REFUND_SUCCEEDED"), None)
    if refund is None:
        return False
    if not any(e["type"] == "PAYMENT_FAILED" for e in events):
        paid = next(e for e in events if e["type"] == "PAYMENT_SUCCEEDED")
        events.insert(events.index(paid), {"type": "PAYMENT_FAILED", "offset_sec": max(1, paid["offset_sec"] - 10),
                                           "method": paid["method"], "fail_reason": pick(rng, FAIL_REASONS)})
    refund["payment"] = "failed"
    return True


def _inject_refund_order_mismatch(rng: random.Random, order: dict, order_nos: list) -> bool:
    refund = next((e for e in order["events"] if e["type"] == "REFUND_SUCCEEDED"), None)
    others = [no for no in order_nos if no != order["order_no"]]
    if refund is None or not others:
        return False
    refund["order_no"] = rng.choice(others)
    return True


def _inject_duplicate_item_id(rng: random.Random, order: dict) -> bool:
    if len(order["items"]) < 2:
        return False
    row = rng.randint(2, len(order["items"]))
    order["items"][row - 1]["reuse_item_id_of"] = rng.randint(1, row - 1)
    return True


def inject_errors(config: dict, day: date, orders: list[dict]) -> None:
    """하루치 주문 일부를 오류 주문으로 바꾼다 (제자리 수정). 오류가 된 주문에는 "error": 종류 가 붙는다.

    주문마다 (시드, 날짜, 주문 번호)로 정한 난수 하나로 종류를 고르고(비율만큼) 세부를 정한다. 다른 주문의 난수와 섞이지 않는다.
    """
    rates = error_rates(config)
    if not rates:
        return
    order_nos = [order["order_no"] for order in orders]
    for order in orders:
        rng = random.Random(f"{config['seed']}-{day.isoformat()}-{order['order_no']}-error")
        roll, kind = rng.random(), None
        for name, rate in rates.items():
            if roll < rate:
                kind = name
                break
            roll -= rate
        if kind is None:
            continue
        injected = {"fault": lambda: _inject_fault(rng, order),
                    "double_payment": lambda: _inject_double_payment(rng, order),
                    "duplicate_order_created": lambda: _inject_duplicate_order_created(rng, order),
                    "refund_to_failed_payment": lambda: _inject_refund_to_failed_payment(rng, order),
                    "refund_order_mismatch": lambda: _inject_refund_order_mismatch(rng, order, order_nos),
                    "duplicate_item_id": lambda: _inject_duplicate_item_id(rng, order)}[kind]()
        if injected:
            order["error"] = kind


def error_summary(scenarios: list[dict]) -> dict:
    """{오류 종류: 주문 수}. 오류 주문이 없으면 빈 dict."""
    counts = {}
    for scenario in scenarios:
        for order in scenario["orders"]:
            if "error" in order:
                counts[order["error"]] = counts.get(order["error"], 0) + 1
    return counts


def refund_factor(config: dict, day: date) -> float:
    """그 달의 환불 주문 비중 배수. exp(N(0, refund_sigma)) 이며 (시드, 연, 월)로 고정된다 (달마다 환불율이 흔들린다)."""
    shock = random.Random(f"{config['seed']}-refund-{day.year}-{day.month}").gauss(0, config["refund_sigma"])
    return math.exp(shock)


def order_kinds(config: dict, day: date) -> tuple:
    """ORDER_KINDS에서 환불 가중치만 그 달의 배수로 바꾼 목록."""
    factor = refund_factor(config, day)
    return tuple((kind, weight * factor if kind == "REFUNDED" else weight) for kind, weight in ORDER_KINDS)


def make_order(rng: random.Random, order_no: int, skus: dict, config: dict, kinds: tuple, customers: int,
               scenario_start: datetime, day_offset: int, limit: datetime) -> dict:
    """주문 하나. day_offset은 시나리오 시작(그날 00:00 KST)부터 주문 시작까지의 초, limit은 데이터 끝 시각.

    customers는 그날의 고객 풀 크기다. 번호가 작을수록 오래된 단골이라 자주 산다 (제곱 쏠림).
    """
    items = make_items(rng, skus)
    room_sec = int((limit - scenario_start).total_seconds()) - day_offset
    events = make_events(rng, pick(rng, kinds), items, room_sec)
    mark_duplicates(rng, events, config["duplicate_rate"])
    user = f"U{int(rng.random() ** 2 * customers) + 1:06d}"
    return {"order_no": order_no, "user_id": user, "start_offset_sec": day_offset, "items": items, "events": events}


def make_day(skus: dict, config: dict, day: date, mean: float, customers: int, limit: datetime) -> dict:
    """하루치 시나리오. 그날의 난수는 (시드, 날짜)로 정해지고 평균 주문 수만 모형에서 받는다."""
    rng = random.Random(f"{config['seed']}-{day.isoformat()}")
    scenario_start = datetime(day.year, day.month, day.day, tzinfo=KST)
    # 데이터 끝 직전 주문은 10분 안에 결제가 끝나도록 마지막 시각을 제한한다
    latest = min(86399, int((limit - scenario_start).total_seconds()) - 600)
    count = draw_count(rng, mean, config["nb_dispersion"])
    starts = sorted(start_second(rng, latest, config["hour_weights"]) for _ in range(count))
    kinds = order_kinds(config, day)
    orders = [make_order(rng, no, skus, config, kinds, customers, scenario_start, offset, limit) for no, offset in enumerate(starts, 1)]
    inject_errors(config, day, orders)
    return {"id": f"L{day:%Y%m%d}", "title": f"장기 데이터 {day.isoformat()}", "start_kst": scenario_start.isoformat(),
            "orders": orders}


def channel_config(config: dict, channel: str) -> dict:
    """한 채널의 단일 흐름 설정. 최상위 base_orders_per_day, customers_start는 '채널 평균'이고 채널 값은 비중(share)을 곱해 만든다.

    비중의 합이 1이면 세 채널 합은 평균 x 3이다. 성장률은 채널 값을 쓰고, 시드에 채널을 붙여 채널마다 독립된 월 충격·판촉일·주문을 얻는다.
    """
    channels = config["channels"]
    spec, scale = channels[channel], len(channels) * channels[channel]["share"]
    return dict(config, seed=f"{config['seed']}-{channel}", shared_seed=config["seed"], growth_annual=spec["growth_annual"],
                base_orders_per_day=config["base_orders_per_day"] * scale,
                customers_start=round(config["customers_start"] * scale))


def build_channel_scenarios(skus: dict, config: dict) -> dict[str, list[dict]]:
    """{채널: 365일 시나리오 목록}. 채널마다 규모와 성장이 다르다 (config의 channels)."""
    return {channel: build_scenarios(skus, channel_config(config, channel)) for channel in config["channels"]}


def build_scenarios(skus: dict, config: dict) -> list[dict]:
    """config의 start~end(양 끝 포함) 날짜마다 시나리오 하나씩, 날짜 순서로 돌려준다."""
    end = date.fromisoformat(config["end"])
    limit = datetime(end.year, end.month, end.day, tzinfo=KST) + timedelta(days=1)
    return [make_day(skus, config, day, mean, customers, limit)
            for (day, mean), customers in zip(daily_mean_orders(config), customer_pools(config))]
