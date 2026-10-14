"""이벤트·주문·결제·환불 ID를 만든다. 규칙은 event_spec.md 1.1절 "ID 규칙"이다.

세 가지 방식이 같은 메서드를 가진다. canonical.build_events(..., ids=...)가 호출한다.
  ReadableIds   읽기 쉬운 규칙 ID (`A-S01-1-E1`). 고정 픽스처용이며 기본값이다.
  NamedIds      이름 기반 UUID(v5). 같은 (시드, 채널, 시나리오, 번호)면 항상 같은 ID라 덤프를 다시 만들어도 ID가 같다.
  RandomIds     무작위 UUID(v4). 시뮬레이터 서버용이며 호출할 때마다 다르다.
메서드: order_id(channel, scenario_id, order_no), event_id(order_id, no), payment_id(order_id, no),
        refund_id(order_id, no), order_item_id(order_id, seq)
order_item_id는 모든 방식에서 `{order_id}-{순번}`이다 (주문 안의 상품 행이라 주문 ID에서 파생한다).
UUID는 소문자 하이픈 36자 문자열이다. 파이프라인은 ID를 해석하지 않는다.
표준 라이브러리만 쓴다.
"""
import uuid

# NamedIds가 쓰는 고정 이름공간. 값을 바꾸면 모든 ID가 바뀐다.
NAMESPACE = uuid.UUID("6f1c2b0e-3d4a-4c5b-8e7f-0a1b2c3d4e5f")


class ReadableIds:
    """고정 픽스처의 규칙 (scenarios.md): `{채널}-{시나리오}-{번호}`에 접미사를 붙인다."""

    def order_id(self, channel: str, scenario_id: str, order_no: int) -> str:
        return f"{channel}-{scenario_id}-{order_no}"

    def event_id(self, order_id: str, no: int) -> str:
        return f"{order_id}-E{no}"

    def payment_id(self, order_id: str, no: int) -> str:
        return f"{order_id}-P{no}"

    def refund_id(self, order_id: str, no: int) -> str:
        return f"{order_id}-R{no}"

    def order_item_id(self, order_id: str, seq: int) -> str:
        return f"{order_id}-{seq}"


class NamedIds(ReadableIds):
    """이름 기반 UUID(v5). 시드가 다르면 ID 전체가 달라지고, 만드는 순서와 무관하다."""

    def __init__(self, seed):
        self.seed = str(seed)

    def _uuid(self, *parts) -> str:
        return str(uuid.uuid5(NAMESPACE, "|".join([self.seed, *map(str, parts)])))

    def order_id(self, channel: str, scenario_id: str, order_no: int) -> str:
        return self._uuid("order", channel, scenario_id, order_no)

    def event_id(self, order_id: str, no: int) -> str:
        return self._uuid("event", order_id, no)

    def payment_id(self, order_id: str, no: int) -> str:
        return self._uuid("payment", order_id, no)

    def refund_id(self, order_id: str, no: int) -> str:
        return self._uuid("refund", order_id, no)


class RandomIds(ReadableIds):
    """무작위 UUID(v4). 호출할 때마다 새 값이라 같은 입력도 다른 ID를 받는다 (운영 서버용)."""

    def order_id(self, channel: str, scenario_id: str, order_no: int) -> str:
        return str(uuid.uuid4())

    def event_id(self, order_id: str, no: int) -> str:
        return str(uuid.uuid4())

    def payment_id(self, order_id: str, no: int) -> str:
        return str(uuid.uuid4())

    def refund_id(self, order_id: str, no: int) -> str:
        return str(uuid.uuid4())
