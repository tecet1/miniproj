"""수신 처리 로직. Azure에 의존하지 않는 순수 함수라서 로컬(그리고 CI)에서 테스트할 수 있다.

요청 한 건을 받아 (1) 채널을 확인하고 (2) 수신 시각을 붙여 봉투(envelope)로 감싸고 (3) Event Hubs로 보낸다.
payload는 해석하지 않는다. 깨진 JSON도 그대로 Bronze까지 보내고, 검증과 격리는 Silver가 한다.

봉투(Event Hubs 메시지 본문): {"channel": "A", "received_at": "2026-10-12T01:00:00.000Z", "payload": "<본문 문자열 그대로>"}
"""
import json

CHANNELS = ("A", "B", "C")


def handle_event(channel: str, body: bytes, send, received_at: str) -> tuple[int, str]:
    """요청 한 건을 처리하고 (HTTP 상태 코드, 응답 메시지)를 돌려준다.

    channel      URL 경로의 채널 값 (정확히 "A", "B", "C"만 허용, 소문자는 허용하지 않는다)
    body         요청 본문(bytes). UTF-8로 디코딩한 문자열이 payload가 된다
    send         봉투 JSON 문자열 하나를 받아 Event Hubs로 보내는 함수. 실패하면 예외를 낸다
    received_at  수신 시각 문자열 (호출하는 쪽이 만들어 넘긴다. 테스트에서 값을 고정하기 위함)

    규칙
      - 채널이 CHANNELS에 없으면 (400, ...) 이고 send를 호출하지 않는다
      - 본문이 UTF-8이 아니면 (400, ...) 이고 send를 호출하지 않는다
      - 성공하면 send를 정확히 한 번 호출하고 (202, ...) 를 돌려준다
      - send가 예외를 내면 (503, ...) 를 돌려준다 (송신기가 재시도하도록 5xx)
      - 봉투 직렬화: json.dumps(..., ensure_ascii=False, separators=(",", ":"))
        payload 문자열은 한 글자도 바꾸지 않는다 (공백, 한글, 이스케이프 그대로)
    """
    raise NotImplementedError
