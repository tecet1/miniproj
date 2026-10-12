r"""fixtures의 events.jsonl을 읽어 Function에 HTTP POST로 보낸다. 표준 라이브러리만 쓴다.

events.jsonl 한 줄 = {"channel": "A", "payload": "<원본 본문 문자열>"}  ->  POST /api/events/A (본문 = payload)

실행 (프로젝트 루트, PowerShell)
  $env:FUNCTION_KEY = "<호스트 키>"        # 키를 코드나 커밋에 넣지 않는다
  python .\02_pipeline\ingestion\send_events.py --base-url https://<함수앱 호스트 이름>
"""
import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

CHANNELS = ("A", "B", "C")
ROOT = Path(__file__).resolve().parents[2]
DEFAULT_FIXTURES = ROOT / "01_contracts" / "data" / "fixtures"


class SendError(Exception):
    """재시도해도 해결되지 않거나, 재시도 횟수를 모두 쓴 경우."""


def read_events(path) -> list[tuple[str, str]]:
    """events.jsonl을 읽어 (channel, payload) 목록을 파일 순서 그대로 돌려준다."""
    events = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            if line.strip():
                outer = json.loads(line)
                events.append((outer["channel"], outer["payload"]))
    return events


def http_post(url: str, key: str, body: str) -> int:
    """본문을 POST하고 HTTP 상태 코드를 돌려준다. 오류 응답(4xx, 5xx)도 예외 대신 상태 코드로 돌려준다."""
    request = urllib.request.Request(
        url, data=body.encode("utf-8"), method="POST",
        headers={"x-functions-key": key, "Content-Type": "application/json; charset=utf-8"})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def send_event(base_url: str, key: str, channel: str, payload: str,
               post=http_post, retries: int = 3, sleep=time.sleep) -> None:
    """이벤트 한 건을 보낸다. 202면 성공이다.

    - 5xx 또는 연결 오류: 재시도 (1초, 2초, 4초 ... 간격). 모두 실패하면 SendError
    - 4xx: 요청 자체가 잘못된 것이므로 재시도하지 않고 바로 SendError
    같은 이벤트가 두 번 도착해도 Silver에서 channel + event_id로 중복을 제거한다.
    """
    url = f"{base_url.rstrip('/')}/api/events/{channel}"
    status = 0
    for attempt in range(1, retries + 1):
        try:
            status = post(url, key, payload)
        except OSError:                      # 연결 오류, 시간 초과 (urllib.error.URLError 포함)
            status = 0
        if status == 202:
            return
        if 400 <= status < 500:
            raise SendError(f"{channel} rejected with {status}: not retried")
        if attempt < retries:
            sleep(2 ** (attempt - 1))
    raise SendError(f"{channel} failed after {retries} attempts (last status {status})")


def send_all(events: list[tuple[str, str]], base_url: str, key: str,
             post=http_post, retries: int = 3, sleep=time.sleep) -> dict[str, int]:
    """모든 이벤트를 순서대로 보내고 채널별 전송 건수를 돌려준다."""
    sent = dict.fromkeys(CHANNELS, 0)
    for channel, payload in events:
        send_event(base_url, key, channel, payload, post=post, retries=retries, sleep=sleep)
        sent[channel] += 1
    return sent


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="fixtures의 이벤트를 Function으로 전송")
    parser.add_argument("--base-url", default=os.environ.get("FUNCTION_BASE_URL"),
                        help="Function App 주소 (예: https://<호스트 이름>). 환경 변수 FUNCTION_BASE_URL로도 지정")
    parser.add_argument("--fixtures", type=Path, default=DEFAULT_FIXTURES)
    args = parser.parse_args(argv)

    key = os.environ.get("FUNCTION_KEY")
    if not args.base_url or not key:
        sys.exit("--base-url(또는 FUNCTION_BASE_URL)과 환경 변수 FUNCTION_KEY가 필요합니다.")

    started_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    events = []
    for channel in CHANNELS:
        events += read_events(args.fixtures / channel / "events.jsonl")
    sent = send_all(events, args.base_url, key)
    print(f"started_at={started_at}  (verify_arrival.py의 --since 값으로 사용)")
    print("sent:", sent, "total:", sum(sent.values()))


if __name__ == "__main__":
    main()
