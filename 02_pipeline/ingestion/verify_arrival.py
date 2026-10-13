r"""전송한 이벤트가 Event Hubs에 빠짐없이, 원본 그대로 도착했는지 확인한다.

소비자 그룹 verify로 파티션 전체를 처음부터 읽어, fixtures의 (channel, payload)와 건수까지 비교한다.
실행 (프로젝트 루트, PowerShell. az login 필요, 사용자 계정에 'Azure Event Hubs Data Receiver' 역할 필요)
  $env:EVENTHUB_FQDN = "<네임스페이스>.servicebus.windows.net"; $env:EVENTHUB_NAME = "commerce-events"
  python .\02_pipeline\ingestion\verify_arrival.py --since <send_events.py가 출력한 started_at>

compare, load_expected는 순수 함수라 로컬에서 테스트한다. read_envelopes는 Azure 접속이 필요해 배포 후 확인한다.
"""
import argparse
import json
import os
import sys
import threading
from collections import Counter
from pathlib import Path

CHANNELS = ("A", "B", "C")
ROOT = Path(__file__).resolve().parents[2]
DEFAULT_FIXTURES = ROOT / "01_contracts" / "data_fixtures"


def load_expected(fixtures_dir) -> Counter:
    """fixtures의 events.jsonl 전체를 {(channel, payload): 건수}로 센다. 중복 전송은 건수 2로 남는다."""
    expected = Counter()
    for channel in CHANNELS:
        with open(Path(fixtures_dir) / channel / "events.jsonl", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    outer = json.loads(line)
                    expected[(outer["channel"], outer["payload"])] += 1
    return expected


def compare(expected: Counter, received: Counter) -> list[str]:
    """기대와 수신의 차이를 문장 목록으로 돌려준다. 빈 목록이면 일치한다."""
    problems = []
    for key, count in (expected - received).items():
        problems.append(f"missing x{count}: channel={key[0]} payload={key[1][:80]}")
    for key, count in (received - expected).items():
        problems.append(f"unexpected x{count}: channel={key[0]} payload={key[1][:80]}")
    return problems


def read_envelopes(fqdn: str, hub: str, consumer_group: str, idle_seconds: int = 10) -> list[dict]:
    """Event Hubs를 처음부터 읽어 봉투 dict 목록을 돌려준다.

    모든 파티션에서 idle_seconds 동안 새 이벤트가 없으면 끝난 것으로 본다.
    Azure 패키지는 이 함수 안에서만 import한다 (다른 함수를 Azure 없이 테스트하기 위함).
    """
    from azure.eventhub import EventHubConsumerClient
    from azure.identity import DefaultAzureCredential

    client = EventHubConsumerClient(fully_qualified_namespace=fqdn, eventhub_name=hub,
                                    consumer_group=consumer_group, credential=DefaultAzureCredential())
    partition_ids = client.get_partition_ids()
    envelopes, idle = [], set()

    def on_batch(context, events):
        if events:
            idle.discard(context.partition_id)
            envelopes.extend(json.loads(event.body_as_str()) for event in events)
        else:                                  # max_wait_time 동안 새 이벤트가 없으면 빈 목록으로 호출된다
            idle.add(context.partition_id)

    thread = threading.Thread(target=client.receive_batch, kwargs=dict(
        on_event_batch=on_batch, max_batch_size=100, max_wait_time=idle_seconds, starting_position="-1"))
    thread.start()
    while len(idle) < len(partition_ids):
        thread.join(timeout=1)
    client.close()
    thread.join()
    return envelopes


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Event Hubs 도착 확인")
    parser.add_argument("--since", required=True, help="이 시각(received_at) 이후 수신분만 비교 (예: 2026-10-12T01:00:00.000Z)")
    parser.add_argument("--consumer-group", default="verify")
    parser.add_argument("--fixtures", type=Path, default=DEFAULT_FIXTURES)
    args = parser.parse_args(argv)

    fqdn, hub = os.environ.get("EVENTHUB_FQDN"), os.environ.get("EVENTHUB_NAME")
    if not fqdn or not hub:
        sys.exit("환경 변수 EVENTHUB_FQDN, EVENTHUB_NAME이 필요합니다.")

    envelopes = read_envelopes(fqdn, hub, args.consumer_group)
    # received_at은 모두 같은 형식의 UTC 문자열이라 문자열 비교로 시각 순서를 비교할 수 있다
    recent = [e for e in envelopes if e.get("received_at", "") >= args.since]
    received = Counter((e["channel"], e["payload"]) for e in recent)
    expected = load_expected(args.fixtures)

    print(f"expected {sum(expected.values())}, received {sum(received.values())} (since {args.since})")
    problems = compare(expected, received)
    for problem in problems:
        print(problem)
    print("OK" if not problems else f"FAIL: {len(problems)} differences")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
