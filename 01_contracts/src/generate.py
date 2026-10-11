"""생성기 진입점. 시나리오를 읽어 채널별 events.jsonl을 만든다.

events.jsonl 한 줄 = {"channel": "A", "payload": "<원본 본문을 JSON 문자열로 직렬화한 값>"}
송신기는 payload 문자열을 그대로 HTTP 본문으로 보내고, 수신 시각(received_at)은 Function이 찍는다.
"""
import json  # noqa: F401
from pathlib import Path  # noqa: F401

import canonical  # noqa: F401
import channel_a
import channel_b
import channel_c
import faults  # noqa: F401
import scenario_loader  # noqa: F401

CHANNELS = {"A": channel_a, "B": channel_b, "C": channel_c}
ROOT = Path(__file__).resolve().parents[2]


def build_lines(channel: str, scenarios: list[dict], skus: dict[str, dict]) -> list[str]:
    """한 채널의 events.jsonl 줄 목록을 돌려준다 (줄바꿈 문자는 붙이지 않는다).

    각 중립 이벤트마다: channel 모듈의 to_raw -> fault가 있으면 faults.apply_fault -> payload 문자열로 직렬화
    -> {"channel", "payload"} 한 줄. send_count 만큼 같은 줄을 반복한다 (S19).
    직렬화 규칙: json.dumps(..., ensure_ascii=False, separators=(",", ":")) 로 한글을 그대로 두고 공백을 없앤다.
    같은 입력이면 항상 같은 결과여야 한다 (현재 시각, 난수를 쓰지 않는다).
    """
    raise NotImplementedError


def write_fixtures(out_dir, scenarios: list[dict], skus: dict[str, dict]) -> None:
    """out_dir/{A,B,C}/events.jsonl 을 쓴다. UTF-8, 줄바꿈은 LF(\\n)로 고정, 마지막 줄도 줄바꿈으로 끝낸다.

    힌트: Path.write_text(..., encoding="utf-8", newline="\\n"), 폴더가 없으면 mkdir(parents=True, exist_ok=True)
    """
    raise NotImplementedError


def main() -> None:
    """명령줄 실행. 기본 입력은 01_contracts/contracts, 기본 출력은 01_contracts/data/fixtures.

    순서: 파일 읽기 -> validate_scenarios -> write_fixtures. 끝나면 채널별 줄 수를 출력한다.
    """
    raise NotImplementedError


if __name__ == "__main__":
    main()
