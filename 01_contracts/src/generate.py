"""생성기 진입점. 시나리오를 읽어 채널별 events.jsonl을 만든다.

events.jsonl 한 줄 = {"channel": "A", "payload": "<원본 본문을 JSON 문자열로 직렬화한 값>"}
송신기는 payload 문자열을 그대로 HTTP 본문으로 보내고, 수신 시각(received_at)은 Function이 찍는다.
"""
import argparse
import json
from pathlib import Path

import canonical
import channel_a
import channel_b
import channel_c
import faults
import scenario_loader
import scenario_validator

CHANNELS = {"A": channel_a, "B": channel_b, "C": channel_c}
ROOT = Path(__file__).resolve().parents[2]


def _dump(obj) -> str:
    """한글을 그대로 두고 공백 없이 직렬화한다 (같은 입력 -> 같은 문자열)."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))


def build_lines(channel: str, scenarios: list[dict], skus: dict[str, dict]) -> list[str]:
    """한 채널의 events.jsonl 줄 목록을 돌려준다 (줄바꿈 문자는 붙이지 않는다).

    중립 이벤트마다: to_raw -> fault가 있으면 apply_fault -> payload 문자열 -> {"channel", "payload"} 한 줄.
    send_count 만큼 같은 줄을 반복한다 (S19). 현재 시각·난수를 쓰지 않으므로 결과는 항상 같다.
    """
    module = CHANNELS[channel]
    lines = []
    for scenario in scenarios:
        for event in canonical.build_events(scenario, skus, channel):
            raw = module.to_raw(event)
            if event["fault"]:
                raw = faults.apply_fault(raw, event["fault"], module.FAULT_PATHS, module.UNKNOWN_EVENT_CODE)
            line = _dump({"channel": channel, "payload": _dump(raw)})
            lines.extend([line] * event["send_count"])
    return lines


def write_fixtures(out_dir, scenarios: list[dict], skus: dict[str, dict]) -> None:
    """out_dir/{A,B,C}/events.jsonl 을 쓴다. UTF-8, 줄바꿈은 LF로 고정, 마지막 줄도 줄바꿈으로 끝낸다."""
    for channel in CHANNELS:
        path = Path(out_dir) / channel / "events.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        text = "\n".join(build_lines(channel, scenarios, skus)) + "\n"
        path.write_text(text, encoding="utf-8", newline="\n")


def main(argv=None) -> None:
    """명령줄 실행. 기본 입력은 01_contracts/contracts, 기본 출력은 01_contracts/data_fixtures.

    순서: 파일 읽기(scenario_loader) -> scenario_validator.validate_scenarios -> write_fixtures.
    """
    parser = argparse.ArgumentParser(description="채널별 events.jsonl 생성")
    parser.add_argument("--out", default=ROOT / "01_contracts" / "data_fixtures", type=Path)
    args = parser.parse_args(argv)

    contracts = ROOT / "01_contracts" / "contracts"
    skus = scenario_loader.load_skus(contracts / "skus.json")
    scenarios = scenario_loader.load_scenarios(contracts / "scenarios.json")
    scenario_validator.validate_scenarios(scenarios, skus)
    write_fixtures(args.out, scenarios, skus)
    for channel in CHANNELS:
        print(f"{channel}: {len(build_lines(channel, scenarios, skus))} lines -> {args.out / channel / 'events.jsonl'}")


if __name__ == "__main__":
    main()
