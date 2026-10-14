"""1년치 덤프를 만든다: 채널별 원본 이벤트 줄(events.jsonl)과, 픽스처를 합친 독립 기대값(expected_dump.json).

[입력] 01_contracts/contracts/{skus,scenarios}.json, 04_experiments/configs/dump.json
[출력] 04_experiments/results/dump/{A,B,C}/events.jsonl  (크다. 저장소에 커밋하지 않는다: .gitignore)
       04_experiments/results/expected_dump.json         (작다. 커밋 대상)
[기대값] expected.compute_expected(픽스처 시나리오 + 합성 시나리오) 한 번의 결과다. 이벤트 생성 모듈과 로직을 공유하지 않는다.
        이 기대값은 Bronze에 픽스처(Function 경로)와 덤프(직접 적재)가 모두 들어간 상태의 값이다.
[ID] 이벤트 줄의 ID는 ids.NamedIds(seed)로 만든다. 같은 설정이면 바이트까지 같다. 픽스처는 읽기 쉬운 ID라 충돌하지 않는다.
시나리오 생성은 synth_scenarios(채널별 규모·성장이 다르다), 줄 만들기는 generate가 한다 (여기서는 이어 붙이기만 한다).
"""
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path[:0] = [str(ROOT / "01_contracts" / "src"), str(Path(__file__).resolve().parent)]

import expected  # noqa: E402
import generate  # noqa: E402
import scenario_loader  # noqa: E402
import scenario_validator  # noqa: E402
import synth_scenarios  # noqa: E402
from ids import NamedIds  # noqa: E402

CONTRACTS = ROOT / "01_contracts" / "contracts"
DEFAULT_CONFIG = ROOT / "04_experiments" / "configs" / "dump.json"
DEFAULT_RESULTS = ROOT / "04_experiments" / "results"


def combined_expected(fixture_scenarios: list[dict], synthetic: dict[str, list[dict]], skus: dict[str, dict]) -> dict:
    """픽스처(세 채널 공통) + 채널별 합성 시나리오의 독립 기대값. expected는 sku를 목록으로 받는다."""
    return expected.compute_expected(fixture_scenarios, list(skus.values()), channel_scenarios=synthetic)


def build_dump(config: dict, skus: dict[str, dict], fixture_scenarios: list[dict]) -> tuple[dict[str, list[dict]], dict]:
    """({채널: 합성 시나리오}, 합친 기대값). 채널마다 규모와 성장이 다르며, 시나리오는 검증을 통과해야 한다."""
    synthetic = synth_scenarios.build_channel_scenarios(skus, config)
    for scenarios in synthetic.values():
        scenario_validator.validate_scenarios(scenarios, skus)
    return synthetic, combined_expected(fixture_scenarios, synthetic, skus)


def write_dump(results_dir, config: dict, skus: dict[str, dict], fixture_scenarios: list[dict]) -> dict:
    """results_dir/dump/{A,B,C}/events.jsonl 과 results_dir/expected_dump.json 을 쓰고 기대값을 돌려준다.

    줄 형식은 generate.write_fixtures와 같다 (UTF-8, LF, 마지막 줄도 줄바꿈으로 끝낸다).
    """
    results_dir = Path(results_dir)
    synthetic, combined = build_dump(config, skus, fixture_scenarios)
    ids = NamedIds(config["seed"])
    for channel, scenarios in synthetic.items():
        path = results_dir / "dump" / channel / "events.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        text = "\n".join(generate.build_lines(channel, scenarios, skus, ids)) + "\n"
        path.write_text(text, encoding="utf-8", newline="\n")
    text = json.dumps(combined, ensure_ascii=False, indent=2)
    (results_dir / "expected_dump.json").write_text(text + "\n", encoding="utf-8", newline="\n")
    return combined


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description="1년치 덤프 이벤트와 합친 기대값 생성")
    parser.add_argument("--config", default=DEFAULT_CONFIG, type=Path)
    parser.add_argument("--out", default=DEFAULT_RESULTS, type=Path)
    args = parser.parse_args(argv)

    config = json.loads(args.config.read_text(encoding="utf-8"))
    skus = scenario_loader.load_skus(CONTRACTS / "skus.json")
    fixture_scenarios = scenario_loader.load_scenarios(CONTRACTS / "scenarios.json")
    combined = write_dump(args.out, config, skus, fixture_scenarios)
    for channel in generate.CHANNELS:
        path = args.out / "dump" / channel / "events.jsonl"
        lines = path.read_text(encoding="utf-8").count("\n")
        print(f"{channel}: {lines} lines -> {path} (픽스처 포함 기대 bronze_rows {combined['per_channel'][channel]['bronze_rows']})")
    print(f"wrote {args.out / 'expected_dump.json'}")


if __name__ == "__main__":
    main()
