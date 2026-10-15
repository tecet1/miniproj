"""파이프라인 설정 파일(pipeline_config.json)을 읽는다. 노트북은 이 함수로 트리거를 받아 stream 함수에 넘기기만 한다.

[트리거 형식] writeStream.trigger(**trigger)에 그대로 넘길 dict. {"processingTime": "30 seconds"} 또는 {"availableNow": True}.
Gold 증분 스트림(run_gold_stream)은 초 단위 정수를 받으므로 trigger_seconds로 바꿔 쓴다.
"""
import json
import re
from pathlib import Path

CONFIG_PATH = Path(__file__).resolve().with_name("pipeline_config.json")
LAYERS = ("bronze", "silver", "gold")


def load_config(path=CONFIG_PATH) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def trigger_for(layer: str, config: dict = None) -> dict:
    """계층의 트리거 dict. 알 수 없는 계층이나 모르는 트리거 종류는 오류다 (잘못된 설정이 조용히 기본값으로 돌지 않게)."""
    if layer not in LAYERS:
        raise ValueError(f"알 수 없는 계층: {layer}")
    trigger = (config or load_config())["triggers"][layer]
    if set(trigger) == {"availableNow"} and trigger["availableNow"] is True:
        return dict(trigger)
    if set(trigger) == {"processingTime"} and re.fullmatch(r"[1-9][0-9]* seconds?", str(trigger["processingTime"])):
        return dict(trigger)
    raise ValueError(f"{layer} 트리거 형식이 잘못되었다: {trigger}")


def trigger_seconds(layer: str, config: dict = None) -> int:
    """processingTime 트리거의 초. availableNow면 None (한 번 처리하고 끝내는 설정)."""
    trigger = trigger_for(layer, config)
    return int(trigger["processingTime"].split()[0]) if "processingTime" in trigger else None
