"""S21 오류 주입. 채널 원본 dict에 '검증 실패' 조건을 만든다. 채널마다 필드 위치가 달라 경로 표를 받아서 일한다."""
import copy


def apply_fault(raw: dict, fault: str, paths: dict, unknown_code: str) -> dict:
    """raw의 복사본에 fault를 적용해 돌려준다 (원본 raw는 바꾸지 않는다).

    fault 종류와 동작 (paths는 각 채널 모듈의 FAULT_PATHS)
      MISSING_REQUIRED   paths["payment_amount"] 위치의 키를 삭제
      INVALID_TYPE       paths["quantity"] 위치의 값을 숫자가 아닌 문자열("N/A")로 교체
      UNKNOWN_EVENT_TYPE paths["event_code"] 위치의 값을 unknown_code로 교체
    그 외 이름이면 ValueError.
    """
    result = copy.deepcopy(raw)
    if fault == "MISSING_REQUIRED":
        container, key = _locate(result, paths["payment_amount"])
        del container[key]
    elif fault == "INVALID_TYPE":
        container, key = _locate(result, paths["quantity"])
        container[key] = "N/A"
    elif fault == "UNKNOWN_EVENT_TYPE":
        container, key = _locate(result, paths["event_code"])
        container[key] = unknown_code
    else:
        raise ValueError(f"unknown fault: {fault}")
    return result


def _locate(raw: dict, path: tuple):
    """경로 튜플을 따라 내려가 (마지막 키를 담은 컨테이너, 마지막 키)를 돌려준다."""
    container = raw
    for key in path[:-1]:
        container = container[key]
    return container, path[-1]
