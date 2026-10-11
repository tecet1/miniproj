"""S21 오류 주입. 채널 원본 dict에 '검증 실패' 조건을 만든다. 채널마다 필드 위치가 달라 경로 표를 받아서 일한다."""


def apply_fault(raw: dict, fault: str, paths: dict, unknown_code: str) -> dict:
    """raw의 복사본에 fault를 적용해 돌려준다 (원본 raw는 바꾸지 않는다).

    fault 종류와 동작 (paths는 각 채널 모듈의 FAULT_PATHS)
      MISSING_REQUIRED   paths["payment_amount"] 위치의 키를 삭제
      INVALID_TYPE       paths["quantity"] 위치의 값을 숫자가 아닌 문자열("N/A")로 교체
      UNKNOWN_EVENT_TYPE paths["event_code"] 위치의 값을 unknown_code로 교체
    그 외 이름이면 ValueError.

    힌트: copy.deepcopy, 경로 튜플을 따라 마지막 직전까지 내려가는 작은 보조 함수
    """
    raise NotImplementedError
