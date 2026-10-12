"""채널 변환기가 함께 쓰는 필드 읽기 도구. 실패하면 ConversionError(사유)를 낸다."""
from conversion_error import ConversionError


def get_required(data: dict, key: str):
    """필수 필드 값을 돌려준다. 키가 없거나 값이 None이면 MISSING_REQUIRED."""
    value = data.get(key)
    if value is None:
        raise ConversionError("MISSING_REQUIRED", key)
    return value


def get_required_int(data: dict, key: str) -> int:
    """필수 정수 필드 값을 돌려준다. 없으면 MISSING_REQUIRED, 정수가 아니면 INVALID_TYPE.

    문자열, 소수, bool은 정수로 보지 않는다. (bool은 파이썬에서 int의 하위 타입이라 따로 막는다)
    """
    value = get_required(data, key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConversionError("INVALID_TYPE", f"{key}={value!r}")
    return value


def get_required_dict(data: dict, key: str) -> dict:
    """필수 객체(중첩 dict) 필드 값을 돌려준다. 없으면 MISSING_REQUIRED, dict가 아니면 INVALID_TYPE."""
    value = get_required(data, key)
    if not isinstance(value, dict):
        raise ConversionError("INVALID_TYPE", f"{key}={value!r}")
    return value


def map_code(table: dict, value, default: str = "OTHER") -> str:
    """원본 코드를 공통 값으로 바꾼다. 표에 없으면(None 포함) default를 돌려준다. 오류가 아니다.

    결제수단, 사유, 결제 실패 사유에 쓴다. (환불 유형은 default가 없으므로 build_refund에서 따로 처리)
    """
    return table.get(value, default)
