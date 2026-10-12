"""채널 변환기가 함께 쓰는 필드 읽기 도구. 실패하면 ConversionError(사유)를 낸다."""
from conversion_error import ConversionError


def require(data: dict, key: str):
    """필수 필드 값을 돌려준다. 키가 없거나 값이 None이면 MISSING_REQUIRED."""
    value = data.get(key)
    if value is None:
        raise ConversionError("MISSING_REQUIRED", key)
    return value


def as_int(value, name: str) -> int:
    """정수만 받는다. 문자열, 소수, bool은 INVALID_TYPE. (bool은 파이썬에서 int의 하위 타입이라 따로 막는다)"""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ConversionError("INVALID_TYPE", f"{name}={value!r}")
    return value
