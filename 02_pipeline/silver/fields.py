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


def get_required_str(data: dict, key: str) -> str:
    """필수 문자열 필드(ID 등) 값을 돌려준다. 없거나 None이거나 빈 문자열이면 MISSING_REQUIRED, 문자열이 아니면 INVALID_TYPE."""
    value = get_required(data, key)
    if not isinstance(value, str):
        raise ConversionError("INVALID_TYPE", f"{key}={value!r}")
    if value == "":
        raise ConversionError("MISSING_REQUIRED", key)
    return value


def get_optional_str(data: dict, key: str):
    """선택 문자열 필드 값을 돌려준다. 없거나 None이거나 빈 문자열이면 None(오류 아님), 문자열이 아니면 INVALID_TYPE."""
    value = data.get(key)
    if value is None or value == "":
        return None
    if not isinstance(value, str):
        raise ConversionError("INVALID_TYPE", f"{key}={value!r}")
    return value


def get_required_dict_list(data: dict, key: str) -> list:
    """필수 객체 배열 필드 값을 돌려준다. 없거나 비어 있으면 MISSING_REQUIRED, 배열이 아니거나 원소가 객체가 아니면 INVALID_TYPE."""
    value = get_required(data, key)
    if not isinstance(value, list):
        raise ConversionError("INVALID_TYPE", f"{key}={value!r}")
    if not value:
        raise ConversionError("MISSING_REQUIRED", key)
    for element in value:
        if not isinstance(element, dict):
            raise ConversionError("INVALID_TYPE", f"{key}[]={element!r}")
    return value


def lookup(table: dict, value):
    """원본 코드를 표에서 찾는다. 표에 없거나 문자열이 아니면 None.

    리스트·객체처럼 해시할 수 없는 값을 table.get에 넣으면 TypeError가 나므로 문자열일 때만 찾는다.
    """
    return table.get(value) if isinstance(value, str) else None


def map_code(table: dict, value, default: str = "OTHER") -> str:
    """원본 코드를 공통 값으로 바꾼다. 표에 없으면(None, 문자열이 아닌 값 포함) default를 돌려준다. 오류가 아니다.

    결제수단, 사유, 결제 실패 사유에 쓴다. (환불 유형은 default가 없으므로 build_refund에서 따로 처리)
    """
    mapped = lookup(table, value)
    return default if mapped is None else mapped
