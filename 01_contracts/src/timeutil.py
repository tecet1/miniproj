"""시각 형식 변환. 채널별 원본 시각 형식 3종을 만든다.

입력은 항상 시간대 정보가 있는(aware) datetime이다. 표준 라이브러리만 쓴다.
"""
from datetime import UTC, datetime, timedelta, timezone

KST = timezone(timedelta(hours=9))   # 한국은 일광절약시간이 없어 고정 오프셋으로 충분하다


def to_iso_utc(dt: datetime) -> str:
    """A 채널 형식. 예: 2026-10-12T01:00:00.000Z (UTC, 밀리초 3자리, 끝에 Z)

    힌트: dt.astimezone(UTC), strftime, microsecond
    """
    return f"{dt.astimezone(UTC).strftime('%Y-%m-%dT%H:%M:%S')}.{dt.microsecond // 1000:03d}Z"


def to_epoch_ms(dt: datetime) -> int:
    """B 채널 형식. 1970-01-01T00:00:00Z 이후 경과 밀리초(정수).

    힌트: 부동소수점 오차를 피하려면 timestamp() * 1000 대신 timedelta 나눗셈을 쓴다.
    """
    return int((dt.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(milliseconds=1))

def to_kst_string(dt: datetime) -> str:
    """C 채널 형식. 예: 2026-10-12 10:00:00 (KST, 시간대 표기 없음)."""
    return dt.astimezone(KST).strftime('%Y-%m-%d %H:%M:%S')