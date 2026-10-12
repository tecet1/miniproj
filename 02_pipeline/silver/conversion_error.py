"""채널 원본 한 건을 공통 이벤트로 바꾸지 못할 때 내는 오류. reason_code가 Quarantine 사유가 된다."""

REASON_CODES = ("MALFORMED_JSON", "UNKNOWN_EVENT_TYPE", "MISSING_REQUIRED", "INVALID_TYPE",
                "INVALID_VALUE", "UNKNOWN_CHANNEL")   # DUPLICATE_CONFLICT는 후속 (이벤트 명세 4절)


class ConversionError(Exception):
    def __init__(self, reason_code: str, detail: str = ""):
        super().__init__(f"{reason_code}: {detail}")
        self.reason_code = reason_code
        self.detail = detail
