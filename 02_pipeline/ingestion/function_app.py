"""Azure Functions HTTP 진입점. 얇게 유지한다: 요청에서 값을 꺼내 ingest.handle_event에 넘기고 결과를 응답으로 바꾼다.

POST /api/events/{channel}  (Function 키 필요: x-functions-key 헤더)
이 파일은 azure-functions 패키지가 필요해서 로컬 단위 테스트에서 import하지 않는다.
"""
import logging
from datetime import datetime, timezone

import azure.functions as func

import eventhub_client
import ingest

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)


@app.route(route="events/{channel}", methods=["POST"])
def receive_event(req: func.HttpRequest) -> func.HttpResponse:
    channel = req.route_params.get("channel", "")
    status, message = ingest.handle_event(channel, req.get_body(), eventhub_client.send, _now_iso())
    if status >= 500:
        logging.error("event not sent: channel=%s status=%s", channel, status)
    return func.HttpResponse(message, status_code=status)


def _now_iso() -> str:
    """수신 시각. 예: 2026-10-12T01:00:00.123Z (UTC, 밀리초 3자리)"""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"
