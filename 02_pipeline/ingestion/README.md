# ingestion — HTTP 수신 → Event Hubs 전송

흐름: `send_events.py` →(POST /api/events/{A|B|C})→ `function_app.py` → `ingest.handle_event` → `eventhub_client.send` → Event Hubs

| 파일 | 역할 | 비고 |
|---|---|---|
| `ingest.py` | 채널 검사, 본문 검증, 봉투 생성. 순수 함수 | **직접 구현** (테스트: `tests/ingestion/test_ingest.py`) |
| `function_app.py` | HTTP 트리거 → `handle_event` 호출 | 배포 대상 |
| `eventhub_client.py` | Managed Identity로 Event Hubs 전송 | 배포 대상, 미검증 |
| `send_events.py` | fixtures를 Function에 전송(재시도 1s·2s·4s) | 로컬 실행, 배포 제외 |
| `verify_arrival.py` | Event Hubs 도착분을 fixtures와 비교 | 로컬 실행, 배포 제외, 미검증 |

환경 변수(Function): `EVENTHUB_FQDN`, `EVENTHUB_NAME`. 로컬 실행용 `local.settings.json`은 커밋하지 않는다.
응답 코드: 202 성공 / 400 채널·인코딩 오류 / 503 전송 실패.

테스트(표준 라이브러리만 사용): `python -m unittest discover -s .\02_pipeline\tests\ingestion -v`
