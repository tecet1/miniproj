# 수집·송신

- **추가: 기존 Function App을 통해 A 고정 이벤트를 한 건씩 HTTP POST하는 송신기를 준비했다.**
- **변경: Function의 파티션 키는 Silver의 order_id가 아니라 A 원본의 order_no를 사용한다.**
- 로컬 검증만 완료하며 실제 Azure 배포·송신·Bronze 적재는 직접 실행해 확인해야 한다.

## Function App 수정

- **변경: 기존 create_batch의 event['order_id']를 event['order_no']로 바꾼다.**
- **추가: channel_id/order_no가 없거나 빈 문자열이면 송신 전에 HTTP 400을 반환한다.**

function_app.py는 제공된 함수의 수정본이다. 기존 앱에 다른 함수가 있다면 해당 receive_events 변경 부분만 반영한다. 전체 앱을 덮어쓰지 않는다.

```python
batch = producer.create_batch(partition_key=f"{event['channel_id']}:{event['order_no']}")
```

원본 JSON은 변환하지 않고 그대로 Event Hubs로 전달한다. Function의 관리 ID에는 해당 Hub 범위의 Azure Event Hubs Data Sender 권한이 필요하다. EVENT_HUB_NAMESPACE는 '<namespace>.servicebus.windows.net'이며 https://와 :9093은 붙이지 않는다. EVENT_HUB_NAME은 Bronze 노트북의 subscribe와 같아야 한다(현재 commerce-events).

## 실행 순서

- **추가: Function 수정·배포 → 일반 컴퓨트의 Bronze 스트림 시작 → 단건 송신 → 전체 6건 송신 순서다.**
- **추가: 매 실행의 test_run_id로 기존 입력과 이번 결과를 구분한다. event_id는 재전송해도 유지한다.**

프로젝트 루트에서 실행한다. 송신기는 Python 표준 라이브러리만 사용한다. Function용 Azure SDK는 로컬 송신기에 필요하지 않다.

```powershell
$env:FUNCTION_APP_URL = "https://<현재-function-host>/api/receive_events"
python .\02_pipeline\ingestion\send_http_events.py --limit 1
python .\02_pipeline\ingestion\send_http_events.py --interval 10
```

Function URL에는 ?code=를 넣지 않는다. Function key는 실행 중 숨겨진 입력으로 받는다. 키를 코드·fixture·Git에 저장하지 않는다. 필요하면 기존 FUNCTION_APP_KEY 환경 변수를 사용할 수 있다.

단건 요청이 HTTP 200이고 해당 test_run_id의 원문이 Bronze에 적재되는지 먼저 확인한다. 이어서 전체 실행을 하고 새 test_run_id로 6건을 확인한다. --interval 10은 요청 사이 10초 대기이며 첫 요청 전/마지막 요청 후에는 대기하지 않는다. --file로 다른 계약 버전의 fixture를 선택할 수 있다.

## 함수·문법과 설계 이유

- **추가: 입력 읽기(load_events), 한 건 송신(send_event), CLI 실행(main)을 분리한다.**
- **추가: 실행 메타데이터는 원본 업무 필드를 바꾸지 않고 복사본에만 추가한다.**

load_events는 pathlib.Path와 with로 UTF-8 JSONL을 읽는다. send_event의 {**event, ...}는 원본 dict를 펼친 새 dict이며 test_run_id와 실제 송신 직전 published_at을 붙인다. *, run_id는 반드시 이름으로 전달하게 하여 URL·키·실행 ID를 혼동하지 않도록 한다.

urllib.request.Request의 method/data/headers는 POST·UTF-8 JSON bytes·인증 헤더를 지정한다. urlopen(timeout=30)과 with는 요청 대기를 제한하고 응답 자원을 닫는다. uuid4는 매 실행을 구분하고 argparse는 --limit/--interval/--file을 재사용 가능한 옵션으로 제공한다. 자동 재시도는 하지 않고 실패한 이벤트 ID에서 중단한다. 타임아웃이 송신 실패를 확정하지는 않으므로 재송신 여부는 Bronze 결과와 함께 판단한다.

## 실패 위치 확인

- **추가: HTTP 성공과 Bronze 성공을 각각 확인한다.**
- HTTP 400은 본문·필드, 401/403은 Function 키·접근 정책, 500은 Function 로그의 키 접근·Hub 설정·관리 ID 권한·SDK 오류를 확인한다.

HTTP 200은 함수의 send_batch가 예외 없이 끝났다는 뜻이다. Bronze 적재 완료를 의미하지 않는다. Event Hubs Incoming Messages는 보조 지표이고 갱신 지연이 있을 수 있다. Bronze가 늘지 않으면 Hub 이름, 스트림 활성 상태·예외·입력 배치, Databricks의 읽기 권한·네트워크를 확인한다.

## 로컬 검증

- **추가: Azure를 호출하지 않고 6개 원본의 파티션 키·본문 보존·HTTP 직렬화·잘못된 입력 거절을 검증한다.**
- SDK mock 테스트는 실제 Azure 인증·연결·배포를 검증하지 않는다.

```powershell
python -m unittest discover -s .\02_pipeline\tests -p test_http_ingestion.py -v
```

참고: [Function HTTP 인증](https://learn.microsoft.com/en-us/azure/azure-functions/functions-bindings-http-webhook-trigger), [Event Hubs 관리 ID](https://learn.microsoft.com/en-us/azure/event-hubs/authenticate-managed-identity).
