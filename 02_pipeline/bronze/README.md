# Bronze 원문 적재

- **추가: 01_event_hubs_to_bronze_live.ipynb로 일반 컴퓨트에서 계속 실행되는 A 스트리밍을 확인한다.**
- 기존 원문·출처 컬럼과 Delta 테이블·체크포인트를 유지한다.
- 로컬 구조·문법 검증은 완료했지만 실제 Databricks 실행은 미검증이다.

## 실행 조건

- **변경: 기존 availableNow=True는 시작 시점 입력을 처리하고 종료한다. 새 입력을 계속 받으려면 processingTime='5 seconds'로 실행한다.**
- **추가: 같은 체크포인트를 사용하는 기존 쿼리를 먼저 중지한다. 체크포인트를 삭제하거나 두 쿼리를 동시에 실행하지 않는다.**

일반 컴퓨트에 노트북을 Import하거나 연결된 Git 폴더에서 연다. databricks.serviceCredential은 DBR 16.1 이상이 필요하다. 이 컴퓨트에서 commerce_event_hubs_reader 자격 증명 사용·Event Hubs 읽기·Volume 읽기/쓰기·테이블 생성/추가가 가능한지 확인한다. 기존 노트북이 다른 컴퓨트에서 성공했다면 현재 컴퓨트의 접근 모드·권한·네트워크도 다시 확인해야 한다.

## 확인 순서

- **추가: 설정 → 스트림 시작 → HTTP 송신 → 상태/결과 반복 확인 → 스트림 중지 순서다.**
- **추가: 송신기에서 출력한 test_run_id를 위젯에 넣어 이번 실행의 원문만 조회한다.**

스트림 시작 셀은 awaitTermination을 호출하지 않으므로 다음 셀을 실행할 수 있다. 단건 송신 확인은 EXPECTED_COUNT=1, 전체 송신은 6을 사용한다. 상태 셀과 결과 셀은 데이터 도착 중 다시 실행한다. 전체 송신 후 row_count=6, distinct_event_ids=6, missing_ids/unexpected_ids=[], duplicate_rows=0을 확인한다. 원문 중복이 발견되면 별도로 기록하며 Bronze가 업무 이벤트 중복 제거까지 수행한다고 주장하지 않는다.

raw_payload의 event_id/event_name/order_no와 원본 fixture도 대조한다. 기존 브론즈 스키마를 유지하므로 test_run_id/published_at은 새 테이블 컬럼이 아니라 JSON 본문에만 있다. 체크포인트가 존재하면 startingOffsets='earliest'보다 저장된 진행 위치를 사용한다.

검증 후 STOP_STREAM=True로 마지막 셀을 실행한다. 5초는 확인 간격이며 적재 지연 보장이 아니다. 고정 occurred_at으로 수집 지연을 계산하지 않는다. published_at은 HTTP 송신 직전 시각, bronze_ingested_at은 micro-batch 평가 시각이므로 커밋 완료까지의 정확한 지연은 별도 실험이 필요하다.

## 유지하는 범위

- Silver 정규화·Quarantine·멱등 저장은 다음 단계다.
- 파일 채널의 Landing·Auto Loader는 추후 별도로 연결한다.

참고: [트리거 설정과 체크포인트 재사용](https://learn.microsoft.com/en-us/azure/databricks/structured-streaming/triggers), [Kafka 서비스 자격 증명](https://learn.microsoft.com/en-us/azure/databricks/connect/streaming/kafka/authentication).
