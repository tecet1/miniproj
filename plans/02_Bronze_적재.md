# 계획 02. Event Hubs → Bronze 적재

## 목표

Event Hubs의 봉투 메시지를 원본 그대로 Bronze 단일 테이블에 적재한다.

## 입력 / 산출물

- 입력: Event Hubs `commerce-events`의 봉투 `{"channel","received_at","payload"}`. 계획 01 7단계(도착 확인 72건)가 통과한 상태여야 한다.
- 산출물
  - `02_pipeline/bronze/`: 적재 함수 `.py`(읽기·봉투 파싱·쓰기를 작은 함수로 분리), 실행 노트북(함수 호출만), 건수·내용 대조 함수
  - Delta 테이블 `<catalog>.<schema>.bronze_events` (미정 항목 참조)
  - 체크포인트 위치 (Volume)

## 설계 결정

- 읽기: Databricks 내장 Kafka 커넥터로 Event Hubs의 Kafka 호환 엔드포인트(`<네임스페이스>.servicebus.windows.net:9093`, 토픽 = 허브 이름)를 `spark.readStream`으로 읽는다. 추가 라이브러리 없음.
- 실행: `trigger(availableNow=True)`로 쌓인 분량을 처리하고 종료한다. 오프셋은 체크포인트가 관리하므로 재실행해도 이미 적재한 분량은 다시 읽지 않는다. `startingOffsets`는 `earliest`.
- Bronze는 단일 테이블, append only다. 중복(S19)과 깨진 payload(S21)도 그대로 적재한다. 중복 제거와 격리는 Silver에서 한다.
- 컬럼
  - `channel`, `received_at`(Function이 부여한 UTC 시각), `payload`(원본 JSON 문자열): 봉투에서 꺼낸다.
  - `eh_partition`, `eh_offset`, `eh_enqueued_at`: Kafka 메타(`partition`, `offset`, `timestamp`).
  - `ingested_at`: 적재 시각.
- 봉투는 Function이 항상 같은 형식으로 만들므로 `from_json`으로 파싱한다. payload는 문자열 그대로 두고 해석하지 않는다.
- 인증: Unity Catalog 서비스 자격 증명(액세스 커넥터, Managed Identity 기반)을 `databricks.serviceCredential` 옵션으로 지정한다. 커넥터에는 Event Hubs 읽기 역할을 이미 부여했다. `kafka.sasl.*`, `kafka.security.protocol`은 함께 주지 않는다. Databricks Runtime 16.1 이상 필요. 연결 문자열·키는 쓰지 않는다.
- 소비자 그룹: `databricks`를 `kafka.group.id`로 지정한다. 오프셋 관리는 체크포인트가 한다.
- 위치: 카탈로그 `commerce_poc`, 테이블 `commerce_poc.bronze.events`, 체크포인트 `/Volumes/commerce_poc/bronze/checkpoints/bronze_events`.
- 클러스터: 서버리스·일반 클러스터 모두 사용 가능. 서비스 자격 증명 방식은 문서상 공유 클러스터와 서버리스에서 권장된다.
- 구현 분담: `kafka_options`는 구현·테스트 완료. `read_events`, `parse_envelope`, `write_bronze`는 입출력 명세와 함께 스텁으로 두고 직접 구현한다.

## 처리 기록

- 구 스키마 테이블: `commerce_poc.bronze.events`에 이전 작업의 테이블(컬럼 `raw_payload`, `source_event_hub`, `partition`, `offset`, `broker_timestamp`, `bronze_ingested_at`)이 있었고, 사용자가 처리한 뒤 새 스키마로 생성했다.
- 이전 테스트 메시지: Event Hubs 파티션 0의 offset 14~20에 이전 A 채널 v1 형식 메시지 7건이 남아 있었다(봉투 없음, `channel`·`received_at` null로 적재됨). 사용자가 Bronze에서 `DELETE ... WHERE channel IS NULL`로 삭제했다. 허브에는 보관 기간 동안 남으며, 체크포인트를 새로 만들면 다시 적재되므로 그때는 다시 지우거나 `channel IS NOT NULL`로 걸러야 한다.
- 인증은 서비스 자격 증명(`commerce_event_hubs_reader`), 포트 9093, 소비자 그룹 `databricks`로 동작했다.

## 단계

0. 계획 01 7단계 통과 확인 (완료: 72건 일치)
1. 설계 값 확정 (위 참조)
2. `02_pipeline/bronze/` 함수 작성: 읽기 옵션 구성 → 봉투 파싱 → Delta 쓰기
3. 노트북 작성(함수 호출만) 후 Git folder 동기화
4. Databricks에서 실행하고 대조 함수로 건수·내용 확인
5. 같은 노트북을 한 번 더 실행해 추가 적재가 0건인지 확인

## 완료 조건

- Bronze 행 수 = 이번 송신분 72건 (채널별 24건). 중복(S19) 2건과 S21 변형 3건×3채널이 모두 포함된다.
- `(channel, payload)` 멀티셋이 `01_contracts/data/fixtures/{A,B,C}/events.jsonl`과 같다.
- `received_at`이 모든 행에 있고, `(eh_partition, eh_offset)`이 중복되지 않는다.
- 재실행 후 행 수가 그대로다.
- 저장소에 키·연결 문자열이 없다.

## 검증 방법

- Databricks에서 대조 함수를 실행해 건수와 멀티셋 비교 결과를 출력한다. 기대값은 fixtures와 `expected.json`이며 수정하지 않는다.
- 같은 허브에 송신을 여러 번 했다면 Event Hubs에는 72건의 배수가 있다. 이 경우 `received_at`이 송신 시작 시각 이후인 행만 비교한다.
- 로컬 테스트는 표준 라이브러리만 쓰므로 Spark 로직은 로컬에서 검증하지 않는다. Databricks 실행 전까지 '미검증'으로 적는다.

## 범위 밖

- 중복 제거, 격리, 채널별 변환(Silver), 스키마 버전 처리, 실행 모니터링, Backfill, Key Vault

## 검증 결과 (2026-10-12, Databricks 서버리스)

확인함
- 채널별 A 24, B 24, C 24 (합계 72). S19 중복과 S21 변형이 포함된다.
- `bronze_check.check_bronze`가 `OK`: `(channel, payload)` 멀티셋이 fixtures와 같다.
- `channel`이 있는 행 중 `received_at`이 null인 행 0건.
- 적재 셀 재실행 후에도 채널별 24/24/24 유지 (사용자 보고).
- `(eh_partition, eh_offset)` 중복 없음: 전체 72행, 고유 72 (사용자 쿼리 결과).

확인하지 않음
- 저장소의 키·연결 문자열 부재는 작성 시 코드 검토로만 확인했다.

## 상태

완료 — 위 '확인함' 범위 기준. '확인하지 않음' 항목은 후속 확인 대상.
