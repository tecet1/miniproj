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

## 확인 필요

1. `commerce_poc.bronze.events`가 이미 있다. 이전 작업의 테이블이면 컬럼이 다를 수 있다. 스키마와 행 수를 확인하고, 새 스키마와 다르면 삭제 후 재생성할지(데이터 삭제) 다른 이름을 쓸지 사용자가 정한다.
2. `checkpoints` 볼륨에 이전 체크포인트가 있으면 새 하위 경로를 쓰므로 영향 없음. 다만 이전 데이터 정리 여부는 사용자가 정한다.
3. 서비스 자격 증명 이름 (노트북 `SERVICE_CREDENTIAL`에 입력)
4. Event Hubs Kafka 엔드포인트 포트는 9093을 가정했다. 연결 실패 시 이 값부터 확인한다. (미검증)

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

## 상태

진행 중 — 계획 01 7단계(도착 확인) 통과. 스텁 구현과 Databricks 실행 대기. 원격 실행은 모두 '미검증'.
