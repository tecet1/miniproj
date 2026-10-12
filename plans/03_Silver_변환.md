# 계획 03. Bronze → Silver 변환

## 목표

Bronze의 채널별 원본을 공통 이벤트 모델로 변환해 이벤트 유형별 Silver 테이블에 적재하고, 검증 실패는 Quarantine으로 격리한다.

## 입력 / 산출물

- 입력
  - `commerce_poc.bronze.events` (채널, 수신 시각, 원본 payload 문자열)
  - `01_contracts/contracts/event_spec.md` (공통 모델·채널 매핑·Quarantine 사유), `A사_ERD.pptx` 1장 (Silver 테이블 구조)
  - 기대값: `01_contracts/data/fixtures/expected/expected.json`
- 산출물
  - `02_pipeline/silver/channels/`: 채널별 변환 `channel_a.py`, `channel_b.py`, `channel_c.py` (원본 JSON 문자열 → 공통 이벤트 또는 격리 사유)
  - `02_pipeline/silver/`: 공통 처리(검증, 중복 제거, 테이블 분배, 적재), 실행 노트북(호출만)
  - 테이블 `commerce_poc.silver.{orders, order_items, payments, refunds, order_cancellations, quarantine}`
  - 테스트 `02_pipeline/tests/silver/`

## 이미 정해진 것 (ERD·명세 기준, 이 계획에서 다시 정하지 않는다)

- 테이블 6개: 이벤트 유형별 적재. 주문 생성 → `orders`(PK channel, order_id)·`order_items`(PK channel, order_item_id), 결제 성공·실패 → `payments`(PK channel, payment_id, `status`로 구분), 환불 → `refunds`(PK channel, refund_id), 주문 취소 → `order_cancellations`(PK channel, order_id, 주문당 0..1), 검증 실패 → `quarantine`.
- 이벤트 1건이 1행이며 추가만 한다. 중복은 `channel + event_id`로 제거한다. 모든 이벤트 테이블에 `received_at`을 둔다.
- 시각은 UTC `TIMESTAMP`로 통일한다. C의 KST 문자열(시간대 표기 없음)은 UTC로 변환한다.
- 비로그인은 `user_id = null` (C의 빈 문자열 포함). C의 `order_item_id`는 `ord_no-seq`.
- 결제수단·사유·실패 코드가 매핑에 없으면 `OTHER`이며 오류가 아니다. 환불 유형은 `OTHER`가 없어 매핑에 없으면 `INVALID_VALUE`로 격리한다 (2026-10-12 승인).
- 결제가 주문보다, 환불이 결제보다 먼저 와도 격리하지 않는다. 조인은 Gold에서 한다.
- Quarantine 사유 코드: `MALFORMED_JSON`, `UNKNOWN_EVENT_TYPE`, `MISSING_REQUIRED`, `INVALID_TYPE`, `INVALID_VALUE`, `UNKNOWN_CHANNEL` (`DUPLICATE_CONFLICT`는 후속, PoC 미구현).
- 채널별 변환은 `channels/`에, 공통 처리는 별도 파일에 둔다 (AGENTS.md).

## 구현된 Bronze와 문서 (정리 완료)

Bronze를 먼저 구현해 문서와 어긋났던 부분을 2026-10-12 사용자 승인으로 구현에 맞췄다.

- `event_spec.md` 0절·1.1절, AGENTS.md §2, ERD 1장(`A사_ERD.pptx`)을 반영했다.
- Bronze 컬럼은 `channel`, `received_at`, `payload`, `eh_partition`, `eh_offset`, `eh_enqueued_at`, `ingested_at`이며 키는 `(eh_partition, eh_offset)`이다. `ingest_run_id`는 두지 않는다.
- `channel`은 요청 URL 경로의 값을 Function이 봉투에 담은 것이다. payload에서 판단하지 않는다.
- Quarantine은 Bronze 행을 그대로 옮기고 사유를 붙인다: Bronze 7컬럼 + `reason_code`, `quarantined_at`. 키는 `(eh_partition, eh_offset)`. (2026-10-12 승인, 명세·ERD 반영)
- 후속 과제: 채널별 수집기·키 분리(채널 인증이 필요해질 때).

## 설계 결정

확정 (2026-10-12)
1. **변환 방식:** 채널별 변환을 순수 Python 함수로 작성하고 Spark에서는 UDF로 호출한다. 로컬·CI에서 fixtures로 검증할 수 있다. 입력(원본 JSON 문자열)과 출력(공통 이벤트 또는 격리 사유) 인터페이스를 고정해 두어, 대량 부하 실험(`04_experiments`)에서 필요하면 pandas UDF나 Spark 표현식으로 구현만 바꿀 수 있게 한다.
2. **적재 방식:** Bronze를 `availableNow` 스트림으로 읽고, `foreachBatch`에서 배치마다 변환해 각 테이블에 `channel + event_id` 기준 insert-only `MERGE`로 쓴다. 같은 배치 안의 중복은 MERGE 전에 먼저 제거한다.
3. **검증 방식:** (가) 왕복 테스트: 생성기의 공통 이벤트 → 채널 원본 → Silver 변환 결과가 처음의 공통 이벤트와 같은지 확인한다. (나) 격리 확인: S21의 오류 주입 이벤트가 맞는 `reason_code`로 격리되고 정상 테이블에 들어가지 않는지 확인한다.
4. **Quarantine 컬럼:** 위 "구현된 Bronze와 문서" 참조.
5. **중복 충돌(`DUPLICATE_CONFLICT`):** 최초 건 유지 규칙만 구현한다. "최초 건" = `received_at` 오름차순, 같으면 `(eh_partition, eh_offset)` 오름차순. 충돌 건의 격리는 PoC에서 구현하지 않는다 (2026-10-12 확정). 단일 행 검증은 Silver, 테이블 간 업무 규칙 검증은 Gold로 나누며, 이 항목은 이벤트 신원 판단이라 Silver 후속 과제다. 후속 조건: 내용 해시 컬럼 또는 이벤트 원장, 충돌 시나리오·기대값 추가(명세 변경 순서 적용).

구현 원칙: 변환 함수는 역할별 작은 함수로 나누고 매핑 상수(이벤트 코드, 결제수단, 사유)는 한곳에 둔다. 채널 파일 3개는 같은 구조를 가져 읽는 순서가 같도록 한다.


## 단계

0. Bronze 적재 완료 (완료)
1. 문서·명세를 구현된 Bronze에 맞춰 정리 (완료)
2. 설계 결정 확정 (위 1~5, 완료)
3. 채널별 변환 A → B → C: 입출력 명세 먼저, 한 채널씩 구현하고 짝 테스트
4. 공통 처리: 필수 필드·타입·값 검증, 중복 제거, 이벤트 유형별 분배
5. Spark 적재와 노트북 (호출만)
6. Databricks 실행, 건수·금액·참조·격리 대조, 재실행

## 완료 조건

- 건수가 `expected.json` 합계와 일치한다: `orders` 27, `order_items` 33, `payments` 27, `order_cancellations` 3, `refunds` 3, `quarantine` 9.
- Bronze 72 = `orders` 27 + `order_cancellations` 3 + `payments` 27 + `refunds` 3 + `quarantine` 9 + 제거된 중복 3.
- 격리 9건은 채널마다 3종(`MISSING_REQUIRED`, `INVALID_TYPE`, `UNKNOWN_EVENT_TYPE`)이 한 건씩이며, 원본 payload와 Bronze 위치가 보존된다.
- `payments`에서 S19 중복이 1건으로 줄고, 주문당 결제 시도 수(S06: 실패 1·성공 1)가 맞는다.
- 금액 정합: 주문별 `payment_amount` = 상품 결제금액 합 + 배송비. 환불은 원결제·상품 행을 참조한다 (참조 누락 0건, `channel + payment_id`·`channel + order_item_id`).
- 시각 변환: A는 ISO UTC, B는 epoch ms, C는 KST 문자열을 같은 UTC 시각으로 변환한다. S23의 결제 시각이 UTC 2026-10-11T15:30:00Z가 된다.
- 비로그인 `user_id`가 null이다.
- 같은 Bronze로 재실행해도 행 수가 그대로다.
- 저장소에 키·연결 문자열이 없다.

## 검증 방법

- 로컬: `python -m unittest discover -s .\02_pipeline\tests\silver -v` (고정 입력 fixtures를 채널별 변환에 넣어 공통 이벤트와 비교)
- Databricks: 적재 후 대조 함수로 테이블별 건수·합계·참조·격리 사유를 `expected.json`과 비교하고 재실행 후 건수를 확인한다. 실행 전까지 '미검증'으로 적는다.
- 기대값은 구현 통과를 위해 수정하지 않는다.

## 범위 밖

- Gold(Fact·Dimension), Metric View, 대시보드, 상품 SCD Type 2, 스키마 버전 처리, 이상 주문 목록·미결 목록, Backfill, 실행 모니터링

## 상태

설계 결정 1~5 확정. 채널 A·B·C 변환 완료 (로컬 Silver 테스트 84개 통과, 2026-10-12). 공통 처리(중복 제거·분배·INVALID_VALUE 검증)와 Spark 적재는 미착수. Databricks 실행은 미검증.
