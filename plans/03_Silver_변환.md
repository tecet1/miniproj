# 계획 03. Bronze → Silver 변환

## 목표

Bronze의 채널별 원본을 공통 이벤트 모델로 변환해 이벤트 유형별 Silver 테이블에 적재하고, 검증 실패는 Quarantine으로 격리한다.

## 입력 / 산출물

- 입력
  - `commerce_poc.bronze.events` (채널, 수신 시각, 원본 payload 문자열)
  - `01_contracts/contracts/event_spec.md` (공통 모델·채널 매핑·Quarantine 사유), `A사_ERD.pptx` 1장 (Silver 테이블 구조)
  - 기대값: `01_contracts/data_fixtures/expected/expected.json`
- 산출물
  - `02_pipeline/silver/channels/`: 채널별 변환 `channel_a.py`, `channel_b.py`, `channel_c.py` (원본 JSON 문자열 → 공통 이벤트 또는 격리 사유)
  - `02_pipeline/silver/` 공통 처리 (한 파일은 한 가지 책임):
    - 행 단위(순수 Python, 로컬 테스트): `silver_row_to_event.py`(채널 분기·JSON 파싱·변환·값 범위), `validate_skus.py`(참조에 없는 SKU), `convert_row.py`(Bronze 행 → UDF 결과 한 건)
    - 테이블 정의: `silver_tables.py`(컬럼·타입·PK, 공통 이벤트 JSON 스키마, 테이블 생성)
    - Spark 적재(Databricks 전용): `silver_stream.py`(UDF 변환, 격리 분리, 중복 제거, 테이블 분배, insert-only MERGE, foreachBatch)
    - 검증: `silver_check.py`(expected.json 대조), 실행 노트북 `run_silver.py`(호출만)
  - 테이블 `commerce_poc.silver.{orders, order_items, payments, refunds, order_cancellations, quarantine}`
  - 테스트 `02_pipeline/tests/silver/`

## 이미 정해진 것 (ERD·명세 기준, 이 계획에서 다시 정하지 않는다)

- 테이블 6개: 이벤트 유형별 적재. 키·컬럼의 기준은 `event_spec.md` 1.3절이다. 주문 생성 → `orders`(PK channel, event_id)·`order_items`(PK channel, order_item_id), 결제 성공·실패 → `payments`(PK channel, event_id, `status`로 구분), 환불 → `refunds`(PK channel, event_id), 주문 취소 → `order_cancellations`(PK channel, event_id, 주문당 0..1), 검증 실패 → `quarantine`(PK eh_partition, eh_offset). `A사_ERD.pptx` 1장은 이에 맞춰 갱신해야 한다 (사용자 작업).
- 행동 테이블은 이벤트 1건이 1행이며(주문 생성 이벤트만 `orders` 1행 + `order_items` N행) 추가만 한다. 중복은 이벤트 단위로 `channel + event_id`를 먼저 제거한 뒤 테이블로 나눈다. `order_items`를 뺀 모든 이벤트 테이블에 `received_at`을 둔다.
- 컬럼 이름은 `{행동}_created_at`(업무 발생 시각), `{행동}_reason_code`를 따르고(`payment_fail_reason_code` 포함) 공통 모델 필드명은 바꾸지 않는다. 테이블에 쓸 때 이름을 바꾼다.
- `payment_id`는 같은 결제의 실패·성공 이벤트가 공유할 수 있어 PK가 아니다 (2026-10-13 승인).
- 시각은 UTC `TIMESTAMP`로 통일한다. C의 KST 문자열(시간대 표기 없음)은 UTC로 변환한다.
- 비로그인은 `user_id = null` (C의 빈 문자열 포함). C의 `order_item_id`는 `ord_no-seq`.
- 결제수단·사유·실패 코드가 매핑에 없으면 `OTHER`이며 오류가 아니다. 환불 유형은 `OTHER`가 없어 매핑에 없으면 `INVALID_VALUE`로 격리한다 (2026-10-12 승인).
- 주문 생성 이벤트의 `product_id`가 `skus.json`에 없으면 `INVALID_VALUE`로 격리한다 (2026-10-13 승인). 자리표시 행은 후속이다 (`sku_spec.md`).
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
2. **적재 방식:** Bronze를 `availableNow` 스트림으로 읽고, `foreachBatch`에서 배치마다 변환해 각 테이블의 PK(`order_items`는 `channel + order_item_id`, 나머지는 `channel + event_id`) 기준 insert-only `MERGE`로 쓴다. 같은 배치 안의 중복은 MERGE 전에 먼저 제거한다.
3. **검증 방식:** (가) 왕복 테스트: 생성기의 공통 이벤트 → 채널 원본 → Silver 변환 결과가 처음의 공통 이벤트와 같은지 확인한다. (나) 격리 확인: S21의 오류 주입 이벤트가 맞는 `reason_code`로 격리되고 정상 테이블에 들어가지 않는지 확인한다.
4. **Quarantine 컬럼:** 위 "구현된 Bronze와 문서" 참조.
5. **중복 충돌(`DUPLICATE_CONFLICT`):** 최초 건 유지 규칙만 구현한다. "최초 건" = `received_at` 오름차순, 같으면 `(eh_partition, eh_offset)` 오름차순. 충돌 건의 격리는 PoC에서 구현하지 않는다 (2026-10-12 확정). 단일 행 검증은 Silver, 테이블 간 업무 규칙 검증은 Gold로 나누며, 이 항목은 이벤트 신원 판단이라 Silver 후속 과제다. 후속 조건: 내용 해시 컬럼 또는 이벤트 원장, 충돌 시나리오·기대값 추가(명세 변경 순서 적용).

구현 원칙: 변환 함수는 역할별 작은 함수로 나누고 매핑 상수(이벤트 코드, 결제수단, 사유)는 한곳에 둔다. 채널 파일 3개는 같은 구조를 가져 읽는 순서가 같도록 한다.


## Spark 처리 흐름 (2026-10-13)

foreachBatch 배치 하나마다 다음을 수행한다. 함수 이름은 `silver_stream.py`와 같다.

1. `read_bronze`: Bronze를 스트림으로 읽는다. `channel`이 있고 `received_at >= since`인 행만 쓴다 (`bronze_check`와 같은 조건).
2. `convert_bronze`: UDF가 행마다 `convert_row`를 호출해 `result(event_id, event_type, event_json, reason_code)`를 붙인다. `convert_row`는 예외를 내지 않는다 (예외로 죽으면 배치 전체가 실패한다).
3. 격리: `reason_code`가 있는 행은 `build_quarantine`으로 Bronze 7컬럼 + `reason_code` + `quarantined_at`을 만들어 `(eh_partition, eh_offset)` 기준 insert-only MERGE.
4. 정상: `dedupe_events`로 `(channel, event_id)`당 최초 1건(`received_at`, `eh_partition`, `eh_offset` 오름차순)만 남기고, `parse_events`가 `event_json`을 `EVENT_SCHEMA`로 읽는다.
5. 분배: `build_orders`·`build_order_items`·`build_cancellations`·`build_payments`·`build_refunds`가 공통 모델 필드를 테이블 컬럼명으로 바꾼다 (`event_time` → `{행동}_created_at`, `fail_reason_code` → `payment_fail_reason_code`, `status`는 `event_type`에서 만든다). 각 테이블은 PK 기준 insert-only MERGE.
6. 같은 배치 안의 중복은 4에서 없애고, 배치 사이의 중복은 MERGE가 막는다.

재실행 검증은 새 체크포인트로 Bronze를 처음부터 다시 읽게 해 건수가 그대로인지 본다. 같은 체크포인트로 다시 실행하면 새 행이 없어 아무것도 하지 않으므로 멱등성 검증이 되지 않는다.

## 단계

0. Bronze 적재 완료 (완료)
1. 문서·명세를 구현된 Bronze에 맞춰 정리 (완료)
2. 설계 결정 확정 (위 1~5, 완료)
3. 채널별 변환 A → B → C: 입출력 명세 먼저, 한 채널씩 구현하고 짝 테스트
4. 공통 처리: 필수 필드·타입·값 검증, 참조 SKU 검증, 테이블 정의 (순수 Python, 로컬 테스트). 중복 제거 규칙은 로컬 테스트에서 같은 규칙을 파이썬으로 흉내 내 건수를 확인한다.
5. Spark 적재(`silver_stream.py`)와 노트북 (호출만). 구현은 Claude가 하고 사용자는 설명을 듣고 검토한다 (2026-10-13 합의).
6. Databricks 실행, 건수·금액·참조·격리 대조, 재실행

## 완료 조건

- 건수가 `expected.json` 합계와 일치한다: `orders` 27, `order_items` 33, `payments` 27, `order_cancellations` 3, `refunds` 3, `quarantine` 9.
- Bronze 72 = `orders` 27 + `order_cancellations` 3 + `payments` 27 + `refunds` 3 + `quarantine` 9 + 제거된 중복 3.
- 격리 9건은 채널마다 3종(`MISSING_REQUIRED`, `INVALID_TYPE`, `UNKNOWN_EVENT_TYPE`)이 한 건씩이며, 원본 payload와 Bronze 위치가 보존된다.
- `payments`에서 S19 중복이 1건으로 줄고, 주문당 결제 시도 수(S06: 실패 1·성공 1)가 맞는다.
- 금액 정합: 주문별 `payment_amount` = 상품 결제금액 합 + 배송비. 환불은 원결제·상품 행을 참조한다 (참조 누락 0건. 결제는 `channel + payment_id`의 `status = SUCCEEDED` 행, 상품 행은 `channel + order_item_id`).
- 시각 변환: A는 ISO UTC, B는 epoch ms, C는 KST 문자열을 같은 UTC 시각으로 변환한다. S23의 결제 시각이 UTC 2026-10-11T15:30:00Z가 된다.
- 참조에 없는 SKU: 단위 테스트로 `INVALID_VALUE` 격리를 확인한다 (현재 픽스처에 해당 시나리오가 없다. 추가하면 Bronze 72건 등 기대값이 바뀌므로 별도 승인).
- 비로그인 `user_id`가 null이다. 변환기 단위 테스트로 확인한다 (현재 픽스처에는 비로그인 이벤트가 없다. `scenarios.md` 보류).
- 업무 ID 중복 검사: `(channel, order_id)`는 `orders`·`order_cancellations`, `(channel, refund_id)`는 `refunds`에서 중복 0건이다. `payment_id`는 같은 ID의 실패·성공 이벤트를 허용하므로 검사 대상이 아니다.
- 같은 Bronze로 재실행해도 행 수가 그대로다.
- 저장소에 키·연결 문자열이 없다.

## 검증 방법

- 로컬: `python -m unittest discover -s .\02_pipeline\tests\silver -v` (고정 입력 fixtures를 채널별 변환에 넣어 공통 이벤트와 비교). `test_silver_fixtures.py`는 72행을 변환·중복 제거해 `expected.json` 채널별 건수·Bronze 수지·금액 정합과 대조하고, `test_silver_tables.py`는 테이블 정의를 `event_spec.md` 1.3절 표와 대조한다.
- Databricks: `run_silver.py` 실행 후 `silver_check.check_silver`로 테이블별 건수·합계·참조·격리 사유를 `expected.json`과 비교하고 재실행 후 건수를 확인한다. 실행 전까지 '미검증'으로 적는다.
- 기대값은 구현 통과를 위해 수정하지 않는다.

## 범위 밖

- Gold(Fact·Dimension), Metric View, 대시보드, 상품 SCD Type 2, 스키마 버전 처리, 이상 주문 목록·미결 목록, Backfill, 실행 모니터링

## 상태

완료 (2026-10-13). 로컬 Silver 테스트와 Databricks 실행(서버리스)을 모두 확인했다.

- 로컬: 채널 A·B·C 변환, 행 단위 공통 처리, 변환 견고성, 참조 SKU 검증, UDF 결과 함수, 테이블 정의, 검사 비교 함수, 가짜 Spark 객체를 쓴 컬럼 순서·MERGE 문장·처리 순서 테스트 (164건 통과, Claude 환경 기준).
- Databricks (사용자 보고): `check_silver`가 최초 실행과 새 체크포인트 재실행 모두 `OK`. 건수는 `orders` 27, `order_items` 33, `payments` 27, `order_cancellations` 3, `refunds` 3, `quarantine` 9로 `expected.json`과 일치했다. 확인 항목은 건수, Bronze 수지, 격리 사유, PK 중복, 참조 누락, 금액 정합이다. 이후 추가한 업무 ID 중복, 격리 원본 보존, S23 시각 변환, S06 결제 시도 수 항목도 `OK`라고 보고받았다.
- 저장소에 키·연결 문자열이 없다: 텍스트 파일(`.py`, `.md`, `.json`, `.yml`, `.txt`)을 키워드로 검색해 확인했다. 배포 워크플로는 GitHub Secrets 참조만 쓴다. Git 이력과 바이너리 파일(pptx, png)은 검사하지 않았다.
- 남은 일(범위 밖): `DUPLICATE_CONFLICT`, 참조에 없는 SKU의 자리표시 행, 무작위 ID, 시나리오 추가는 후속이다. 서버리스 세션이 한 번 끊긴 적이 있어(`Spark session ... no longer usable`) 새 세션에서 재실행해 통과했다.
