# Silver 품질·중복·Quarantine 계약

- **추가:** DQ-01~04를 적용하고 예상 입력 오류만 격리한다.
- **추가:** 정상/오류 ID를 전송 메타데이터와 분리해 재전송·재시도에도 행 수를 유지한다.

## 네 규칙과 처리 순서

- 파싱 → 원본 버전/필드 매핑 → 공통 타입/코드/UTC 변환 → 업무 키 비교 순서다.
- 한 입력에 여러 문제가 있으면 순서상 첫 오류 하나를 기록한다. 범용 규칙 엔진은 만들지 않는다.

| 규칙 | 적용·실패 조건 | 코드 예 | 처리 |
|---|---|---|---|
| DQ-01 | 모든 정상 후보의 (channel_id, event_id) 비교 | EVENT_KEY_CONFLICT | 같은 공통 값은 중복 제거, 다른 값은 격리 |
| DQ-02 | 이벤트별 필수값의 누락/null/빈 문자열·상품 배열의 빈 값 | MISSING_OR_INVALID_FIELD, NULL_ITEM | Quarantine |
| DQ-03 | JSON·시각·타입·이벤트/상태 코드의 정규화 실패 | INVALID_JSON, INVALID_TIMESTAMP, TIMEZONE_REQUIRED, INVALID_INTEGER, UNSUPPORTED_EVENT_TYPE | Quarantine |
| DQ-04 | 선언된 원본 버전·채널·SKU의 미지원/미매핑 | UNSUPPORTED_SCHEMA_VERSION, UNMAPPED_SKU, INVALID_SKU_MAPPING | Quarantine |

필수 문자열의 다른 타입은 INVALID_STRING(DQ-03), payload의 다른 구조는 INVALID_PAYLOAD(DQ-03)다. 수량의 IntegerType 및 금액의 Decimal(18,0) 표현 한계를 넘으면 INTEGER_OVERFLOW/DECIMAL_OVERFLOW(DQ-03)다. 이는 물리 타입 검증이며 양수 금액·누적 환불 대조 같은 추가 업무 규칙이 아니다.

## 이벤트별 적용 범위

- 주문/취소/환불은 비어 있지 않은 items와 항목별 키·SKU·수량·단가를 요구한다.
- 결제 성공/실패는 헤더 이벤트이며 items와 SKU를 요구하지 않는다.

공통 필수값은 schema_version, channel_id, event_id, event_name, occurred_at, order_no, payload다. customer_no는 없거나 null이어도 된다. 제공했다면 비어 있지 않은 문자열이어야 한다. 원본 v2 주문에는 order_total이 필요하며 v1의 total로 대신 읽지 않는다.

items 누락/null/빈 배열 및 배열 안 null은 DQ-02다. 배열 대신 객체를 주거나 배열 안에 객체가 아닌 값을 넣으면 DQ-03다. 허용하지 않는 결제수단·취소사유·실패코드는 DQ-03이다. 추가 필드는 변환에서 사용하지 않으며 Bronze 원문에 보존한다.

## 중복·충돌 정책

- 공통 업무 JSON의 키 순서를 고정하고 SHA-256으로 business_hash를 만든다.
- 원본 버전·test_run_id·published_at·Bronze 출처/시각은 업무 해시에 넣지 않는다.

원본 v1/v2가 같은 공통 업무 값이면 같은 이벤트다. items 순서는 현재 계약에서 유지하므로 임의로 순서를 바꾼 배열까지 같은 이벤트로 판정하지 않는다.

새 키의 서로 다른 값이 같은 micro-batch에 있으면 양쪽을 모두 격리하고 정상 행을 선택하지 않는다. 기존 Silver 키가 있으면 최초 커밋된 값을 유지하고 다른 업무 값만 격리한다. 따라서 입력을 어느 batch로 나눴는지에 따라 최초 수용 여부는 달라질 수 있다. 전역 이력 재판정은 이번 범위가 아니다.

동일 업무 값의 전달 중 첫 Bronze 시각·출처 순으로 정상 후보 한 행을 선택한다. MERGE는 없는 키만 INSERT한다. 기존 행과 최초 처리 시각을 UPDATE하지 않는다. 중복 전달은 Quarantine 오류가 아니다. 정상 전용 시험에서는 입력 건수 - 고유 업무 키 수로 중복 수를 검산한다. 별도 누적 감사 테이블은 만들지 않는다.

## Quarantine 필드와 키

- 원문·최초 출처·실패 코드·검증 시각을 보존한다.
- 같은 오류의 재전송은 하나의 오류 사례로 집계하며, 전달 횟수는 Bronze에 남는다.

| 필드 | 의미 |
|---|---|
| quarantine_id | rule_id + error_code + 오류 입력 fingerprint의 SHA-256 |
| channel_id, event_id | 읽을 수 있는 원본 식별자. 파싱 실패 등에서는 null 가능 |
| rule_id, error_code, error_message | 첫 실패 규칙·안정적인 코드·설명 |
| raw_payload | 최초 실패 원문 |
| source_event_hub, partition, offset | 최초 전달의 Bronze 출처 |
| broker_timestamp, bronze_ingested_at | Bronze에 보존된 시각 |
| validated_at | 최초 Quarantine INSERT의 검증 시각 |

파싱 가능한 객체의 오류 fingerprint는 test_run_id/published_at을 제외한 원본 JSON이다. 파싱 불가능한 JSON·객체가 아닌 입력은 원문 텍스트를 사용한다. 같은 의미라도 파싱 불가 원문 공백이 다르면 다른 오류 사례다. 업무 키 충돌은 공통 business_hash를 fingerprint로 사용한다.

## 저장·재시도 보장과 한계

- Quarantine과 Silver 각각 insert-only MERGE를 사용한다.
- 예상 입력 오류만 잡는다. 코드 버그·권한·Delta 쓰기 오류는 실행을 실패시킨다.

정상/격리 판정을 materialize한 뒤 Quarantine → Silver 순서로 저장한다. Quarantine 이후 Silver가 실패하면 같은 입력을 재시도하여 누락된 저장을 완료한다. 각 테이블의 중복은 키로 막지만 두 테이블의 커밋은 원자적이지 않다. 읽는 사용자는 재시도 완료 전 중간 상태를 볼 수 있다.

동일 writer의 고정 변환 코드·매핑과 append-only Bronze를 전제로 한다. 동시 writer, 오류 재처리/해결 상태 변경, 코드·매핑 변경 뒤 과거 입력 자동 재평가는 구현하지 않는다. 다른 writer가 같은 대상에 쓰지 않도록 운영한다. 규칙을 수정해 재처리하려면 별도 검토가 필요하다.

## 입력과 독립 기대 결과

- 정상·오류·재전송·충돌 입력을 분리한다.
- Delta 결과의 원격 검증은 03_validate_silver.ipynb에서 수행한다.

quality_cases.json은 16개 오류의 원문과 기대 rule_id/error_code다. malformed JSON은 HTTP에서 거절되므로 격리된 Bronze 형식 입력으로 시험한다. replay_events.jsonl은 v1 6건 + v2 6건이며 고유 업무 이벤트는 6개다. conflict_events.jsonl은 새 키에 서로 다른 업무 값 2개를 갖는다. quality_expected.json의 기대값은 파이프라인에서 계산하지 않고 고정했다.
