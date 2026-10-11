# 공통 Silver 이벤트 계약 v1

- 한 행은 한 비즈니스 이벤트다. 상품 배열을 펼쳐 이벤트 행을 늘리지 않는다.
- A 원본 v1/v2는 같은 공통 v1으로 변환한다. 원본 버전과 공통 버전은 별개다.

## 헤더

- 단일 상점 PoC의 업무 키는 (channel_id, event_id)다.
- 다른 채널에서 같은 order_id가 나와도 다른 주문이다.

| 필드 | 논리 타입 / 의미 |
|---|---|
| schema_version | String, 공통 계약 버전 1.0 |
| event_id | String, 재전송에도 유지되는 업무 이벤트 ID |
| channel_id | String, A/B/C |
| event_type | String, ORDER_CREATED / ORDER_CANCELLED / PAYMENT_FAILED / PAYMENT_SUCCEEDED / REFUND_COMPLETED |
| event_time | UTC Timestamp, 업무 발생 시각 |
| order_id | String, 채널 내 주문 ID |
| uid | nullable String, 채널 내 고객 ID |
| data | 이벤트별 아래 필드를 가진 구조 |

로컬 변환 결과는 JSON 호환성을 위해 Timestamp를 UTC 문자열로, 정수 KRW 금액을 int로 표현한다. Spark 적재 시 Timestamp와 Decimal(18,0)을 명시해야 한다. 실제 단일 Silver Delta 테이블의 data StructType과 저장 코드는 아직 구현하지 않았다.

## 이벤트 payload

- 기존에 정의한 다섯 이벤트와 상품 필드를 유지한다.
- 기대 행 6개는 expected_common_events.json에 수기로 명시한다.

| 유형 | data 필드 |
|---|---|
| ORDER_CREATED | order_amount, items(order_item_id, sku_id, quantity, unit_price, line_amount) |
| ORDER_CANCELLED | cancellation_id, cancel_reason, items(order_item_id, sku_id, cancelled_quantity, cancelled_amount) |
| PAYMENT_FAILED | payment_id, requested_amount, payment_method, failure_code |
| PAYMENT_SUCCEEDED | payment_id, paid_amount, payment_method |
| REFUND_COMPLETED | refund_id, payment_id, refund_amount, items(order_item_id, sku_id, refunded_quantity, refunded_amount) |

같은 업무 이벤트의 구·신 형식이 동일한 공통 값으로 변환되면 중복으로 취급한다. 공통 업무 값이 다르면 충돌로 취급한다. 원문과 원본 schema_version은 Bronze에서 보존한다. 형식 버전이 바뀌었다는 이유만으로 별도 event_id를 발행하지 않는다.

상품 마스터의 SCD2는 Silver 이벤트의 schema_version 변경이 아니다. Gold의 SKU와 거래 시각으로 상품 버전을 찾아 fact에 product_key를 보관하는 방향으로 연결한다.
