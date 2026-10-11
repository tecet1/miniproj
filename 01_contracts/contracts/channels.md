# A/B/C 송신·원본·변환 계약

- **변경: 기존 채널 A 계약 문서를 채널 전체의 계약으로 통합했다.**
- **추가: A 원본 v1/v2를 같은 공통 이벤트로 변환한다. B/C는 아래 역할만 기록하고 구체적인 필드는 아직 확정하지 않았다.**

## 송신과 처리 경로

- **추가: 송신 경로와 원본 데이터 계약을 구분한다.**
- **추가: 현재 직접 Event Hubs 송신과 최종 Functions 경유 경로의 검증 상태를 별도로 기록한다.**

| 채널 | 예정 경로 | 현재 상태 |
|---|---|---|
| A | HTTP → Functions → Event Hubs → Bronze | 고정 v1/v2 입력만 생성. 다음 단계는 Event Hubs 직접 송신. Functions 검증은 별도. |
| B | Timer 기반 수집 → Functions → Event Hubs → Bronze | 최소 원본 필드·주기·메시지 단위 미확정 |
| C | 파일 전달 → ADLS Landing → Auto Loader → Bronze | 최소 파일 구조·경로·완료 표시 방식 미확정 |

공식 쇼핑몰 API 연결을 구현한 것으로 표현하지 않는다. B/C의 수집 방식은 역할 배치이며 실제 구현 시 계약을 확정한다. 어느 채널도 수집 누락 대사·Backfill을 기본 범위에 추가하지 않는다.

A의 송신 본문은 events.jsonl 또는 events_v2.jsonl의 한 줄이다. UTF-8 JSON 객체 하나가 하나의 메시지다. 재전송할 때 같은 event_id를 유지한다. 6개를 한 JSON 배열로 감싸 보내지 않는다. 순서대로 송신하더라도 수신·처리 순서를 업무 시각으로 가정하지 않는다.

Bronze는 JSON 전체와 source_event_hub·partition·offset·broker_timestamp·bronze_ingested_at을 보관한다. 향후 C 경로에는 source_file 등 파일 출처를 보관한다.

## A 버전 처리

- **추가: 선언된 원본 버전으로 처리하며, 알 수 없는 버전을 추측하여 변환하지 않는다.**
- **추가: 공통 schema_version은 두 원본 버전 모두 1.0이다.**

| 원본 schema_version | ORDER_CREATED 금액 필드 | 공통 필드 |
|---|---|---|
| 1.0 | payload.total | data.order_amount |
| 2.0 | payload.order_total | data.order_amount |

그 밖의 필드는 두 버전에서 같다. 원본은 서로 다른 JSON이지만 업무 내용이 같은 fixture이므로 event_id도 유지한다. 이벤트 ID의 a-poc-v1은 고정 시나리오 이름이며 원본 schema_version을 뜻하지 않는다.

지원하지 않는 버전, 버전에 필요한 필드 누락, 잘못된 코드·시각·자료형, SKU 미매핑은 normalize_event에서 ValueError를 발생시킨다. 후속 Silver 저장 호출부는 원문과 오류 사유를 Quarantine에 저장해야 한다. 현재 모듈은 Quarantine 저장이나 배치 간 중복 제거를 구현하지 않았다.

## A 원본 필드

- 프로젝트가 직접 정의한 JSON이며 공식 Shopify 웹훅/API 명세가 아니다.
- 원본 헤더와 상품 필드명을 공통 Silver와 다르게 두어 변환 과정을 확인한다.

## 헤더

- 한 줄은 하나의 비즈니스 이벤트다. 주문 상태 스냅샷이나 상품별 행이 아니다.
- 이번 고정 입력에서는 고객 ID를 모든 이벤트에 알고 있는 값으로 제공한다.

| A 원본 필드 | 공통 Silver 필드 / 규칙 |
|---|---|
| `schema_version` | 원본 버전 `1.0`. Silver 버전은 별도로 지정한다. |
| `channel_id` | `A` |
| `event_id` | 고정 이벤트 ID. 채널과 함께 중복 키로 사용한다. |
| `event_name` | 소문자 유형 → 대문자 공통 `event_type` |
| `occurred_at` | UTC 업무 시각 → Timestamp `event_time` |
| `order_no` | 문자열 `order_id` |
| `customer_no` | 문자열 `uid` |
| `payload` | 유형에 따른 공통 이벤트 payload로 변환 |

단일 상점, KRW, 세금·배송비·할인 없음, 미결제 전체 취소, 성공 결제 최대 1번, 부분 환불 1번으로 한정한다. 원본 금액은 정수이며 Silver에서는 Decimal(18,0)으로 변환한다.

## 유형과 payload

- 동일 주문의 생성과 취소는 서로 다른 event_id를 가진다.
- 결제 재시도는 새로운 payment_no를 사용하고 환불은 성공 payment_no를 참조한다.

| `event_name` | 공통 유형 | 원본 payload |
|---|---|---|
| `order_created` | ORDER_CREATED | `total`, `items` |
| `order_cancelled` | ORDER_CANCELLED | `cancel_no`, `reason`, `items` |
| `payment_failed` | PAYMENT_FAILED | `payment_no`, `amount`, `method`, `error` |
| `payment_succeeded` | PAYMENT_SUCCEEDED | `payment_no`, `amount`, `method` |
| `refund_completed` | REFUND_COMPLETED | `refund_no`, `payment_no`, `amount`, `items` |

상품 라인 필드는 `item_no` → order_item_id, `sku` → 매핑 후 sku_id, `qty` → 해당 이벤트의 수량, `price` → 원 단가다. 주문 라인 금액·취소 금액·환불 금액은 각각 qty × price로 계산한다. `sku_mapping.json`의 두 원본 SKU를 사용한다.

취소의 `reason=customer`는 CUSTOMER_REQUEST, 결제의 `method=card`는 CARD, 실패의 `error=card_declined`는 CARD_DECLINED로 표준화한다. 이번 데이터에서 카드 정보는 원본이 직접 제공하는 프로젝트 계약의 값이다.

고정 event_id는 재전송에서도 유지한다. **변경: 같은 ID의 원문 형식이 달라도 공통 업무 값이 같으면 동일 이벤트다. 공통 업무 값이 다르면 충돌로 처리한다.** 상품 배열을 펼쳐 같은 Silver 이벤트를 여러 행으로 만들지 않는다.
