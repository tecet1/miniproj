# 시나리오 명세 (소량 정답 대조 세트)

이벤트 생성기와 정답 대조 테스트가 따르는 기준 문서다. 시나리오의 구체적인 값(주문, 상품, 시각, 이벤트)은 `scenarios.json`이 기준이며, 이 문서는 규칙과 목적만 적는다. 상품은 `skus.json`(설명은 `sku_spec.md`)을 쓴다.

## 원칙

- 9개 시나리오를 채널 A·B·C에 **같은 내용**으로 적용한다. 채널별 차이는 원본 형식뿐이다(이벤트 명세 참고).
- 모든 ID와 시각은 고정한다. 생성기를 몇 번 실행해도 결과가 같다.
- 기대값은 파이프라인·생성기와 로직을 공유하지 않는 계산 스크립트(`expected.py`)가 `scenarios.json`과 `skus.json`만 읽어 만든다. 건수·금액 기대값은 이 문서에 복제하지 않는다.
- 이벤트는 자연스러운 순서(주문 → 결제 → 환불)로 송신한다. 중복(S19)만 같은 이벤트를 두 번 보낸다.

## `scenarios.json` 구조

| 위치 | 필드 | 설명 |
|---|---|---|
| 시나리오 | `id`, `title`, `start_kst` | 시나리오 ID, 이름, 시작 시각(KST, 오프셋 포함) |
| 주문 | `order_no`, `user_id` | 시나리오 안의 주문 번호, 회원 |
| | `start_offset_sec` | (선택) 시나리오 시작 후 주문 시작까지의 초. 기본 0 |
| | `items[]` | `sku`, `quantity`, `discount`(상품 할인 합계). 단가·상품명·카테고리는 `skus.json`에서 가져온다 |
| | `events[]` | 발생 순서대로 나열한 이벤트 |
| 이벤트 | `type`, `offset_sec` | 이벤트 유형, 주문 시작 후 경과 초 |
| | `method`, `fail_reason` | 결제 이벤트의 결제수단, 실패 사유 |
| | `reason` | 주문 취소·환불 사유 |
| | `item`, `quantity`, `refund_type` | 환불 대상 상품 순번(1부터), 환불 수량, `CANCEL`/`RETURN` |
| | `fault` | (선택) 오류 주입: `MISSING_REQUIRED`, `INVALID_TYPE`, `UNKNOWN_EVENT_TYPE` |
| | `send_count` | (선택) 같은 이벤트를 보내는 횟수. 기본 1 |

## ID, 금액, 시각 규칙

| 항목 | 규칙 | 예시 (채널 A, S01) |
|---|---|---|
| order_id | `{채널}-{시나리오}-{주문번호}` | `A-S01-1` |
| order_item_id | `{order_id}-{순번}` | `A-S01-1-1` |
| payment_id | `{order_id}-P{결제 시도 순번}` | `A-S01-1-P1` |
| refund_id | `{order_id}-R{순번}` | `A-S09-1-R1` |
| event_id | `{order_id}-E{순번}` (주문 안에서 발생 순서) | `A-S01-1-E1` |

- 상품 금액: `item_gross_amount = unit_price × quantity`, `item_paid_amount = item_gross_amount − discount`.
- 배송비: 한 주문의 `item_paid_amount` 합계가 20,000원 이상이면 0원, 미만이면 4,000원이다.
- 결제금액: `payment_amount = Σ item_paid_amount + 배송비`.
- 환불금액: `item_paid_amount × 환불수량 ÷ quantity` (정수 나눗셈). 환불 이벤트가 가리키는 결제는 그 주문의 성공 결제다.
- 시각: 이벤트 시각 = `start_kst + start_offset_sec + offset_sec`. 원본 시각 형식은 A는 ISO 8601 UTC, B는 epoch 밀리초, C는 KST 문자열이다.
- 기준일은 2026-10-12(KST)다.

## 시나리오 목적

| ID | 목적 | 핵심 기대 |
|---|---|---|
| S01 | 채널별 원본이 공통 모델을 거쳐 Fact 1행이 되는 기본 흐름 | `fact_sales_item` 1행 |
| S02 | 배송비 규칙과 경계값(2만 원). 주문 ② 시작은 ①보다 5분 뒤 | 3행. 결제금액 = 상품 결제금액 합 + 배송비가 두 주문 모두 성립 |
| S04 | 상품 단위 할인과 결제금액 계산 | 2행 |
| S06 | 결제 실패 이벤트 처리, 주문당 결제 시도 N건 | `silver.payments` 2행(실패 1·성공 1), Fact는 성공 결제 기준 1행 |
| S08 | 주문 취소 이벤트 처리 | `silver.order_cancellations` 1행, Fact 없음 |
| S09 | 환불 이벤트와 환불 합계 반영 (수량 일부 환불) | `fact_refund` 1행, `fact_sales_item`의 환불 수량·금액에 반영 |
| S19 | `channel + event_id` 기준 중복 제거 | Bronze 2행 보존, Silver `payments` 1행 |
| S21 | 누락 검증과 오류 격리. 변형 3종(`MISSING_REQUIRED`, `INVALID_TYPE`, `UNKNOWN_EVENT_TYPE`)은 각각 독립된 단일 이벤트 | Quarantine 3행, Silver·Gold 반영 없음 |
| S23 | 결제일을 KST 기준으로 계산하는지 확인 (결제 KST 2026-10-12 00:30) | `payment_date` = 2026-10-12 (UTC 날짜 10-11이면 오류) |

## 보류한 시나리오 (미검증)

- 환불 변형: 같은 상품에 2회 환불, 상품별 다른 사유, 유형·사유 조합
- 도착 순서: 결제가 주문보다 먼저 도착, 환불이 결제보다 먼저 도착, 주문이 끝내 오지 않는 경우
- 이상 판정: 취소 + 결제 성공, 환불 수량·금액 초과, 같은 `event_id`에 내용 충돌
- 부분 결제: 한 주문에 결제 성공 2건 이상 (성공 결제금액 합 = 상품금액 합 + 배송비인지 검증)
- 결제 ID: 같은 `payment_id`의 결제 실패 → 성공 이벤트 (`silver.payments` PK가 `(channel, event_id)`라 허용되지만, 현재 시나리오는 시도마다 새 ID를 쓴다)
- 기타: 비로그인 주문, 참조에 없는 SKU, 결제 실패만 있는 주문
- 무작위 ID: 운영 가정·실험 단계에서 `event_id` 등을 시드 고정 UUID로 바꾼다. Silver 변환 코드는 ID 형식에 의존하지 않음을 확인했다(2026-10-13, 픽스처 ID를 UUID로 바꿔 변환 결과 비교). 생성기 `channel_c.py`의 `order_item_id` 순번 파싱(`rsplit`)은 함께 고쳐야 한다.
- 이상 주문 목록과 미결 목록(5분 기준)은 구현과 검증을 PoC 이후로 미룬다.
