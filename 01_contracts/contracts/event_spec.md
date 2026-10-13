# 이벤트 명세 (01_contracts/contracts)

> 상태: 초안. 2026-10-12 환불 수량 필드(`refund_quantity`)와 구현된 Bronze 컬럼·채널 전달 방식(요청 경로)을 반영했다. 2026-10-13 결제 ID 정의(1.2절)와 Silver 테이블 키·컬럼(1.3절)을 반영했다. 5절은 확정한 항목이고, 6절(Gold·지표)을 2026-10-13에 추가했다.

> **기준 ERD:** `01_contracts/data_fixtures/A사_ERD.pdf`(① Bronze·Silver, ② Gold, ③ 적재 흐름·Metric View)가 테이블 구조의 레퍼런스다. Gold 테이블·키·컬럼·적재 방식과 Metric View 지표 구성은 ERD ②③을 따른다. 이 문서와 ERD가 어긋나면 구현하지 않고 사용자에게 확인한다.
>
> **ERD ①의 예외:** ①은 2026-10-13 Silver 변경(1.3절)을 아직 반영하지 않았다. 다른 점은 PK(`orders`·`order_cancellations`는 `(channel, order_id)`, `payments`는 `(channel, payment_id)`, `refunds`는 `(channel, refund_id)`), 시각 컬럼명(`created_at`·`event_time` ↔ `{행동}_created_at`), 결제 실패·환불 사유 컬럼명(`fail_reason_code`·`reason_code` ↔ `payment_fail_reason_code`·`refund_reason_code`)이다. Silver는 ERD ①이 갱신될 때까지 1.3절을 따른다.
>
> **재정의 우선:** 2026-10-13 이후 사용자가 다시 정의한 내용은 이 문서의 기존 서술, `03_serving/reference/smartstore/benchmark.md` 같은 지표 논의, `CONTRIBUTIONS.md` 같은 기록보다 먼저 적용한다.

## 0. 적용 원칙

| 항목 | 결정 |
|---|---|
| 전송 경로 | 모든 채널 동일: HTTP POST → Azure Functions → Event Hubs (`commerce-events`) |
| Bronze | 원본 JSON(`payload`)을 수정 없이 보존 + 수신 메타데이터. 채널은 요청 URL 경로(`/api/events/{channel}`)로 전달받아 Function이 봉투에 담고 `channel` 컬럼으로 둔다 |
| Silver | 채널별 원본을 **공통 이벤트 모델**로 변환. 검증 실패는 Quarantine |
| 이벤트 종류 | 주문 생성 · 주문 취소 · 결제 성공 · 결제 실패 · 환불 성공 (5종) |
| 금액 | 원화 정수(KRW). 상품금액·할인·배송비·결제금액·환불금액을 별도 필드로 분리 |
| 시간 | 공통 모델은 UTC `TIMESTAMP`. 채널 원본 형식은 채널별로 다름(아래 참고) |
| 고객 | `channel + user_id`로 식별(채널 간 통합 없음). 비로그인은 `user_id = null` |
| 스키마 버전 | PoC 범위 제외 |

개념 관계: 주문 1 → 주문상품 N → 결제 N(주문 ID 참조) → 환불(상품 단위).
주문과 결제는 별도 이벤트이므로 **결제가 주문보다 먼저 도착할 수 있습니다.** 이 경우 격리하지 않고 Gold에서 조인합니다.

---

## 1. 공통 이벤트 모델 (Silver)

### 1.1 공통 헤더 (모든 이벤트)

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| event_id | STRING | Y | 이벤트 고유 ID. 중복 제거 키 (`channel + event_id` 유일) |
| event_type | STRING | Y | `ORDER_CREATED` `ORDER_CANCELLED` `PAYMENT_SUCCEEDED` `PAYMENT_FAILED` `REFUND_SUCCEEDED` |
| event_time | TIMESTAMP(UTC) | Y | 업무 발생 시각. 결제일 귀속 기준 |
| channel | STRING | Y | `A` `B` `C` |
| order_id | STRING | Y | 주문 ID (채널 내 유일) |
| user_id | STRING | N | 채널 내 회원 ID. 비로그인이면 null |

Bronze 컬럼: `channel`(요청 경로의 채널), `received_at`(Function이 부여한 수신 시각, UTC), `payload`(원본 JSON 문자열), `eh_partition`, `eh_offset`(Event Hubs 위치, 키는 `(eh_partition, eh_offset)`), `eh_enqueued_at`(Event Hubs 수신 시각), `ingested_at`(Bronze 적재 시각).
Function은 payload를 해석하지 않고 `{"channel","received_at","payload"}` 봉투로 감싸 Event Hubs에 보낸다. `channel`은 A·B·C 외에는 400으로 거절한다.

### 1.2 이벤트별 본문

**ORDER_CREATED** — 주문 생성

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| shipping_fee | INT | Y | 주문 단위 배송비 (0 이상) |
| items[] | ARRAY | Y | 1건 이상 |
| items[].order_item_id | STRING | Y | 주문 내 유일한 상품 행 ID |
| items[].product_id | STRING | Y | 판매자 SKU (공통 상품코드) |
| items[].product_name | STRING | N | 표시용 |
| items[].category | STRING | N | 상품 카테고리 |
| items[].quantity | INT | Y | 1 이상 |
| items[].unit_price | INT | Y | 단가 |
| items[].item_discount | INT | Y | 상품 단위 할인 합계 (0 이상, 단가×수량 이하) |

상품금액 = `unit_price × quantity − item_discount` (Gold에서 계산한다. Silver `order_items`에는 저장하지 않는다).

**ORDER_CANCELLED** — 주문 취소 (결제 전). 주문 전체(주문상품 전체)에 대해서만 발생하며 상품 단위 주문 취소는 없다. 결제 후 상품 단위 처리는 환불 이벤트로 한다.

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| cancel_reason_code | STRING | N | 아래 공통 사유 코드 |

**PAYMENT_SUCCEEDED / PAYMENT_FAILED** — 결제

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| payment_id | STRING | Y | 결제 ID. 같은 결제의 실패·성공 이벤트가 같은 ID를 가질 수 있어 채널 안에서 유일하지 않다. 이벤트는 `event_id`로 구분한다. 생성기는 현재 시도마다 새 ID를 쓴다 |
| payment_method | STRING | Y | `CARD` `EASY_PAY` `BANK_TRANSFER` `VIRTUAL_ACCOUNT` `OTHER` |
| payment_amount | INT | Y | 결제 요청/승인 금액 (배송비 포함, 할인 반영 후) |
| fail_reason_code | STRING | 실패 시 Y | `LIMIT_EXCEEDED` `CARD_DECLINED` `TIMEOUT` `OTHER` |

**REFUND_SUCCEEDED** — 환불 (결제 후, 상품 단위)

| 필드 | 타입 | 필수 | 설명 |
|---|---|---|---|
| refund_id | STRING | Y | 환불 ID |
| payment_id | STRING | Y | 원결제 참조 (귀속일 판별) |
| order_item_id | STRING | Y | 환불 대상 상품 행 |
| refund_quantity | INT | Y | 환불 수량 (1 이상, 해당 상품 행 수량 이하). 수량 일부 환불 가능 |
| refund_amount | INT | Y | 환불 금액 (환불 수량분) |
| refund_type | STRING | Y | `CANCEL`(발송 전) `RETURN`(반품) |
| refund_reason_code | STRING | N | 공통 사유 코드 |

공통 사유 코드: `CHANGE_OF_MIND` `DEFECT` `WRONG_ITEM` `DELAY` `OTHER`

### 1.3 Silver 테이블

Silver는 이벤트를 채널별 행동 맥락으로 해석해 이벤트 유형별 테이블에 쌓는다. 행동 테이블의 단위(grain)는 이벤트 1건이며 PK는 `(channel, event_id)`다. 같은 주문·결제·환불 ID에 이벤트가 여러 번 올 수 있어 업무 ID는 PK가 아니다. `order_items`만 상품 행 단위다. `channel`은 모든 테이블에 있다.

| 테이블 | 원천 이벤트 | PK | 컬럼 (`channel`, PK의 `event_id` 제외) |
|---|---|---|---|
| orders | ORDER_CREATED | channel, event_id | order_id, user_id, shipping_fee, order_created_at, received_at |
| order_items | ORDER_CREATED의 items[] | channel, order_item_id | order_id(FK → orders), product_id, product_name, category, quantity, unit_price, item_discount |
| order_cancellations | ORDER_CANCELLED | channel, event_id | order_id, user_id, cancel_reason_code, cancel_created_at, received_at |
| payments | PAYMENT_SUCCEEDED, PAYMENT_FAILED | channel, event_id | order_id, user_id, payment_id, status, payment_method, payment_amount, payment_fail_reason_code, payment_created_at, received_at |
| refunds | REFUND_SUCCEEDED | channel, event_id | order_id, user_id, refund_id, payment_id, order_item_id, refund_quantity, refund_amount, refund_type, refund_reason_code, refund_created_at, received_at |
| quarantine | 검증 실패 | eh_partition, eh_offset | 4절 Quarantine 컬럼 |

- **컬럼 이름:** 시각은 `{행동}_created_at`, 사유는 `{행동}_reason_code`다. `*_created_at`은 업무 발생 시각(공통 모델의 `event_time`, UTC)이며 적재 시각이 아니다. 결제 실패 사유는 `payment_fail_reason_code`다. 공통 모델 필드명(1.1·1.2절)은 그대로 두고 테이블에 쓸 때만 이름을 바꾼다.
- **`payments.status`:** `SUCCEEDED` 또는 `FAILED`다. 결제 성공 여부는 이 값으로 가른다.
- **`received_at`:** `order_items`를 뺀 모든 이벤트 테이블에 있다. `order_items`는 같은 이벤트에서 나온 `orders` 행을 따른다.
- **중복 제거:** 이벤트 단위로 `(channel, event_id)`를 먼저 제거한 뒤 테이블로 나눈다. 주문 생성 이벤트만 그 뒤에 `orders` 1행과 `order_items` N행으로 펼치며, `order_items`는 `(channel, order_item_id)`로 쓴다. `event_id`는 채널 안에서 이벤트 종류와 관계없이 유일하다고 가정한다(생성기가 보장).
- **업무 ID 참조:** `order_id`·`payment_id`·`refund_id`로 이어지는 참조는 Gold에서 푼다. `payment_id`는 `status = SUCCEEDED` 행을 기준으로 한다.

---

## 2. 채널별 원본 형식

채널마다 필드명·시간 형식·이벤트 코드를 다르게 둡니다. 변환 규칙이 Silver의 핵심 산출물이 됩니다.

| 구분 | A 자사몰 | B 종합 오픈마켓 | C 스포츠 전문몰 |
|---|---|---|---|
| 필드 표기 | snake_case, 평면+items 배열 | camelCase, 중첩 객체 | 약어 snake_case |
| 시간 형식 | ISO 8601 UTC (`2026-10-12T01:23:45.123Z`) | epoch 밀리초 (UTC) | `yyyy-MM-dd HH:mm:ss` **KST, 시간대 표기 없음** |
| 이벤트 구분 필드 | `event_type` | `eventName` | `evt_cd` |
| 비로그인 | `user_id: null` 또는 `""` | `buyer.memberId` 없음 | `mem_no: ""` (빈 문자열) |
| 금액 | 정수 | 정수 | 정수 |

### 2.1 이벤트 종류 매핑

| 공통 | A | B | C |
|---|---|---|---|
| ORDER_CREATED | `order.created` | `ORDER_PLACED` | `O01` |
| ORDER_CANCELLED | `order.cancelled` | `ORDER_CANCELED` | `O09` |
| PAYMENT_SUCCEEDED | `payment.succeeded` | `PAY_APPROVED` | `P01` |
| PAYMENT_FAILED | `payment.failed` | `PAY_DECLINED` | `P02` |
| REFUND_SUCCEEDED | `refund.succeeded` | `REFUND_DONE` | `R01` |

### 2.2 필드 매핑

**공통 헤더**

| 공통 | A | B | C |
|---|---|---|---|
| event_id | `event_id` | `eventId` | `evt_id` |
| event_time | `occurred_at` | `occurredAtMs` | `evt_dt` (KST→UTC 변환) |
| order_id | `order_id` | `orderNo` | `ord_no` |
| user_id | `user_id` | `buyer.memberId` | `mem_no` (빈 값→null) |

**ORDER_CREATED**

| 공통 | A | B | C |
|---|---|---|---|
| shipping_fee | `shipping_fee` | `delivery.fee` | `dlv_amt` |
| items[] | `items[]` | `orderItems[]` | `lines[]` |
| order_item_id | `order_item_id` | `lineId` | `ord_no` + `-` + `seq` |
| product_id | `sku` | `sellerSku` | `goods_cd` |
| product_name | `product_name` | `itemName` | `goods_nm` |
| category | `category` | `categoryName` | `cate_nm` |
| quantity | `quantity` | `qty` | `qty` |
| unit_price | `unit_price` | `unitPrice` | `price` |
| item_discount | `discount_amount` | `discount.total` | `dc_amt` |

**ORDER_CANCELLED**

| 공통 | A | B | C |
|---|---|---|---|
| cancel_reason_code | `reason` | `cancelReason` | `rsn_cd` |

**PAYMENT_***

| 공통 | A | B | C |
|---|---|---|---|
| payment_id | `payment_id` | `paymentId` | `pay_no` |
| payment_method | `method` | `payMethod` | `pay_type` |
| payment_amount | `amount` | `paidAmount` | `pay_amt` |
| fail_reason_code | `failure_reason` | `declineCode` | `fail_cd` |

**REFUND_SUCCEEDED**

| 공통 | A | B | C |
|---|---|---|---|
| refund_id | `refund_id` | `refundId` | `rfd_no` |
| payment_id | `payment_id` | `paymentId` | `pay_no` |
| order_item_id | `order_item_id` | `lineId` | `ord_no` + `-` + `seq` |
| refund_quantity | `refund_quantity` | `refundQty` | `rfd_qty` |
| refund_amount | `amount` | `refundAmount` | `rfd_amt` |
| refund_type | `type` (`cancel`/`return`) | `claimType` (`CANCEL`/`RETURN`) | `clm_tp` (`1`/`2`) |
| refund_reason_code | `reason` | `claimReason` | `rsn_cd` |

### 2.3 코드 값 매핑

| 항목 | 공통 | A | B | C |
|---|---|---|---|---|
| 결제수단 | CARD | `card` | `CARD` | `10` |
| | EASY_PAY | `easy_pay` | `SIMPLE` | `20` |
| | BANK_TRANSFER | `bank` | `TRANSFER` | `30` |
| | VIRTUAL_ACCOUNT | `vbank` | `VBANK` | `40` |
| | OTHER | 그 외 | 그 외 | 그 외 |
| 사유 | CHANGE_OF_MIND | `change_of_mind` | `BUYER_REMORSE` | `01` |
| | DEFECT | `defect` | `DEFECTIVE` | `02` |
| | WRONG_ITEM | `wrong_item` | `WRONG_DELIVERY` | `03` |
| | DELAY | `delay` | `DELAYED` | `04` |
| | OTHER | 그 외 | 그 외 | 그 외 |
| 결제 실패 | LIMIT_EXCEEDED / CARD_DECLINED / TIMEOUT / OTHER | `limit_exceeded` `declined` `timeout` | `LIMIT` `DECLINED` `TIMEOUT` | `F1` `F2` `F3` |

(결제수단·사유·실패 코드는 매핑에 없는 값이면 `OTHER`로 처리하고 오류로 보지 않음. 환불 유형은 `OTHER`가 없으며, 매핑에 없는 값은 `INVALID_VALUE`로 격리)

---

## 3. 원본 예시 (주문 생성)

**A**
```json
{"event_id":"A-evt-000001","event_type":"order.created","occurred_at":"2026-10-12T01:23:45.123Z",
 "order_id":"A-20261012-0001","user_id":"u1001","shipping_fee":3000,
 "items":[{"order_item_id":"A-20261012-0001-1","sku":"SKU-RUN-001","product_name":"러닝화 X","category":"신발",
           "quantity":2,"unit_price":50000,"discount_amount":10000}]}
```

**B**
```json
{"eventId":"B-evt-000001","eventName":"ORDER_PLACED","occurredAtMs":1791768225123,
 "orderNo":"B-20261012-0001","buyer":{"memberId":"m77"},"delivery":{"fee":0},
 "orderItems":[{"lineId":"L1","sellerSku":"SKU-RUN-001","itemName":"러닝화 X","categoryName":"신발",
                "qty":2,"unitPrice":50000,"discount":{"total":0}}]}
```

**C**
```json
{"evt_id":"C-evt-000001","evt_cd":"O01","evt_dt":"2026-10-12 10:23:45",
 "ord_no":"C-20261012-0001","mem_no":"","dlv_amt":2500,
 "lines":[{"seq":1,"goods_cd":"SKU-RUN-001","goods_nm":"러닝화 X","cate_nm":"신발",
           "qty":2,"price":50000,"dc_amt":5000}]}
```

위 세 건은 모두 `ORDER_CREATED`로 변환되며, C는 `mem_no`가 빈 값이라 `user_id = null`, `order_item_id = C-20261012-0001-1`이 됩니다.

---

## 4. 처리 규칙

**중복·순서**
- 중복 제거 키: `channel + event_id`. 동일 키가 다시 오면 무시(멱등). 키는 이벤트 단위이며 테이블 PK는 1.3절을 따른다.
- "최초 건"은 `received_at`이 이른 행, 같으면 `(eh_partition, eh_offset)`이 작은 행이다. 같은 키가 다시 오면 최초 건만 남기고 나머지는 무시한다.
- 같은 `event_id`에 내용이 다른 경우: 최초 건 유지는 같다. 충돌 건을 Quarantine(`DUPLICATE_CONFLICT`)으로 격리하는 것은 **PoC에서 구현하지 않는다(후속).** 판정에 내용 해시 컬럼 또는 이벤트 원장이 필요하고 시나리오·기대값도 없기 때문이다.
- 결제가 주문보다 먼저 와도 격리하지 않음. Gold에서 `order_id`로 조인.
- 환불이 결제보다 먼저 오는 경우도 격리하지 않고 Gold 조인 시점에 매칭.

**Quarantine 사유 코드**

| 코드 | 조건 |
|---|---|
| `MALFORMED_JSON` | 파싱 불가 |
| `UNKNOWN_EVENT_TYPE` | 매핑표에 없는 이벤트 코드 |
| `MISSING_REQUIRED` | 필수 필드 누락, 필수 ID 필드(`event_id`, `order_id`, 상품·결제·환불 ID 등)가 null 또는 빈 문자열, 주문의 `items`가 비어 있음 |
| `INVALID_TYPE` | 정수/시간 형식 오류, ID 필드가 문자열이 아님, `items`가 객체 배열이 아님 |
| `INVALID_VALUE` | 음수 금액, 수량 0 이하, 할인 > 단가×수량, 매핑에 없는 환불 유형, 기준 데이터(`skus.json`)에 없는 `product_id` |
| `UNKNOWN_CHANNEL` | channel이 A/B/C 아님 |
| `DUPLICATE_CONFLICT` | 같은 event_id, 다른 내용 (후속, PoC 미구현) |

**Quarantine 컬럼:** 격리한 Bronze 행을 그대로 옮기고 사유를 붙인다.
`channel`, `received_at`, `payload`, `eh_partition`, `eh_offset`, `eh_enqueued_at`, `ingested_at`(Bronze와 같은 값·이름) + `reason_code`, `quarantined_at`(격리 시각). 키는 `(eh_partition, eh_offset)`.

**지표와의 연결**
- 지표 정의는 6.5절이 기준이다(결제일 귀속은 KST 기준, 원거래 기준). 결제고객수·상품결제건수 등 이전 서술은 6.5절로 옮겼다.

---

## 5. 확정한 항목 (2026-10-13)

| 항목 | 결정 |
|---|---|
| 결제 1건이 여러 상품을 포함 | 상품별 총판매금액은 Gold에서 `order_items`의 상품금액을 `product_id`(SKU)로 상품에 매핑해 계산한다. `payment_amount`는 그 결제 1건의 금액이다. |
| 부분·분할 결제 | 한 주문에 결제 성공 2건 이상을 허용한다. 생성기는 PoC 1차에서 주문당 성공 1건만 만든다. 주문별 성공 결제금액 합은 상품금액 합 + 배송비와 같아야 하며, 이 대조는 테이블 간 규칙이라 Gold에서 검증한다. |
| 취소 단위 | 주문 취소는 주문 전체(주문상품 전체)만 허용한다. 상품별 취소는 없다. |
| 배송비 환불 | 환불은 상품 단위만 두고 배송비 환불은 제외한다. 순판매금액은 상품금액 기준이다. |
| 채널 A/B/C 형식 차이 | 현재 수준(필드명·시간·코드)을 유지한다. |

**남은 결정 (Gold 설계 시):** 결제 성공이 2건 이상인 주문에서 `fact_sales_item`의 `payment_id`·`payment_at`(결제일 귀속)을 어느 결제로 잡을지.

---

## 6. Gold와 지표 (2026-10-13)

테이블 구조의 레퍼런스는 `A사_ERD.pdf` ②③이다. Gold는 Silver에서 주기적으로 다시 계산해 MERGE한다. 이 절이 지표 정의의 기준이다.

### 6.1 Fact

| 테이블 | grain (단위) | PK | 컬럼 (`channel`, PK 제외) |
|---|---|---|---|
| fact_sales_item | 결제 성공한 주문상품 1행 | channel, order_item_id | product_id(FK), payment_date(FK, 결제일 KST), payment_method(FK), order_id, user_id, payment_id, payment_at, quantity, item_gross_amount, item_discount, item_paid_amount, refund_quantity, refund_amount, last_refund_date |
| fact_refund | 환불 이벤트 1행 | channel, refund_id | product_id(FK), payment_date(FK, 원결제일 KST), refund_date(FK, 환불일 KST), refund_profile_key(FK), order_item_id, order_id, payment_id, refund_quantity, refund_amount |

- **fact_sales_item 원천:** `order_items`를 `orders`(`channel, order_id`)와 `payments`(`channel, order_id`, `status = SUCCEEDED`)에 조인한다. 결제 성공이 있는 주문의 모든 상품 행이 대상이다. 주문당 결제 성공은 최대 1건이다(5절).
- **금액:** `item_gross_amount = unit_price × quantity`, `item_paid_amount = item_gross_amount − item_discount`. 배송비는 넣지 않는다.
- **환불 합계:** `refund_quantity`·`refund_amount`는 `refunds`를 `(channel, order_item_id)`로 합산한다. 환불이 없으면 0, `last_refund_date`는 null이다.
- **fact_refund 원천:** `refunds`를 `order_items`(`channel, order_item_id`)와 `payments`(`channel, payment_id`, `status = SUCCEEDED`)에 조인한다. `payment_date`는 원결제의 결제일이다. `refund_profile_key = {refund_type}-{refund_reason}`이며 사유가 없으면(`refund_reason_code` null) `OTHER`로 둔다.
- **이상 주문 제외:** 취소(`order_cancellations`)와 결제 성공이 함께 있는 주문은 `fact_sales_item`에서 제외하고, 그 주문의 환불도 `fact_refund`에서 제외한다(환불 합계 대조를 맞추기 위함). 이상 주문 목록(뷰)은 PoC 이후다.
- **시각 컬럼:** `payment_at`은 UTC `TIMESTAMP`, `*_date`는 KST `DATE`다.

### 6.2 Dimension

| 테이블 | grain | PK | 컬럼 | 원천 |
|---|---|---|---|---|
| dim_date | KST 일자 1행 | date | weekday(월~일), week_start(월요일 일자), month(`yyyy-MM`), holiday(공휴일 여부) | 생성. 범위는 두 Fact의 `payment_date`·`refund_date` 최소~최대 |
| dim_channel | 채널 1행 | channel | channel_name | 상수: A 자사몰, B 종합 오픈마켓, C 스포츠 전문몰 |
| dim_product | SKU 1행(현재 값만) | product_id | product_name, category | `skus.json` (SCD 없음) |
| dim_payment_method | 결제수단 1행 | payment_method | method_name | 상수: CARD, EASY_PAY, BANK_TRANSFER, VIRTUAL_ACCOUNT, OTHER |
| dim_refund_profile | 환불 유형 × 사유 1행(10행, junk) | refund_profile_key | refund_type, refund_reason | 상수: `CANCEL`·`RETURN` × 공통 사유 코드 5종 |

`holiday`는 코드에 둔 2026년 법정 공휴일 목록(대체공휴일 제외)으로 채우며 공식 달력과 대조하지 않았다.

### 6.3 날짜 귀속

- 모든 일자는 업무 발생 시각(UTC)을 **KST**로 바꾼 날짜다(예: S23의 결제는 UTC 10-11 15:30이므로 결제일 10-12). 수신·적재 시각은 쓰지 않는다.
- 판매·환불 지표는 **결제일(`payment_date`, 원거래 기준)**로 귀속한다. 환불은 원결제일에 귀속하므로 같은 결제일 범위 안에서 환불금액이 총판매금액을 넘지 않는다. 환불 발생일(`refund_date`)은 별도 분석 축이다.

### 6.4 적재 규칙

- Silver에서 Fact를 다시 계산한 결과를 키로 MERGE한다. 키는 `(channel, order_item_id)`, `(channel, refund_id)`다. 대상에 있으면 갱신하고, 없으면 추가하며, 새 결과에 없는 대상 행(예: 나중에 취소가 도착해 제외 대상이 된 주문)은 삭제한다.
- 결제·환불이 주문·결제보다 먼저 와서 조인이 안 되는 행은 이번 실행에서 빠지고 짝이 도착한 다음 실행에서 반영된다.
- Dimension은 키로 갱신·추가하며 삭제하지 않는다.

### 6.5 지표

| 지표 | 정의 |
|---|---|
| 총판매금액 | `fact_sales_item.item_paid_amount` 합 |
| 환불금액 | 판매: `fact_sales_item.refund_amount` 합. 환불 분석: `fact_refund.refund_amount` 합 |
| 순판매금액 | 총판매금액 − 환불금액 |
| 환불금액비율 | 환불금액 ÷ 총판매금액 (총판매금액 0이면 NULL) |
| 상품결제건수 | `fact_sales_item` 행 수 |
| 상품결제단가 | 총판매금액 ÷ 상품결제건수 |
| 결제고객수 | `fact_sales_item`의 `channel + user_id` distinct (`user_id` null 제외, 이후 환불한 고객 포함) |
| 객단가 | 총판매금액 ÷ 결제고객수 |
| 환불수량 | `fact_refund.refund_quantity` 합 |
| 사유별 비중 | 사유별 환불금액 ÷ 전체 환불금액 |

- 분석 기준: 일자(`payment_date`), 채널, 상품, 결제수단, 환불 유형·사유.
- 결제고객수·객단가·상품결제단가·비율은 비가산이다. 일·채널·상품별 값을 더하지 않고 조회 범위에서 분자·분모를 다시 집계한다.
- 현재 지표에 방문수·전환율·결제 성공률·취소율은 없다.

### 6.6 Gold 대조 규칙

- 성공 결제금액 = 주문별 `Σ item_paid_amount` + 배송비 (배송비: `Σ item_paid_amount` 2만 원 이상 0원, 미만 4,000원).
- `Σ fact_refund.refund_amount = Σ fact_sales_item.refund_amount`, 환불 수량도 같다.
- PK 중복 0건. FK 누락 0건(상품·결제수단·환불 프로필·채널·일자가 각 Dimension에 있다).
- 건수·금액·지표는 `expected.json`과 일치한다.
